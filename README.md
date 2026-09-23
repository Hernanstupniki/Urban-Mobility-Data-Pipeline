# Urban Mobility Data Pipeline

A local, cloud-shaped data engineering project: a medallion lakehouse
(PostgreSQL OLTP → Bronze → Silver → Gold) with an analytics serving layer
(PostgreSQL `mobility_dw` → Power BI target), built with **PySpark 3.5 +
Delta Lake 3.1**, orchestrated by **Airflow 2.10 (Celery) on Docker
Compose inside WSL 2**, plus GDPR right-to-be-forgotten propagation and
retention/vacuum housekeeping.

The source OLTP intentionally produces incomplete, inconsistent, "dirty"
operational data; the pipeline's job is to make it trustworthy
deterministically (see `docs/data_contracts.md`).

## Architecture

```text
PostgreSQL (mobility_oltp)            WSL 2 / Docker Compose
      |  JDBC (Bronze)                +---------------------------+
      v                               | airflow-webserver :8080   |
 Bronze (raw Delta)                   | airflow-scheduler         |
      |  typing, dedup, SCD2, DQ      | airflow-worker (Spark     |
      v                               |   driver) + celery        |
 Silver (conformed Delta)             | spark-master :8081        |
      |  snapshot / hist SCD2 /       | spark-worker-1..2 :8082/3 |
      v   static dims + marts         | postgres (Airflow meta)   |
 Gold (Star-schema Delta)             | redis (Celery broker)     |
      |  publish: JDBC + atomic swap  | oltp-db-proxy (bridge)    |
      v                               | postgres-analytics :5433  |
 Analytics PostgreSQL (mobility_dw)   +---------------------------+
   reporting / control schemas  ----> Power BI (serving layer)

 GDPR erasure:  OLTP gdpr_requests --> propagation + VACUUM --> republish
 Retention:     Bronze/Silver deletes, Gold vacuum-only cleanup
```

All layers live on the local filesystem as Delta tables under
`data/<ENV>/` (git-ignored). Lake metadata (watermarks, runs, GDPR audit)
lives in `data/<ENV>/_control/`.

The `publish_reporting` job copies the Gold star-schema into a **separate**
analytics PostgreSQL (`mobility_dw`, service `postgres-analytics`) using
full-refresh via staging + an atomic swap transaction, with its own
`control.publish_state`/`publish_log`. That database is the intended serving
layer for BI (Power BI) and is never the OLTP instance.

## DAGs

Defined in `infra/airflow/dags/`, planned in `infra/airflow/dags/DAGS.md`:

| DAG | Purpose | Prod schedule |
|---|---|---|
| `urban_mobility_pipeline` | Bronze -> Silver -> Gold core run via `scripts/run` wrappers, ending in `publish_reporting` | daily |
| `dag_gdpr_compliance` | Propagate OLTP erasure requests to every layer + targeted VACUUM, then republish serving | weekly |
| `dag_lakehouse_retention_vacuum` | Bronze/Silver aged-row deletion + Gold vacuum-only cleanup | monthly |
| `dag_generate_mock_data` | Append a dirty synthetic OLTP batch (dev tool) | manual |

Every task shells out to a wrapper under `scripts/run/` — the single
source of truth for job execution, identical from CLI and Airflow. Spark
jobs share the `spark_pool` single slot so only one driver runs at a time.

## Repository layout

```text
db/              OLTP schema + seed (mobility_oltp.sql)
migrations/      versioned lake migrations (control, SCD2, contracts)
src/             Bronze/Silver/Gold jobs; src/common/ = shared engine
gdpr/            erasure propagation job
retention/       Bronze/Silver/Gold cleanup entry points
publishing/      Gold -> analytics PostgreSQL serving-layer publish job
scripts/         generate_oltp_data/ + run/ wrappers (_common.sh resolver)
infra/airflow/   Dockerfile, docker-compose.yaml, DAGs, Airflow ops README
bi/              Power BI Project (PBIP): TMDL semantic model + PBIR report
tests/           unit + integration pytest suites
docs/            pipeline.md, data_contracts.md, troubleshooting.md
```

## Getting started (portability)

Prerequisites: Linux or WSL2 with Docker + Compose v2, Python 3.10+ (host
CLI only), a PostgreSQL 14+ instance for the OLTP database.

1. Clone and enter the repo.
2. `cp infra/airflow/.env.example infra/airflow/.env` and fill it in.
   `DB_PASSWORD`, `ANALYTICS_DB_PASSWORD` (serving DB) and `GDPR_HASH_KEY` are
   required; the GDPR key must be at least 16 chars (`openssl rand -hex 32`).
   Never commit `.env`.
3. Create the OLTP database:
   `psql -U postgres -f db/mobility_oltp.sql`
4. Run lake migrations:
   `scripts/run/migrations/run_000_create_control_tables.sh` through `003`.
5. Seed operational data:
   `scripts/generate_oltp_data/run_generate_oltp_data.sh`
6. Start the stack:
   `cd infra/airflow && docker compose --profile spark-cluster up -d --build`
   This also starts `postgres-analytics` (`mobility_dw`, host port 5433) with
   the `reporting`/`control`/`staging` schemas pre-created.
   UI at `http://localhost:8080` (default dev user `admin`/`admin` — change
   before any non-local exposure).
7. Unpause and trigger `urban_mobility_pipeline`; monitor Bronze→Gold→publish.
   Verify serving counts: `reporting.*` vs `control.publish_state` in
   `postgres-analytics`.

Host CLI runs need the same env as the containers; the wrappers accept
both `OLTP_DB_*` (containers) and `DB_*` (host). Optional Spark tuning:
`SPARK_MASTER` (defaults to the standalone cluster; `local[2]` to debug),
`SPARK_SHUFFLE_PARTITIONS`, memory vars — see `infra/airflow/README.md`.

## Tests

```bash
python -m pytest tests/unit        # fast, no Spark JVM
python -m pytest tests             # includes integration (needs Docker stack)
```

CI-able policy tests enforce: one and only one module builds Spark
sessions (`tests/unit/test_spark_policy.py`), config safety, data
contracts and dirty-data handling.

## Operational notes

- Re-execution semantics, GDPR vacuum windows and retention thresholds:
  `docs/pipeline.md`.
- Known incidents and verified fixes: `docs/troubleshooting.md`
  (consult before debugging; register new findings).
- GDPR erasure survival: anonymized Silver/Gold values persist across
  pipeline reruns (verified end-to-end, see troubleshooting 2026-09-18).
