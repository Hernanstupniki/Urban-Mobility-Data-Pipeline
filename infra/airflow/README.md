# Airflow and Spark operations

## Local stack

Docker Compose runs Apache Airflow 2.10.5, PostgreSQL metadata storage, Redis, a Celery worker, Spark master and workers, and the analytics PostgreSQL service. The repository is mounted at `/opt/project`; DAG files come from `infra/airflow/dags/`. The web UI is available at `http://localhost:8080` while the stack is running. The development `admin/admin` account must not be used for a non-local deployment.

The DAGs only orchestrate jobs. Each data task invokes its wrapper under `scripts/run/`, so CLI and Airflow use the same entry point. Spark jobs create sessions through `src/common/spark.py`, not inside the DAG file.

## Start and inspect

Run these commands from `infra/airflow/`:

```bash
docker compose --profile spark-cluster up -d
docker compose ps
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker
```

Use `docker compose stop` to stop services without deleting data. Rebuild the image with `docker compose build` after changing the Dockerfile or its dependencies. Never remove persistent volumes as part of a routine restart.

## DAGs

| DAG | Work | Test branch |
|---|---|---|
| `urban_mobility_pipeline` | Bronze, Silver, Gold, then atomic publish to analytics PostgreSQL | Manual |
| `dag_gdpr_compliance` | Propagate erasures, vacuum affected tables, and republish | Manual |
| `dag_lakehouse_retention_vacuum` | Bronze and Silver retention, then Gold vacuum | Manual |
| `dag_generate_mock_data` | Append synthetic OLTP data for tests | Manual only |

The production branch schedules the three operational DAGs in `America/Asuncion`. The synthetic generator remains manual. See `dags/DAGS.md` for the branch schedule. Airflow's `spark_pool` has one slot, and each DAG limits active runs to protect the local Spark cluster. A trailing space after each `.sh` BashOperator command prevents Airflow from treating the command as a Jinja template path.

## Configuration

Copy `.env.example` to `.env` and supply the required database passwords and `GDPR_HASH_KEY`. Do not commit `.env`. The Compose file passes `OLTP_DB_*` to the application; generic `DB_HOST` conflicts with the Airflow image entrypoint. `ENV` selects the data directory under `data/<ENV>/`.

The normal Spark master is `spark://spark-master:7077`. The driver runs in the Airflow worker. The default local limits are Celery concurrency 2, one Spark pool slot, one active run per DAG, a 1 GB driver, and two Spark workers with 1 core and 768 MB each. Override resource settings through the variables documented in `.env.example` when needed.

Local interfaces: Airflow `:8080`, Spark master `:8081`, Spark workers `:8082` and `:8083`, and the active Spark driver `:4040`.

## Verify a change

Check DAG imports before running a task:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags list
```

Inspect task logs in Airflow or with `docker compose logs`. For a new failure, check `docs/troubleshooting.md` before changing the pipeline.

The optional `urban-mobility-airflow.service` starts the distributed stack after Docker becomes available in WSL. Its source file is in this directory; installing or enabling it changes host startup behavior and is a separate operation.
