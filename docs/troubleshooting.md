# Troubleshooting log

Read this log before diagnosing a recurring failure. Record new reproducible incidents even when the cause is still unknown. Use `DETECTED` for an unconfirmed cause, `PENDING` for incomplete work or validation, and `RESOLVED` only after a verified fix. Keep dates, exact symptoms, evidence, affected files, and follow-up steps. Never record credentials or other secrets.

## Incidents

### 2026-09-16 — WSL restarted under Spark and Airflow load

- Status: `RESOLVED`.
- Component: Airflow, Celery, PySpark, and WSL.
- Symptom: WSL restarted while a DAG launched several Spark jobs.
- Evidence: Celery allowed 16 concurrent tasks, multiple Spark sessions and JVMs ran at once, and local resource limits were too broad.
- Cause: concurrent Spark processes exceeded the practical local resource budget.
- Files: `infra/airflow/docker-compose.yaml`, `infra/airflow/dags/urban_mobility_pipeline.py`, `src/common/spark.py`, and `infra/wsl/urban-mobility-airflow.service`.
- Fix: use one master, two workers with 1 core and 768 MB each, a 1 GB driver, one active DAG run, one `spark_pool` slot, Celery concurrency 2, and Docker limits. Keep `local[2]` only as a fallback.
- Verified: local and distributed smoke tests passed; 11 tests passed.

### 2026-09-16 — Spark worker UI advertised an internal IP

- Status: `RESOLVED`.
- Component: Spark worker UI.
- Symptom: worker links pointed to an unreachable address such as `172.18.0.6:8081`.
- Cause: Spark advertised its container IP.
- Files: `infra/airflow/docker-compose.yaml`.
- Fix: set `SPARK_PUBLIC_DNS=localhost` and publish separate host ports `8082` and `8083`.
- Verified: both worker UIs returned HTTP 200 from localhost.

### 2026-09-16 — Bronze JDBC attempted PostgreSQL on localhost

- Status: `RESOLVED`.
- Component: Bronze JDBC ingestion.
- Symptom: `Connection to localhost:5432 refused` in the Bronze tasks.
- Evidence: `Settings` read `DB_HOST`, but Compose supplied `OLTP_DB_HOST`.
- Cause: application and container environment names differed.
- Files: `src/common/config.py`, `infra/airflow/docker-compose.yaml`.
- Fix: prefer `OLTP_DB_*` in Airflow and retain `DB_*` as a host CLI fallback.
- Verified: Airflow connected to PostgreSQL and Bronze zones read through JDBC.

### 2026-09-16 — `DB_HOST` conflicted with the Airflow entrypoint

- Status: `RESOLVED`.
- Component: Airflow worker and Redis.
- Symptom: the worker exhausted 20 connection retries against `host.docker.internal:6379`, and the Spark driver disappeared.
- Evidence: the container reported `DB_HOST=host.docker.internal`, `DB_PORT=6379`, and a refused Redis connection.
- Cause: the Airflow image uses `DB_HOST` internally.
- Files: `infra/airflow/docker-compose.yaml`, `src/common/config.py`.
- Fix: remove generic `DB_*` variables from the common Airflow environment and use `OLTP_DB_*` for the application.
- Verified: the worker stayed healthy with `RestartCount=0`, and a Spark job completed.

### 2026-09-16 — Docker could not reach WSL PostgreSQL

- Status: `RESOLVED`.
- Component: PostgreSQL 14 and Docker networking.
- Symptom: `host.docker.internal` first failed to resolve, then refused connections.
- Evidence: PostgreSQL listened only on `127.0.0.1:5432`; the Compose bridge used `172.18.0.0/16` and gateway `172.18.0.1`.
- Cause: native Docker in WSL lacked Docker Desktop's host mapping, and PostgreSQL did not listen on the bridge.
- Files: `infra/airflow/docker-compose.yaml`, PostgreSQL `pg_hba.conf`, and its effective `listen_addresses`.
- Fix: map `host.docker.internal` to the bridge gateway, listen only on localhost and `172.18.0.1`, and allow `mobility_oltp` from `172.18.0.0/16` with `scram-sha-256`. A `pg_hba.conf.pre-urban-mobility` backup was created.
- Verified: Airflow and both Spark workers passed TCP checks; PostgreSQL authentication passed. If the bridge subnet changes, update the gateway, listener, and `pg_hba.conf` together.

### 2026-09-16 — OLTP password did not match PostgreSQL

- Status: `RESOLVED`.
- Component: PostgreSQL and JDBC.
- Symptom: `FATAL: password authentication failed for user "postgres"`.
- Cause: the local PostgreSQL role and `DB_PASSWORD` in `infra/airflow/.env` differed.
- Files: local PostgreSQL role and untracked `.env`.
- Fix: synchronize the role with the configured secret without printing or committing it.
- Verified: Airflow `psycopg2` authentication and Spark JDBC reads passed.

### 2026-09-16 — Shared Delta mount denied access

- Status: `RESOLVED`.
- Component: Spark executors and `/opt/project/data`.
- Symptom: `FileNotFoundException ... Permission denied` under `data/dev/_control/etl_control`.
- Cause: Airflow used UID 1000 while Spark workers used UID 50000 on the same bind mount.
- Files: `infra/airflow/docker-compose.yaml`, `infra/airflow/Dockerfile`.
- Fix: run workers under `${AIRFLOW_UID}` and register `RUNTIME_UID` in the image.
- Verified: both workers resolved UID 1000 with `getent passwd`; Bronze read and wrote Delta.

### 2026-09-16 — Spark executors could not resolve UID 1000

- Status: `RESOLVED`.
- Component: Hadoop UserGroupInformation.
- Symptom: `KerberosAuthException` with `NullPointerException: invalid null input: name`.
- Evidence: setting `SPARK_USER` helped the daemon but not the executor JVMs.
- Cause: UID 1000 had no `/etc/passwd` entry inside the worker image.
- Files: `infra/airflow/Dockerfile`, `infra/airflow/docker-compose.yaml`.
- Fix: register `RUNTIME_UID` during the image build and retain `SPARK_USER=airflow` for the daemon.
- Verified: executors started and processed distributed stages on both workers.

### 2026-09-16 — Spark worker could not create its work directory

- Status: `RESOLVED`.
- Component: Spark standalone worker.
- Symptom: `AccessDeniedException` while creating `${SPARK_HOME}/work`.
- Cause: the image-owned Spark directory was not writable by UID 1000.
- Files: `infra/airflow/docker-compose.yaml`.
- Fix: use `SPARK_WORKER_DIR=/tmp/spark-work` and pass `--work-dir` to the daemon.
- Verified: both workers registered with the master and launched executors.

### 2026-09-16 — Spark reported that a job accepted no resources

- Status: `RESOLVED`.
- Component: Spark scheduler.
- Symptom: `Initial job has not accepted any resources` despite two visible workers.
- Evidence: the master repeatedly launched executors that exited with code 1.
- Cause: the work directory and UID lookup errors killed the executors; the scheduler message was secondary.
- Fix: correct `SPARK_WORKER_DIR` and register `RUNTIME_UID` without increasing cores or memory.
- Verified: Bronze zones completed with `exit 0` and `status=NO_DATA` on two executors.

### 2026-09-16 — Gold DAG referenced a missing file

- Status: `RESOLVED`.
- Component: `urban_mobility_pipeline` DAG.
- Symptom: a task referenced the absent `src/gold/driver_payouts.py`.
- Files: `infra/airflow/dags/urban_mobility_pipeline.py`.
- Fix: call the existing `run_agg_driver_daily.sh` wrapper as `compute_driver_daily_kpis`.
- Verified: Python compiled, Airflow loaded both Gold tasks, and `airflow dags list-import-errors` was empty.

### 2026-09-16 — Corrected DAG needed a full run

- Status: `PENDING`.
- Component: run `manual__2026-09-16T06:14:39+00:00`.
- Symptom: the run started while the scheduler still held the old serialized graph.
- Evidence: after scheduler, webserver, and triggerer recreation, Airflow listed `gold_marts.compute_driver_daily_kpis`; compile, 11 tests, import checks, and distributed Bronze zones passed.
- Follow-up: confirm whether that run completed or start a new run if it retained the old graph, then verify every task.

### 2026-09-16 — Interrupted Compose recreation left temporary names

- Status: `RESOLVED`.
- Component: Docker Compose.
- Symptom: container-name conflict during `--force-recreate` after the command timed out.
- Cause: Compose was interrupted during its rename and recreate sequence.
- Fix: let reconciliation finish, then run `docker compose ... up -d --no-build`. Volumes and data were kept.
- Verified: Airflow services and the Spark master were healthy, and both workers registered.

### 2026-09-16 — Ivy resolved dependencies at each Spark start

- Status: `RESOLVED`.
- Component: Delta Lake and Ivy.
- Symptom: logs showed `resolving dependencies`, which looked like repeated downloads.
- Evidence: Ivy reported `0 artifacts copied, 3 already retrieved` and `0 downloaded`.
- Cause: `configure_spark_with_delta_pip()` still resolves coordinates but uses the preloaded cache.
- Files: `infra/airflow/Dockerfile`, `src/common/spark.py`.
- Fix: preload Delta 3.1.0 at image build and share the functional Ivy cache; retain PySpark 3.5.0 and Delta 3.1.0.
- Verified: later runs downloaded no artifacts.

### 2026-09-16 — Silver and Gold created duplicate Spark sessions

- Status: `RESOLVED`.
- Component: PySpark/Delta jobs and wrappers.
- Symptom: jobs used direct `SparkSession.builder.getOrCreate()` and local tuning while wrappers invoked `spark-submit --packages/--jars` first.
- Cause: parallel session setup paths could disagree on master, memory, Delta, parallelism, and classpath.
- Files: `src/common/spark.py`, `scripts/run/_common.sh`, Silver jobs, static Gold dimensions, and related tests.
- Fix: use `build_spark(job_name)` in every job; wrappers run Python without a second SparkContext.
- Verified: session creation exists only in the factory, wrapper scripts no longer use `spark-submit`, syntax checks passed, and 10 unit tests passed.

### 2026-09-16 — Host integration pytest waited for Ivy

- Status: `PENDING`.
- Component: WSL virtual environment integration test.
- Symptom: pytest waited after the test moved to `build_spark()`.
- Evidence: local Java requested `io.delta:delta-spark_2.12:3.1.0` without the image's Ivy cache; 6.1 GiB RAM was available and swap was unused.
- Cause: Delta package resolution in the host environment.
- Follow-up: run integration in the preloaded Docker image or supply a valid host Ivy cache. Do not create a second Spark session for a faster test.

### 2026-09-16 — Wrappers required Git inside the image

- Status: `RESOLVED`.
- Component: `scripts/run` wrappers.
- Symptom: `run_zones_bronze.sh: line 4: git: command not found`.
- Cause: wrappers used `git rev-parse` to locate the root, but the image had no Git.
- Files: `infra/airflow/docker-compose.yaml`, wrapper scripts.
- Fix: Compose supplies `PROJECT_DIR=/opt/project`; Git remains a host fallback.
- Verified: Compose validated. Runtime smoke validation was tracked with the Docker incident below.

### 2026-09-16 — Docker metadata broke during recreation

- Status: `RESOLVED` on 2026-09-19.
- Component: WSL Docker Engine and Compose.
- Symptom: temporary-name conflicts, `No such container`, missing RW layers, and an inactive Docker daemon.
- Evidence: Docker logs showed missing layers for old Airflow containers after an interrupted `--force-recreate`; concurrent Compose/systemd recovery worsened the state.
- Cause: ephemeral container metadata was left between removal and recreation. Persistent volumes remained intact.
- Fix: remove orphaned project containers without `-v`, then stop Docker and its socket, remove only three confirmed dead container metadata directories, start Docker, and run one Compose reconciliation.
- Verified: Docker became active; the worker saw `GDPR_HASH_KEY` length 64, GDPR run `manual__2026-09-19T00:12:54+00:00` succeeded, and the core pipeline had completed successfully.

### 2026-09-18 — BashOperator treated `.sh` commands as Jinja templates

- Status: `RESOLVED`.
- Component: `urban_mobility_pipeline` BashOperator tasks.
- Symptom: `jinja2.exceptions.TemplateNotFound` for `bash /opt/project/scripts/run/run_zones_bronze.sh`; Bronze failed before command execution.
- Evidence: failure occurred in `render_templates` for run `manual__2026-09-18T21:48:11+00:00`.
- Cause: BashOperator interprets a command ending in `.sh` as a template path.
- Files: DAG wrapper commands.
- Fix: leave one harmless trailing space after every wrapper command.
- Verified: run `manual__2026-09-18T21:55:14+00:00` completed Bronze through Gold in 15 minutes with one Spark pool slot.

### 2026-09-20 — Ratings SCD2 merge created duplicate current rows

- Status: `RESOLVED`.
- Component: `src/silver/ratings_bronze_to_silver.py`.
- Symptom: publishing failed with `UniqueViolation` for duplicate `rating_id=3945`.
- Evidence: 4,566 ratings had more than one current Silver row, while source OLTP keys were unique.
- Cause: both SCD2 merges matched on `trip_id` instead of the entity key `rating_id`.
- Fix: match on current `rating_id` and collapse repeat observations per rating using `row_number()` ordered by `raw_loaded_at`. Rebuild inconsistent development lake data from OLTP with the documented rebootstrap path.
- Verified: 5,194 ratings published without primary-key errors; the next incremental run added exactly one trip and one rating without duplicates.

### 2026-09-20 — Core DAG omitted `fact_trips` and Gold dependencies

- Status: `RESOLVED`.
- Component: `urban_mobility_pipeline` and `src/common/gold_marts.py`.
- Symptom: `build_fact_trips` could not find `dim_zone`; aggregates could also use stale trip facts.
- Cause: static and snapshot dimensions did not precede `fact_trips`, and the fact build was absent from an earlier DAG graph.
- Fix: order Bronze, Silver dimensions and facts, Gold dimensions, trip facts, payment/rating facts and aggregates, then publish with explicit dependency edges.
- Verified: bootstrap and incremental DAG runs succeeded; 11 of 11 reporting tables published and `reporting.fact_trips` matched OLTP at 43,001.

### 2026-09-22 — Gold SCD3 was unused and facts used current dimensions

- Status: `RESOLVED`.
- Component: Gold dimensions, marts, contracts, DAG, retention, and GDPR.
- Symptom: historical trips and ratings resolved dimension keys against current snapshots; no consumer used the SCD3 previous-value columns.
- Cause: SCD3 was only a demonstration, while valid SCD2 history already existed in Silver.
- Files: `src/common/gold_dimensions.py`, `src/common/gold_marts.py`, contracts, DAG, retention, GDPR, and temporal tests.
- Fix: remove the unused SCD3 variant. Add deterministic surrogate keys to Gold history and current snapshots; resolve facts against the version effective at each event, with unknown key 0 for missing or earlier events.
- Verified: 24 unit tests and temporal integration passed; historical keys remained stable across reruns, intervals did not overlap, 11 reporting tables published, and all 48,000 trips resolved dimension surrogate keys.

### 2026-09-23 — Power BI Desktop imported rows slowly

- Status: `RESOLVED`.
- Component: Power Query and analytics PostgreSQL.
- Symptom: after fixing duplicate `trip_id` in the Trip bridge, `fact_trips` advanced only about 2,051 rows in several minutes.
- Evidence: PostgreSQL read 48,000 rows in about 35 ms, then waited on the client for over six minutes; Mashup Container used one CPU core.
- Cause: four local-hour columns were evaluated row by row in Power Query.
- Files: `Trip.tmdl`, `fact_trips.tmdl`, and the Windows PBIP test copy.
- Fix: select Trip keys directly and compute local-hour columns in PostgreSQL through a read-only native query, preserving UTC timestamps and the fixed UTC−3 transformation.
- Verified: PostgreSQL returned 48,000 rows, the full query plan took about 42 ms, structural model checks passed, and the owner confirmed that Desktop refresh became fast.
- Follow-up: time a full refresh if a formal benchmark is needed.

### 2026-09-23 — Revenue Leakage DAX failed in the redesigned report

- Status: `RESOLVED`.
- Component: `Revenue Leakage Trips` and `Revenue Leakage Amount` measures.
- Symptom: Desktop could not determine a single `fact_trips[status]` value.
- Cause: the expression read status without context transition and compared a payment trip key against a non-scalar trip column.
- Fix: evaluate status per trip ID and keep each trip ID in a scalar variable when searching for paid payments. Preserve the completed-trip-without-paid-payment definition.
- Verified: model structure passed, Desktop rendered Revenue, and Leakage Amount was 192,027 without filters, matching reporting SQL; all 48 PBIR files passed official schema validation.
