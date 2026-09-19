"""
DAG: dag_generate_mock_data

Development helper: appends a new batch of synthetic trips, passengers,
drivers, ratings, payments and GDPR erasure requests to the OLTP database
(scripts/generate_oltp_data/generate_oltp_data.py).

The generator intentionally produces dirty data (broken_rate) and soft
deletions / erasure requests (gdpr_erasure_rate) to exercise the pipeline's
quality contracts end to end. It is NOT idempotent: each run appends fresh
rows, which is the point. Keep this DAG manual-only; it has no place in a
production schedule.

Tunables are env vars with sensible defaults in the wrapper:
N_TRIPS, N_PASSENGERS, N_DRIVERS, BROKEN_RATE, GDPR_ERASURE_RATE.

Note: this job is plain Python + psycopg2 (no Spark), so it does not
consume spark_pool slots.
"""

from datetime import datetime, timedelta

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "hernan-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # One retry is enough: the generator runs in bounded transactions and
    # a second failure usually means the OLTP itself is down.
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

# Repository mount point inside the containers (see docker-compose.yaml).
PROJECT_DIR = "/opt/project"

GEN_SCRIPTS = f"{PROJECT_DIR}/scripts/generate_oltp_data"

# bash_command strings end with a trailing space on purpose: Jinja would
# otherwise try to load commands ending in .sh as template files
# (docs/troubleshooting.md, 2026-09-18).

DAG_MAX_ACTIVE_RUNS = int(os.getenv("AIRFLOW_DAG_MAX_ACTIVE_RUNS", "1"))


def pre_execution_audit(**context):
    """Log the batch knobs before generating data."""
    logical_date = context.get("logical_date") or context.get("execution_date")
    print(f"=== MOCK DATA GENERATION START === logical_date={logical_date}")
    for var in ("N_TRIPS", "N_PASSENGERS", "N_DRIVERS", "BROKEN_RATE", "GDPR_ERASURE_RATE"):
        print(f"{var}={os.getenv(var, '(default from wrapper)')}")
    return "pre-execution audit completed"


with DAG(
    dag_id="dag_generate_mock_data",
    default_args=default_args,
    description="Synthetic OLTP batch generator for dev/testing (appends dirty data)",
    start_date=datetime(2026, 1, 1),
    # Manual only: this simulates upstream business activity, not a pipeline.
    schedule=None,
    catchup=False,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=["mock-data", "oltp", "dev"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")
    end = EmptyOperator(task_id="end_pipeline")

    audit_task = PythonOperator(
        task_id="audit_pre_execution",
        python_callable=pre_execution_audit,
    )

    generate_data = BashOperator(
        task_id="generate_oltp_data",
        bash_command=f"bash {GEN_SCRIPTS}/run_generate_oltp_data.sh ",
    )

    start >> audit_task >> generate_data >> end
