"""
DAG: dag_lakehouse_retention_vacuum

Periodic housekeeping for the lakehouse (src/common/retention.py):

- Bronze: deletes raw observations older than RETENTION_DAYS (default 14)
  and vacuums with a 168-hour retention window.
- Silver: deletes closed SCD2 history (valid_to) older than RETENTION_DAYS
  (default 30) and vacuums accordingly.
- Gold: vacuum-only. Gold is derived and rebuilt deterministically, so no
  rows are deleted; the vacuum (default 60 days) removes the obsolete files
  each rebuild leaves behind. Note this also caps time-travel depth on Gold
  tables at 60 days.

The layers are independent jobs; they run sequentially here for simpler
logs, but the spark_pool slot serializes them either way. Table names and
target paths are validated inside the script to stay within DATA_ROOT/ENV.
"""

from datetime import timedelta

import pendulum

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
    # Safe to retry: deletions are bounded by date predicates and the
    # vacuum retention floor refuses unsafe windows outside dev.
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# Repository mount point inside the containers (see docker-compose.yaml).
PROJECT_DIR = "/opt/project"

RUN_WRAPPERS = f"{PROJECT_DIR}/scripts/run"

# bash_command strings end with a trailing space on purpose: Jinja would
# otherwise try to load commands ending in .sh as template files
# (docs/troubleshooting.md, 2026-09-18).

# Single-slot pool: one Spark driver at a time, protects WSL resources.
SPARK_POOL = os.getenv("AIRFLOW_SPARK_POOL", "spark_pool")

DAG_MAX_ACTIVE_RUNS = int(os.getenv("AIRFLOW_DAG_MAX_ACTIVE_RUNS", "1"))


def pre_execution_audit(**context):
    """Cheap fail-fast check that runs before any Spark job."""
    logical_date = context.get("logical_date") or context.get("execution_date")
    print(f"=== LAKEHOUSE RETENTION START === logical_date={logical_date}")
    print(f"Working directory: {PROJECT_DIR}")
    data_dir = os.path.join(PROJECT_DIR, "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)
        print("Created data/ directory for the Bronze/Silver/Gold layers.")
    return "pre-execution audit completed"


with DAG(
    dag_id="dag_lakehouse_retention_vacuum",
    default_args=default_args,
    description="Lakehouse housekeeping: Bronze/Silver retention cleanup + Delta VACUUM",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule="0 5 1 * *",
    catchup=False,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=["retention", "maintenance", "lakehouse", "delta"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")
    end = EmptyOperator(task_id="end_pipeline")

    audit_task = PythonOperator(
        task_id="audit_pre_execution",
        python_callable=pre_execution_audit,
    )

    bronze_retention = BashOperator(
        task_id="bronze_retention_vacuum",
        bash_command=f"bash {RUN_WRAPPERS}/retention/run_bronze_retention_cleanup.sh ",
        pool=SPARK_POOL,
    )

    silver_retention = BashOperator(
        task_id="silver_retention_vacuum",
        bash_command=f"bash {RUN_WRAPPERS}/retention/run_silver_retention_cleanup.sh ",
        pool=SPARK_POOL,
    )

    # Vacuum-only: removes obsolete rebuild files, never deletes Gold rows.
    gold_vacuum = BashOperator(
        task_id="gold_vacuum_only",
        bash_command=f"bash {RUN_WRAPPERS}/retention/run_gold_vacuum_only.sh ",
        pool=SPARK_POOL,
    )

    start >> audit_task >> bronze_retention >> silver_retention >> gold_vacuum >> end
