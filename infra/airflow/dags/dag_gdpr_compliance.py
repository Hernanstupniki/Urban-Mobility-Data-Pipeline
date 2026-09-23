"""
DAG: dag_gdpr_compliance

Propagates erasure requests from the OLTP mobility.gdpr_requests table into
every lakehouse layer and vacuums the touched Delta tables
(gdpr/gdpr_propagate_erasure.py).

The job is a single task on purpose: propagation must be atomic and
idempotent, and its watermark + audit completion markers are managed inside
the script. Splitting it per table would break those guarantees.

Requires GDPR_HASH_KEY (see infra/airflow/.env): the deterministic
tokenization secret. Rotating it makes previously erased values
irreproducible, so treat it as a credential.
"""

from datetime import timedelta

import pendulum

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "hernan-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Safe to retry: requests already completed are skipped via audit markers.
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# Repository mount point inside the containers (see docker-compose.yaml).
PROJECT_DIR = "/opt/project"

RUN_WRAPPERS = f"{PROJECT_DIR}/scripts/run"

# The bash_command below ends with a trailing space on purpose: Jinja would
# otherwise try to load commands ending in .sh as template files
# (docs/troubleshooting.md, 2026-09-18).

# Single-slot pool: one Spark driver at a time, protects WSL resources.
SPARK_POOL = os.getenv("AIRFLOW_SPARK_POOL", "spark_pool")

DAG_MAX_ACTIVE_RUNS = int(os.getenv("AIRFLOW_DAG_MAX_ACTIVE_RUNS", "1"))


def pre_execution_audit(**context):
    """Cheap fail-fast check that runs before any Spark job."""
    logical_date = context.get("logical_date") or context.get("execution_date")
    print(f"=== GDPR COMPLIANCE START === logical_date={logical_date}")
    print(f"Working directory: {PROJECT_DIR}")
    data_dir = os.path.join(PROJECT_DIR, "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)
        print("Created data/ directory for the Bronze/Silver/Gold layers.")
    return "pre-execution audit completed"


with DAG(
    dag_id="dag_gdpr_compliance",
    default_args=default_args,
    description="GDPR erasure propagation across lakehouse layers + Delta VACUUM",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Buenos_Aires"),
    schedule="0 23 * * 0",
    catchup=False,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=["gdpr", "compliance", "delta"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")
    end = EmptyOperator(task_id="end_pipeline")

    audit_task = PythonOperator(
        task_id="audit_pre_execution",
        python_callable=pre_execution_audit,
    )

    with TaskGroup(group_id="gdpr_propagate_erasure") as gdpr_group:

        BashOperator(
            task_id="gdpr_propagate_erasure",
            bash_command=f"bash {RUN_WRAPPERS}/gdpr/run_gdpr_propagate_erasure.sh ",
            pool=SPARK_POOL,
        )

    # Republish the serving layer so erasures become visible to consumers
    # without waiting for the next core pipeline run.
    publish_task = BashOperator(
        task_id="publish_reporting",
        bash_command=f"bash {RUN_WRAPPERS}/publishing/run_publish_reporting.sh ",
        pool=SPARK_POOL,
    )

    start >> audit_task >> gdpr_group >> publish_task >> end
