"""
DAG: urban_mobility_pipeline

Main medallion pipeline: PostgreSQL OLTP -> Bronze -> Silver -> Gold,
built with PySpark and Delta Lake.

Airflow only defines order and dependencies; every task delegates to the
wrappers under scripts/run/, the single source of truth for job execution
(same entry points used from the CLI). In Azure these tasks would become
DatabricksSubmitRunOperator / Synapse batch jobs.
"""

from datetime import datetime, timedelta

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
    # Transient Spark/infra failures get one automatic retry.
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
    print(f"=== URBAN MOBILITY PIPELINE START === logical_date={logical_date}")
    print(f"Working directory: {PROJECT_DIR}")
    data_dir = os.path.join(PROJECT_DIR, "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)
        print("Created data/ directory for the Bronze/Silver/Gold layers.")
    return "pre-execution audit completed"


with DAG(
    dag_id="urban_mobility_pipeline",
    default_args=default_args,
    description="Medallion pipeline: OLTP -> Bronze -> Silver -> Gold",
    start_date=datetime(2026, 1, 1),
    # Manual during development; use '0 3 * * *' in production.
    schedule=None,
    catchup=False,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=["mobility", "lakehouse", "pyspark", "delta", "azure-ready"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")
    end = EmptyOperator(task_id="end_pipeline")

    audit_task = PythonOperator(
        task_id="audit_pre_execution",
        python_callable=pre_execution_audit,
    )

    # Bronze: raw ingestion; independent entities can run in parallel.
    with TaskGroup(group_id="bronze_ingestion") as bronze_group:

        BashOperator(
            task_id="ingest_zones",
            bash_command=f"bash {RUN_WRAPPERS}/run_zones_bronze.sh ",
            pool=SPARK_POOL,
        )

        BashOperator(
            task_id="ingest_passengers",
            bash_command=f"bash {RUN_WRAPPERS}/run_passengers_bronze.sh ",
            pool=SPARK_POOL,
        )

        BashOperator(
            task_id="ingest_drivers",
            bash_command=f"bash {RUN_WRAPPERS}/run_drivers_bronze.sh ",
            pool=SPARK_POOL,
        )

        BashOperator(
            task_id="ingest_trips",
            bash_command=f"bash {RUN_WRAPPERS}/run_trips_bronze.sh ",
            pool=SPARK_POOL,
        )

    # Silver: dimensions are cleaned first; trips validate against them.
    with TaskGroup(group_id="silver_processing") as silver_group:

        clean_dimensions = BashOperator(
            task_id="clean_dimension_tables",
            bash_command=f"bash {RUN_WRAPPERS}/run_zones_silver.sh ",
            pool=SPARK_POOL,
        )

        clean_facts = BashOperator(
            task_id="clean_fact_trips",
            bash_command=f"bash {RUN_WRAPPERS}/run_trips_silver.sh ",
            pool=SPARK_POOL,
        )

        clean_dimensions >> clean_facts

    # Gold: deterministic rebuilds of the daily marts.
    with TaskGroup(group_id="gold_marts") as gold_group:

        BashOperator(
            task_id="compute_daily_trip_kpis",
            bash_command=f"bash {RUN_WRAPPERS}/gold/_marts/aggregates/run_agg_trips_daily.sh ",
            pool=SPARK_POOL,
        )

        BashOperator(
            task_id="compute_driver_daily_kpis",
            bash_command=f"bash {RUN_WRAPPERS}/gold/_marts/aggregates/run_agg_driver_daily.sh ",
            pool=SPARK_POOL,
        )

    start >> audit_task >> bronze_group >> silver_group >> gold_group >> end
