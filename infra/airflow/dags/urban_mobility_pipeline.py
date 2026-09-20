"""
DAG: urban_mobility_pipeline

Main medallion pipeline: PostgreSQL OLTP -> Bronze -> Silver -> Gold,
built with PySpark and Delta Lake.

Airflow only defines order and dependencies; every task delegates to the
wrappers under scripts/run/, the single source of truth for job execution
(same entry points used from the CLI). In Azure these tasks would become
DatabricksSubmitRunOperator / Synapse batch jobs.

Execution order mirrors docs/pipeline.md: all Bronze ingestion (parallel),
Silver dimensions before Silver facts (trips validate the driver/vehicle
pair against the current vehicle dimension), Gold static and conformed
dimensions, then fact_trips -> fact_payments/fact_ratings/aggregates.
"""

from datetime import datetime, timedelta

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.helpers import chain
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


def _spark_task(task_id, wrapper):
    return BashOperator(
        task_id=task_id,
        bash_command=f"bash {RUN_WRAPPERS}/{wrapper} ",
        pool=SPARK_POOL,
    )


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

    # Bronze: raw JDBC ingestion; independent entities fan out in parallel.
    with TaskGroup(group_id="bronze_ingestion") as bronze_group:
        _spark_task("ingest_zones", "run_zones_bronze.sh")
        _spark_task("ingest_passengers", "run_passengers_bronze.sh")
        _spark_task("ingest_drivers", "run_drivers_bronze.sh")
        _spark_task("ingest_vehicles", "run_vehicles_bronze.sh")
        _spark_task("ingest_trips", "run_trips_bronze.sh")
        _spark_task("ingest_payments", "run_payments_bronze.sh")
        _spark_task("ingest_ratings", "run_ratings_bronze.sh")

    # Silver: dimensions first (vehicle dim gates trips), then facts.
    with TaskGroup(group_id="silver_processing") as silver_group:
        clean_zones = _spark_task("clean_dimension_tables", "run_zones_silver.sh")
        clean_passengers = _spark_task("clean_passenger_dim", "run_passengers_silver.sh")
        clean_drivers = _spark_task("clean_driver_dim", "run_drivers_silver.sh")
        clean_vehicles = _spark_task("clean_vehicle_dim", "run_vehicles_silver.sh")
        clean_trips = _spark_task("clean_fact_trips", "run_trips_silver.sh")
        clean_payments = _spark_task("clean_payments", "run_payments_silver.sh")
        clean_ratings = _spark_task("clean_ratings", "run_ratings_silver.sh")
        [clean_zones, clean_passengers, clean_drivers, clean_vehicles] >> clean_trips
        [clean_zones, clean_passengers, clean_drivers] >> clean_payments
        [clean_passengers, clean_drivers] >> clean_ratings

    # Gold: conformed dimensions, then facts, then daily aggregates.
    # Aggregates recompute from the freshly rebuilt fact_trips, never a stale one.
    with TaskGroup(group_id="gold_marts") as gold_group:
        dim_date = _spark_task("build_dim_date", "gold/_conformed/static/run_dim_date.sh")
        dim_payment = _spark_task("build_dim_payment_method", "gold/_conformed/static/run_dim_payment.sh")
        dim_zone = _spark_task("build_dim_zone", "gold/_conformed/static/run_dim_zone.sh")

        snap_passenger = _spark_task("build_snapshot_dim_passenger", "gold/_conformed/snapshot/run_dim_passenger.sh")
        snap_driver = _spark_task("build_snapshot_dim_driver", "gold/_conformed/snapshot/run_dim_driver.sh")
        snap_vehicle = _spark_task("build_snapshot_dim_vehicle", "gold/_conformed/snapshot/run_dim_vehicle.sh")
        hist_passenger = _spark_task("build_hist_dim_passenger", "gold/_conformed/hist/run_dim_passenger.sh")
        hist_driver = _spark_task("build_hist_dim_driver", "gold/_conformed/hist/run_dim_driver.sh")
        hist_vehicle = _spark_task("build_hist_dim_vehicle", "gold/_conformed/hist/run_dim_vehicle.sh")
        scd3_passenger = _spark_task("build_scd3_dim_passenger", "gold/_conformed/scd3/run_dim_passenger.sh")
        scd3_driver = _spark_task("build_scd3_dim_driver", "gold/_conformed/scd3/run_dim_driver.sh")
        scd3_vehicle = _spark_task("build_scd3_dim_vehicle", "gold/_conformed/scd3/run_dim_vehicle.sh")

        fact_trips = _spark_task("build_fact_trips", "gold/_marts/facts/run_fact_trips.sh")
        fact_payments = _spark_task("build_fact_payments", "gold/_marts/facts/run_fact_payments.sh")
        fact_ratings = _spark_task("build_fact_ratings", "gold/_marts/facts/run_fact_ratings.sh")
        agg_trips = _spark_task("compute_daily_trip_kpis", "gold/_marts/aggregates/run_agg_trips_daily.sh")
        agg_drivers = _spark_task("compute_driver_daily_kpis", "gold/_marts/aggregates/run_agg_driver_daily.sh")

        chain([snap_passenger, snap_driver, snap_vehicle],
              [hist_passenger, hist_driver, hist_vehicle],
              [scd3_passenger, scd3_driver, scd3_vehicle])
        [snap_passenger, snap_driver, snap_vehicle] >> fact_trips
        fact_trips >> [fact_ratings, agg_trips, agg_drivers]
        dim_payment >> fact_payments

    start >> audit_task >> bronze_group >> silver_group >> gold_group >> end
