"""
===============================================================================
DAG: urban_mobility_pipeline

DESCRIPTION:

    Main orchestrator for the Urban Mobility Data Lakehouse.

    Implements the Medallion Architecture (Bronze -> Silver -> Gold)

    using PySpark and Delta Lake.

HOW THIS FILE WORKS:

    1. Airflow does not perform heavy data transformations here;
       it only defines the ORDER and DEPENDENCIES.

    2. Tasks use BashOperator to invoke modular scripts from /opt/project/src/.

    3. If this were running in Azure in production, instead of BashOperator
       you would use:

       - DatabricksSubmitRunOperator (Azure Databricks)

       - AzureSynapseRunSparkBatchOperator (Azure Synapse)

===============================================================================
"""

from datetime import datetime, timedelta

import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# ------------------------------------------------------------------------------
# 1. GENERAL CONFIGURATION AND RETRY POLICIES (SLA / RETRIES)
# ------------------------------------------------------------------------------

default_args = {

    "owner": "hernan-data-eng",

    "depends_on_past": False,

    "email_on_failure": False,

    "email_on_retry": False,

    # In production: if a Spark job fails because of a temporary issue, retry it

    "retries": 1,

    "retry_delay": timedelta(seconds=30),

}


# Absolute path inside the containers where the repository is mounted

PROJECT_DIR = "/opt/project"

SPARK_POOL = os.getenv("AIRFLOW_SPARK_POOL", "spark_pool")

DAG_MAX_ACTIVE_RUNS = int(
    os.getenv("AIRFLOW_DAG_MAX_ACTIVE_RUNS", "1")
)


# ------------------------------------------------------------------------------
# 2. AUXILIARY FUNCTIONS (Lightweight auditing with PythonOperator)
# ------------------------------------------------------------------------------

def pre_execution_audit(**context):

    """
    Checks preconditions before moving gigabytes of data:

    - Verifies that the project directory is mounted.

    - Prints the logical execution date (time partition).
    """

    logical_date = context.get("logical_date") or context.get("execution_date")

    print(f"=== [INICIO PIPELINE DE MOVILIDAD] ===")

    print(f"Fecha de procesamiento programada: {logical_date}")

    print(f"Directorio de trabajo: {PROJECT_DIR}")

    data_dir = os.path.join(PROJECT_DIR, "data")

    if not os.path.exists(data_dir):

        os.makedirs(data_dir, exist_ok=True)

        print("Directorio 'data/' creado para albergar capas Bronze/Silver/Gold.")

    return "Auditoria completada con exito"


# ------------------------------------------------------------------------------
# 3. DAG DEFINITION
# ------------------------------------------------------------------------------

with DAG(

    dag_id="urban_mobility_pipeline",

    default_args=default_args,

    description="Pipeline Medallion: OLTP -> Bronze -> Silver -> Gold",

    # start_date: Date from which Airflow recognizes the pipeline

    start_date=datetime(2026, 1, 1),

    # schedule: None means manual execution.

    # In production you could use '0 3 * * *' to run every day at 3 AM

    schedule=None,

    # catchup: False prevents Airflow from executing old scheduled runs
    # when the DAG is enabled

    catchup=False,

    max_active_runs=DAG_MAX_ACTIVE_RUNS,

    tags=["mobility", "lakehouse", "pyspark", "delta", "azure-ready"],

) as dag:

    # Dummy start and end nodes
    # Common Airflow practice for connecting pipeline branches

    start = EmptyOperator(
        task_id="start_pipeline"
    )

    end = EmptyOperator(
        task_id="end_pipeline"
    )


    # Pre-execution control task

    audit_task = PythonOperator(

        task_id="audit_pre_execution",

        python_callable=pre_execution_audit,

    )


    # ==========================================================================
    # LAYER 1: BRONZE (Raw ingestion from OLTP Postgres to Delta Lake)
    # ==========================================================================

    # Why TaskGroup:
    # Groups related tasks visually in the Airflow web interface.

    # Why parallel execution:
    # 'zones', 'passengers' and 'drivers' do not depend on each other.
    # Airflow can launch them simultaneously.

    with TaskGroup(group_id="bronze_ingestion") as bronze_group:

        bronze_zones = BashOperator(

            task_id="ingest_zones",

            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"PYTHONPATH={PROJECT_DIR} "
                f"python3 src/bronze/zones_oltp_to_bronze.py"
            ),

            pool=SPARK_POOL,

        )


        bronze_passengers = BashOperator(

            task_id="ingest_passengers",

            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"PYTHONPATH={PROJECT_DIR} "
                f"python3 src/bronze/passengers_oltp_to_bronze.py"
            ),

            pool=SPARK_POOL,

        )


        bronze_drivers = BashOperator(

            task_id="ingest_drivers",

            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"PYTHONPATH={PROJECT_DIR} "
                f"python3 src/bronze/drivers_oltp_to_bronze.py"
            ),

            pool=SPARK_POOL,

        )


        bronze_trips = BashOperator(

            task_id="ingest_trips",

            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"PYTHONPATH={PROJECT_DIR} "
                f"python3 src/bronze/trips_oltp_to_bronze.py"
            ),

            pool=SPARK_POOL,

        )


    # ==========================================================================
    # LAYER 2: SILVER (Cleaning, Deduplication, Typing and Delta Schemas)
    # ==========================================================================

    # Why it runs after Bronze:
    # Silver requires the raw data to already exist.

    # It is divided into:
    # 1. Dimensions (Zones, Passengers, Drivers)
    # 2. Facts (Trips): uses the cleaned dimension data.

    with TaskGroup(group_id="silver_processing") as silver_group:

        clean_dimensions = BashOperator(

            task_id="clean_dimension_tables",

            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"PYTHONPATH={PROJECT_DIR} "
                f"python3 src/silver/zones_bronze_to_silver.py"
            ),

            pool=SPARK_POOL,

        )


        clean_facts = BashOperator(

            task_id="clean_fact_trips",

            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"PYTHONPATH={PROJECT_DIR} "
                f"python3 src/silver/trips_bronze_to_silver.py"
            ),

            pool=SPARK_POOL,

        )


        # In Silver, dimensions are cleaned first,
        # and then the Trips fact table is processed

        clean_dimensions >> clean_facts


    # ==========================================================================
    # LAYER 3: GOLD (Analytical Marts and Business KPIs)
    # ==========================================================================

    # Why it runs after Silver:
    # Gold consumes already cleaned and consistent tables.

    # Aggregated metrics are calculated here
    # such as daily revenue and demand by zone.

    with TaskGroup(group_id="gold_marts") as gold_group:

        gold_daily_trips = BashOperator(

            task_id="compute_daily_trip_kpis",

            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"bash scripts/run/gold/_marts/aggregates/"
                f"run_agg_trips_daily.sh"
            ),

            pool=SPARK_POOL,

        )


        gold_driver_payouts = BashOperator(

            task_id="compute_driver_payouts",

            bash_command=(
                f"cd {PROJECT_DIR} && "
                f"PYTHONPATH={PROJECT_DIR} "
                f"python3 src/gold/driver_payouts.py"
            ),

            pool=SPARK_POOL,

        )


    # ==========================================================================
    # 4. GLOBAL ORDER AND DEPENDENCIES (Pipeline Graph)
    # ==========================================================================

    # Logical flow:
    # Start -> Audit -> Bronze Ingestion -> Silver Processing
    # -> Gold Analytics -> End

    start >> audit_task >> bronze_group >> silver_group >> gold_group >> end
