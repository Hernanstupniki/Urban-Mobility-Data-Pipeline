"""Single Spark/Delta session factory used by every PySpark job."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_spark(job_name: str) -> SparkSession:
    master = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

    builder = (
        SparkSession.builder
        .appName(job_name)
        .master(master)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "768m"))
        .config("spark.executor.cores", os.getenv("SPARK_EXECUTOR_CORES", "1"))
        .config("spark.ui.port", os.getenv("SPARK_DRIVER_UI_PORT", "4040"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"),
        )
        .config(
            "spark.default.parallelism",
            os.getenv("SPARK_DEFAULT_PARALLELISM", "4"),
        )
        .config(
            "spark.sql.files.maxPartitionBytes",
            os.getenv("SPARK_MAX_PARTITION_BYTES", "64MB"),
        )
        .config(
            "spark.databricks.delta.schema.autoMerge.enabled",
            os.getenv("DELTA_AUTO_MERGE", "false"),
        )
        .config(
            "spark.databricks.delta.retentionDurationCheck.enabled",
            os.getenv("DELTA_RETENTION_DURATION_CHECK_ENABLED", "true"),
        )
    )

    if master.startswith("spark://"):
        builder = (
            builder
            .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "airflow-worker"))
            .config(
                "spark.driver.bindAddress",
                os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"),
            )
        )

    ivy_dir = os.getenv("SPARK_IVY_DIR")
    if ivy_dir:
        builder = builder.config("spark.jars.ivy", ivy_dir)

    extra_jars = os.getenv("SPARK_JARS", os.getenv("POSTGRES_JAR", "")).strip()
    if extra_jars:
        builder = builder.config("spark.jars", extra_jars)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark
