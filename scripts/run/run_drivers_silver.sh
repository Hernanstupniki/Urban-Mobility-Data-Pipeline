#!/bin/bash
set -e

echo "Starting drivers Bronze → Silver ETL"

export DELTA_AUTO_MERGE="${DELTA_AUTO_MERGE:-1}"
export ENV="${ENV:-dev}"

echo "[CONFIG] ENV=$ENV"
echo "[CONFIG] DELTA_AUTO_MERGE=$DELTA_AUTO_MERGE"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/silver/drivers_bronze_to_silver.py

echo "Drivers Silver ETL finished successfully"
