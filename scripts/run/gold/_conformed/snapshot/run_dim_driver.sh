#!/bin/bash
set -e

echo "Starting dim_driver (Gold _conformed) build"

export DELTA_AUTO_MERGE="${DELTA_AUTO_MERGE:-1}"
export ENV="${ENV:-dev}"

echo "[CONFIG] ENV=$ENV"
echo "[CONFIG] DELTA_AUTO_MERGE=$DELTA_AUTO_MERGE"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  src/gold/_conformed/snapshot/dim_driver.py

echo "dim_driver build finished successfully"
