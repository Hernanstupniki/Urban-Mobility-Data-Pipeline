#!/bin/bash
set -e

echo "Starting dim_zone (Gold _conformed/static) build"

export DELTA_AUTO_MERGE="${DELTA_AUTO_MERGE:-1}"
export ENV="${ENV:-dev}"

echo "[CONFIG] ENV=$ENV"
echo "[CONFIG] DELTA_AUTO_MERGE=$DELTA_AUTO_MERGE"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  src/gold/_conformed/static/dim_zone.py

echo "dim_zone build finished successfully"