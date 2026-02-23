#!/bin/bash
set -e

echo "Starting dim_date (Gold _conformed/static) build"

export ENV="${ENV:-dev}"

echo "[CONFIG] ENV=$ENV"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  src/gold/_conformed/static/dim_date.py

echo "dim_date build finished successfully"