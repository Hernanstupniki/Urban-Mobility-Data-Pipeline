#!/bin/bash
set -e

echo "Starting fact_payments (Gold _marts/facts) build"

export DELTA_AUTO_MERGE="${DELTA_AUTO_MERGE:-1}"
export ENV="${ENV:-dev}"

echo "[CONFIG] ENV=$ENV"
echo "[CONFIG] DELTA_AUTO_MERGE=$DELTA_AUTO_MERGE"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  src/gold/_marts/facts/fact_payments.py

echo "fact_payments build finished successfully"