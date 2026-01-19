#!/bin/bash
set -euo pipefail

echo "Running migration: 001_scd2_trips.py"

export ENV="${ENV:-dev}"
export DELTA_AUTO_MERGE="${DELTA_AUTO_MERGE:-1}"

echo "[CONFIG] ENV=$ENV"
echo "[CONFIG] DELTA_AUTO_MERGE=$DELTA_AUTO_MERGE"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.databricks.delta.schema.autoMerge.enabled=$DELTA_AUTO_MERGE \
  migrations/001_scd2_trips.py

echo "Migration 001 finished successfully"
