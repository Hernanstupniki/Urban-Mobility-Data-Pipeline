#!/bin/bash
set -e

echo "Starting dim_date (Gold _conformed) build"

# Defaults
export ENV="${ENV:-dev}"

# Optional overrides (uncomment if you want fixed range)
# export DATE_START="${DATE_START:-2025-01-01}"
# export DATE_END="${DATE_END:-2026-12-31}"

# Padding around inferred range
export PAD_DAYS_BEFORE="${PAD_DAYS_BEFORE:-30}"
export PAD_DAYS_AFTER="${PAD_DAYS_AFTER:-30}"

# Fallback window if Silver is empty / not found
export FALLBACK_DAYS_BACK="${FALLBACK_DAYS_BACK:-365}"
export FALLBACK_DAYS_FORWARD="${FALLBACK_DAYS_FORWARD:-365}"

echo "[CONFIG] ENV=$ENV"
echo "[CONFIG] PAD_DAYS_BEFORE=$PAD_DAYS_BEFORE"
echo "[CONFIG] PAD_DAYS_AFTER=$PAD_DAYS_AFTER"
echo "[CONFIG] FALLBACK_DAYS_BACK=$FALLBACK_DAYS_BACK"
echo "[CONFIG] FALLBACK_DAYS_FORWARD=$FALLBACK_DAYS_FORWARD"
echo "[CONFIG] DATE_START=${DATE_START:-<infer from silver>}"
echo "[CONFIG] DATE_END=${DATE_END:-<infer from silver>}"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  src/gold/_conformed/dim_date.py

echo "dim_date build finished successfully"
