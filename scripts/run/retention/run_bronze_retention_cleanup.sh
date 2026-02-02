#!/usr/bin/env bash
set -euo pipefail

echo "========================================"
echo "Starting Bronze Retention Cleanup (30d)"
echo "========================================"

export ENV="${ENV:-dev}"
export RETENTION_DAYS="${RETENTION_DAYS:-30}"

# PII tables por defecto
export TABLES="${TABLES:-passengers,drivers,ratings}"

export BRONZE_BASE_PATH="${BRONZE_BASE_PATH:-data/${ENV}/bronze}"

# 30 días = 720 horas
export VACUUM_RETAIN_HOURS="${VACUUM_RETAIN_HOURS:-720}"

# opcionales:
# export COUNT_BEFORE_DELETE=true
# export SKIP_VACUUM=true

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  retention/bronze_retention_cleanup.py

echo "========================================"
echo "Bronze Retention Cleanup finished OK"
echo "========================================"
