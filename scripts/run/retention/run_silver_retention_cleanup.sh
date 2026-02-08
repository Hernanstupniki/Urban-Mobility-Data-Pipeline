#!/usr/bin/env bash
set -euo pipefail

echo "========================================"
echo "Starting Silver Retention Cleanup (SCD2)"
echo "========================================"

ENV="${ENV:-dev}"
echo "[CONFIG] ENV=$ENV"
export ENV="$ENV"

# Optional overrides (env vars):
#   SILVER_BASE_PATH (default: data/$ENV/silver)
#   TABLES           (default: passengers,drivers,ratings)
#   RETENTION_DAYS   (default: 365)
#   VACUUM_RETAIN_HOURS (default: RETENTION_DAYS*24)
#   SKIP_VACUUM      (default: false)
#   UNSAFE_VACUUM    (default: 0)
#   COUNT_BEFORE_DELETE (default: false)

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  retention/silver_retention_cleanup.py

echo "========================================"
echo "Silver Retention Cleanup finished OK"
echo "========================================"
