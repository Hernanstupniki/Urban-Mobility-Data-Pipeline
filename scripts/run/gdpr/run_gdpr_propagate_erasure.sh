#!/bin/bash
set -e

echo "========================================"
echo "Starting GDPR Propagate Erasure -> Lake"
echo "========================================"

# Default ENV if not provided
ENV="${ENV:-dev}"
echo "[CONFIG] ENV=$ENV"
export ENV="$ENV"

# OLTP connection (usa lo que ya tengas en env; solo setea defaults si faltan)
export DB_HOST="${DB_HOST:-localhost}"
export DB_NAME="${DB_NAME:-mobility_oltp}"
export DB_USER="${DB_USER:-postgres}"
# DB_PASSWORD: debe venir seteado en tu entorno (como ya haces en otros jobs)

# Paths (defaults)
export BRONZE_PASSENGERS_PATH="${BRONZE_PASSENGERS_PATH:-data/${ENV}/bronze/passengers}"
export SILVER_PASSENGERS_PATH="${SILVER_PASSENGERS_PATH:-data/${ENV}/silver/passengers}"

# Optional
export ANON_NAME="${ANON_NAME:-ANONYMIZED}"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  gdpr/gdpr_propagate_erasure.py

echo "========================================"
echo "GDPR Propagate Erasure finished OK"
echo "========================================"
