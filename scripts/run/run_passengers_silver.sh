#!/usr/bin/env bash
set -euo pipefail

echo "Starting passengers Bronze → Silver ETL"

ENV="${ENV:-dev}"
echo "[CONFIG] ENV=$ENV"

export ENV="$ENV"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/silver/passengers_bronze_to_silver.py
