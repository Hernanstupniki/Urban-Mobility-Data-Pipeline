#!/bin/bash
set -e

echo "Starting vehicles Bronze → Silver ETL"
echo "[CONFIG] ENV=${ENV:-dev}"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  src/silver/vehicles_bronze_to_silver.py

echo "Vehicles Silver ETL finished successfully"
