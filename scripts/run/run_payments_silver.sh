#!/bin/bash
set -e

echo "Starting payments Bronze → Silver ETL"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/silver/payments_bronze_to_silver.py

echo "Payments Silver ETL finished successfully"
