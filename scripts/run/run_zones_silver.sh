#!/bin/bash
set -e

echo "Starting zones Bronze → Silver ETL"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/silver/zones_bronze_to_silver.py

echo "Zones Silver ETL finished successfully"
