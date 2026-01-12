#!/bin/bash
set -e

echo "Starting trips OLTP → Bronze ETL"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/bronze/test_bronze.py

echo "Trips Bronze ETL finished successfully"
