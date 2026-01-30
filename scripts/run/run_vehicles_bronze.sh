#!/bin/bash
set -e

echo "Starting vehicles OLTP → Bronze ETL"

# Run Spark job with Delta Lake + Postgres JDBC driver
spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/bronze/vehicles_oltp_to_bronze.py

echo "Vehicles Bronze ETL finished successfully"
