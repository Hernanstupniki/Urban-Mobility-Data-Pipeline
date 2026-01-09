#!/bin/bash
set -e

echo "Starting trips OLTP → RAW ETL"

spark-submit \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  bronze/trips_oltp_to_bronze.py

echo "Trips RAW ETL finished successfully"
