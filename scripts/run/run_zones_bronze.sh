#!/bin/bash
set -e

echo "Starting zones OLTP → Bronze ETL"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/bronze/zones_oltp_to_bronze.py

echo "Zones Bronze ETL finished successfully"
