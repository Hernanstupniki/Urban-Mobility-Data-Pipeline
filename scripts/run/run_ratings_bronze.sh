#!/bin/bash
set -e

echo "Starting ratings OLTP → Bronze ETL"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/bronze/ratings_oltp_to_bronze.py

echo "Ratings Bronze ETL finished successfully"
