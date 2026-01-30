#!/bin/bash
set -e

echo "Starting passengers OLTP → Bronze ETL"

export ENV="${ENV:-dev}"

echo "[CONFIG] ENV=$ENV"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/bronze/passengers_oltp_to_bronze.py

echo "Passengers Bronze ETL finished successfully"
