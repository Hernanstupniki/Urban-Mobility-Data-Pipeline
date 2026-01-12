#!/bin/bash
set -e

echo "Starting trips Bronze → Silver ETL"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars /home/hernan/jars/postgresql-42.7.8.jar \
  src/silver/test_silver.py

echo "Trips Silver ETL finished successfully"
