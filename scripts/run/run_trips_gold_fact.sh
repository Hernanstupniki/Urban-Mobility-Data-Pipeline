#!/bin/bash
set -e

echo "Starting Trips Gold (fact_trips) ETL"

export ENV="${ENV:-dev}"

# (No DB vars needed for fact_trips porque lee de Silver/Delta,
#  pero los dejo igual por consistencia con tus scripts)
export DB_HOST="${DB_HOST:-localhost}"
export DB_NAME="${DB_NAME:-mobility_oltp}"
export DB_USER="${DB_USER:-postgres}"

echo "[CONFIG] ENV=$ENV"
echo "[CONFIG] DB_HOST=$DB_HOST"
echo "[CONFIG] DB_NAME=$DB_NAME"
echo "[CONFIG] DB_USER=$DB_USER"
echo "[CONFIG] DB_PASSWORD="*****" (set in env)"

spark-submit \
  --packages io.delta:delta-spark_2.12:3.1.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  src/gold/trips/trips_gold_fact_trips.py

echo "Trips Gold fact_trips ETL finished successfully"
