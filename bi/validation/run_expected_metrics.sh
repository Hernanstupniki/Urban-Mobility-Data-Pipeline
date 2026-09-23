#!/usr/bin/env bash
# Reconciles reporting.* ground-truth KPIs. Usage: bi/validation/run_expected_metrics.sh
SQL_FILE="$(cd "$(dirname "$0")" && pwd)/expected_metrics.sql"
cd "$(dirname "$0")/../../infra/airflow" || exit 1
docker compose exec -T postgres-analytics psql -U analytics -d mobility_dw \
  -F'|' -A < "$SQL_FILE"
