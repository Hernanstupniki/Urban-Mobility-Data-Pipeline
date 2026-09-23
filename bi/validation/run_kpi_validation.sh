#!/usr/bin/env bash
# SQL<->DAX slice ground truth (FASE 5). Usage: bi/validation/run_kpi_validation.sh
SQL_FILE="$(cd "$(dirname "$0")" && pwd)/kpi_validation.sql"
cd "$(dirname "$0")/../../infra/airflow" || exit 1
export PGPASSWORD=$(grep -E '^ANALYTICS_DB_PASSWORD=' .env | cut -d= -f2-)
docker compose exec -T postgres-analytics psql -U analytics -d mobility_dw \
  -F'|' -A -f - < "$SQL_FILE"
