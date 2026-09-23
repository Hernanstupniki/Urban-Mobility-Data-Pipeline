#!/usr/bin/env bash
# rebootstrap_dev.sh — CONTROLLED, DEV-ONLY reset of the whole demo environment.
#
# WHY this exists: after a generator redesign (business model + DQ channels)
# the historical dev dataset mixes two incompatible synthetic generations.
# Bronze watermarks are based on updated_at, so backdated history from the new
# generator would NEVER be ingested over the old lake: a clean bootstrap from
# an empty lake is required. Production/other ENVs must never use this script.
#
# WHAT it removes / regenerates:
#   REMOVES   : data/<ENV> lake (backed up as data/<ENV>_backup_<ts>, NOT deleted)
#   TRUNCATES : mobility_oltp {trips,payments,ratings,gdpr_requests,passengers,drivers,vehicles}
#               (zones kept: they come from db/mobility_oltp.sql seed)
#   REBUILDS  : control tables via migrations 000-003
#   REGENERATES: full ~90-day business dataset + GDPR requests (seeded, reproducible)
#
# After it finishes you must run the pipeline DAGs (urban_mobility_pipeline,
# then dag_gdpr_compliance) — the script triggers nothing in Airflow on purpose.
#
# Usage:  REBOOTSTRAP_CONFIRM=yes N_TRIPS=45000 ./rebootstrap_dev.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT" || exit 1

ENVIRONMENT="${ENV:-dev}"
DB_NAME_V="${DB_NAME:-$(grep -E '^DB_NAME=' infra/airflow/.env | cut -d= -f2- || true)}"
DB_NAME_V="${DB_NAME_V:-mobility_oltp}"

if [ "${REBOOTSTRAP_CONFIRM:-}" != "yes" ]; then
  echo "REFUSING: set REBOOTSTRAP_CONFIRM=yes to run this destructive dev-only script." >&2
  exit 1
fi
if [ "$ENVIRONMENT" != "dev" ]; then
  echo "REFUSING: rebootstrap is only allowed for ENV=dev (got '$ENVIRONMENT')." >&2
  exit 1
fi
if [ "$DB_NAME_V" != "mobility_oltp" ]; then
  echo "REFUSING: unexpected database '$DB_NAME_V'." >&2
  exit 1
fi

export PGPASSWORD=$(grep -E '^DB_PASSWORD=' infra/airflow/.env | cut -d= -f2-)
TS=$(date +%Y%m%d_%H%M%S)

echo "== 1/5: back up lake to data/${ENVIRONMENT}_backup_${TS} =="
if [ -d "data/$ENVIRONMENT" ]; then
  mv "data/$ENVIRONMENT" "data/${ENVIRONMENT}_backup_${TS}"
  echo "   lake moved (not deleted); remove manually after validation"
fi

echo "== 2/5: truncate synthetic tables (zones kept) =="
psql -h 127.0.0.1 -U postgres -d mobility_oltp -v ON_ERROR_STOP=1 <<SQL
TRUNCATE mobility.trips, mobility.payments, mobility.ratings,
         mobility.gdpr_requests, mobility.passengers,
         mobility.drivers, mobility.vehicles
RESTART IDENTITY CASCADE;
SQL

echo "== 3/5: recreate lake control tables (migrations 000-003) =="
# migrations only need the control paths; run them with the lake absent so
# 001-003 no-op on missing silver tables and re-run cleanly afterwards.
for m in 000_create_control_tables 001_scd2_trips 002_reconcile_silver_contracts 003_expand_quality_coverage; do
  DB_HOST=127.0.0.1 DB_NAME=mobility_oltp DB_USER=postgres DB_PASSWORD="$PGPASSWORD" \
    scripts/run/migrations/run_$m.sh >/dev/null 2>&1 || true
done

echo "== 4/5: regenerate full dataset (seed ${RANDOM_SEED:-20260921}) =="
N_TRIPS="${N_TRIPS:-45000}" RANDOM_SEED="${RANDOM_SEED:-20260921}" \
GDPR_ERASURE_RATE="${GDPR_ERASURE_RATE:-1.0}" \
DB_HOST=127.0.0.1 DB_NAME=mobility_oltp DB_USER=postgres DB_PASSWORD="$PGPASSWORD" \
  scripts/generate_oltp_data/run_generate_oltp_data.sh

echo "== 5/5: summary =="
psql -h 127.0.0.1 -U postgres -d mobility_oltp -tA <<SQL
SELECT 'trips='||count(*) FROM mobility.trips;
SELECT 'dates='||count(DISTINCT requested_at::date) FROM mobility.trips;
SELECT 'payments='||count(*) FROM mobility.payments;
SELECT 'ratings='||count(*) FROM mobility.ratings;
SELECT 'gdpr_requests='||count(*) FROM mobility.gdpr_requests;
SQL
echo "DONE. Next: trigger urban_mobility_pipeline, then dag_gdpr_compliance."
