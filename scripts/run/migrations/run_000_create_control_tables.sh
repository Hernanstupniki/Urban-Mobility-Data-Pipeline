#!/usr/bin/env bash
set -euo pipefail

echo "Starting migration 000: create Delta control tables..."

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../" && pwd)"
cd "$ROOT_DIR"

export ENV="${ENV:-dev}"

DELTA_PACKAGE="${DELTA_PACKAGE:-io.delta:delta-spark_2.12:3.1.0}"

# Rutas candidatas donde podría estar el script
CANDIDATES=(
  "scripts/migrations/000_create_control_tables.py"
  "scripts/migrations/bootstrap_control_tables.py"
  "scripts/migrations/000_bootstrap_control_tables.py"
  "migrations/000_create_control_tables.py"
  "migrations/bootstrap_control_tables.py"
  "bootstrap_control_tables.py"
)

PY_SCRIPT="${PY_SCRIPT:-}"

if [[ -n "$PY_SCRIPT" ]]; then
  if [[ ! -f "$PY_SCRIPT" ]]; then
    echo "ERROR: PY_SCRIPT fue seteado pero no existe: $PY_SCRIPT"
    exit 1
  fi
else
  for f in "${CANDIDATES[@]}"; do
    if [[ -f "$f" ]]; then
      PY_SCRIPT="$f"
      break
    fi
  done
fi

if [[ -z "$PY_SCRIPT" ]]; then
  echo "ERROR: No encontré el script Python."
  echo "Probé estas rutas:"
  printf ' - %s\n' "${CANDIDATES[@]}"
  echo
  echo "Solución rápida: ejecutá así indicando la ruta:"
  echo "PY_SCRIPT=RUTA/AL/SCRIPT.py ENV=$ENV ./scripts/run/migrations/run_000_create_control_tables.sh"
  exit 1
fi

echo "ROOT_DIR: $ROOT_DIR"
echo "ENV: $ENV"
echo "PY_SCRIPT: $PY_SCRIPT"

spark-submit \
  --packages "$DELTA_PACKAGE" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  "$PY_SCRIPT"

echo "Migration 000 finished OK."
