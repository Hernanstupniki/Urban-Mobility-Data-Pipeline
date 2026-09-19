#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_DIR="$SCRIPT_DIR"
while [[ "$RUN_DIR" != "/" && ! -f "$RUN_DIR/_common.sh" ]]; do
  RUN_DIR="$(dirname "$RUN_DIR")"
done
if [[ ! -f "$RUN_DIR/_common.sh" ]]; then
  echo "Unable to locate scripts/run/_common.sh from $SCRIPT_DIR" >&2
  exit 1
fi
source "$RUN_DIR/_common.sh"
run_spark_job "src/gold/_conformed/snapshot/dim_vehicle.py" "$@"
