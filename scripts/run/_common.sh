#!/usr/bin/env bash
set -Eeuo pipefail

RUN_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$RUN_COMMON_DIR/../.." && pwd)"
export REPO_ROOT
export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+:$PYTHONPATH}"

resolve_python_bin() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    if command -v "$PYTHON_BIN" >/dev/null 2>&1; then
      command -v "$PYTHON_BIN"
      return
    fi
    if [[ -x "$PYTHON_BIN" ]]; then
      printf '%s\n' "$PYTHON_BIN"
      return
    fi
    echo "PYTHON_BIN is not executable or on PATH: $PYTHON_BIN" >&2
    return 1
  fi

  # Airflow/Spark containers install PySpark + Delta in the image Python.
  # Do not prefer a host-mounted repo venv there.
  if [[ -f /.dockerenv && -n "${AIRFLOW_HOME:-}" ]]; then
    command -v python3
    return
  fi

  if [[ -n "${VIRTUAL_ENV:-}" && -x "$VIRTUAL_ENV/bin/python" ]]; then
    printf '%s\n' "$VIRTUAL_ENV/bin/python"
    return
  fi

  if [[ -x "$REPO_ROOT/venv/bin/python" ]]; then
    printf '%s\n' "$REPO_ROOT/venv/bin/python"
    return
  fi

  command -v python3
}

PYTHON_BIN_RESOLVED="$(resolve_python_bin)"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$PYTHON_BIN_RESOLVED}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYSPARK_PYTHON}"

run_spark_job() {
  local job_path="$1"
  shift
  run_python_job "$job_path" "$@"
}

run_spark_jdbc_job() {
  local job_path="$1"
  shift
  run_python_job "$job_path" "$@"
}

run_python_job() {
  local job_path="$1"
  shift
  cd "$REPO_ROOT"
  "$PYTHON_BIN_RESOLVED" "$job_path" "$@"
}
