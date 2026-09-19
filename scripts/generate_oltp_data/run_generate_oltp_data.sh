#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$REPO_ROOT/scripts/run/_common.sh"

# Defaults keep intentional dirty data enabled, while caller-provided values win.
: "${N_TRIPS:=10000}"
: "${N_PASSENGERS:=2000}"
: "${N_DRIVERS:=500}"
: "${BROKEN_RATE:=0.20}"
: "${GDPR_ERASURE_RATE:=0.10}"
export N_TRIPS N_PASSENGERS N_DRIVERS BROKEN_RATE GDPR_ERASURE_RATE

run_python_job "scripts/generate_oltp_data/generate_oltp_data.py" "$@"
