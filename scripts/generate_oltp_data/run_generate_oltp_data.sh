#!/bin/bash
set -e

echo "========================================"
echo "Starting OLTP Data Generator"
echo "========================================"

# --------------------------------------------------
# Environment variables
# --------------------------------------------------

export DB_HOST=localhost
export DB_NAME=mobility_oltp
export DB_USER=postgres

# Volume control
export N_TRIPS=10000
export N_PASSENGERS=2000
export N_DRIVERS=500

# GDPR / RTBF simulation
export GDPR_ERASURE_RATE=1.0
export N_GDPR_ERASURES_PER_RUN=10
export GDPR_ONLY_NON_DELETED=true
export GDPR_NOTE="simulated_erasure"

# --------------------------------------------------
# Run generator
# --------------------------------------------------

python3 scripts/generate_oltp_data/generate_oltp_data.py

echo "========================================"
echo "OLTP Data Generator finished successfully"
echo "========================================"
