---
name: dirty-data-generation
description: Use when generating or changing synthetic Urban Mobility data, controlled data-quality anomalies, duplicate cases, timestamps, or PII/GDPR test scenarios.
---

# Synthetic dirty data

Apply to `scripts/generate_oltp_data` and tests that exercise expected anomalies. Read the existing generator, `docs/data_profile_for_bi.md`, `docs/data_contracts.md`, and the relevant tests before changing rates or categories. Keep randomness reproducible, preserve valid primary and foreign keys unless a scenario explicitly tests rejection, and keep anomaly flags traceable to their intended case. Do not use real personal data.

Check generated counts, anomaly proportions, date coverage, and downstream contract expectations. Rebootstrap or rewrite persistent data only when the task authorizes that operation.
