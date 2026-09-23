# Synthetic OLTP data

The generator creates mutable operational rows for trips, drivers, passengers, vehicles, payments, ratings, and GDPR requests. It intentionally includes partial observations and controlled quality anomalies so the downstream pipeline can validate and interpret them.

`actual_distance_km` is an observed tracking value. It may be present before a trip is completed or after a cancellation. This is valid operational data; Silver and Gold decide which distances count in analytical metrics.

The source database stores what the operational system observed. It does not contain pipeline watermarks or analytical corrections. Incremental control state lives in Delta under `data/<ENV>/_control/etl_control`, keyed by job name with its last loaded timestamp and status.

Run the generator through `run_generate_oltp_data.sh` or the manual `dag_generate_mock_data` DAG. Each run appends data, so it is not idempotent. Keep it out of production schedules. The wrapper exposes `N_TRIPS`, `N_PASSENGERS`, `N_DRIVERS`, `BROKEN_RATE`, and `GDPR_ERASURE_RATE` for test batches. Check the resulting counts, date coverage, and anomaly rates before using a batch for validation.

Bronze preserves source observations. Silver applies quality and history rules. Gold publishes business measures to the separate analytics database. This separation makes imperfect source data useful for testing rather than silently changing the OLTP to fit analytical expectations.
