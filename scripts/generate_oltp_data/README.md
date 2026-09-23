# Synthetic OLTP data

The generator creates mutable operational rows for trips, drivers, passengers, vehicles, payments, ratings, and GDPR requests. It intentionally includes partial observations and controlled quality anomalies so the downstream pipeline can validate and interpret them.

`actual_distance_km` is an observed tracking value. It may be present before a trip is completed or after a cancellation. This is valid operational data; Silver and Gold decide which distances count in analytical metrics.

The source database stores what the operational system observed. It does not contain pipeline watermarks or analytical corrections. Incremental control state lives in Delta under `data/<ENV>/_control/etl_control`, keyed by job name with its last loaded timestamp and status.

Run the generator through `run_generate_oltp_data.sh` or the manual `dag_generate_mock_data` DAG. Each run appends data, so it is not idempotent. Keep it out of production schedules. The wrapper exposes `N_TRIPS`, `N_PASSENGERS`, `N_DRIVERS`, `BROKEN_RATE`, and `GDPR_ERASURE_RATE` for test batches. Check the resulting counts, date coverage, and anomaly rates before using a batch for validation.

Bronze preserves source observations. Silver applies quality and history rules. Gold publishes business measures to the separate analytics database. This separation makes imperfect source data useful for testing rather than silently changing the OLTP to fit analytical expectations.

The source timestamp text includes ISO, year-first slash, day-first slash, and malformed examples. Apply `db/migrations/004_requested_at_source.sql` to an existing OLTP database before generating a new batch (for example, `psql -d mobility_oltp -f db/migrations/004_requested_at_source.sql`). The generator checks this before writing any data. Bronze retains the raw text; Silver parses known formats and falls back to the typed `requested_at` when text is invalid or inconsistent. The typed time remains the ordering reference. Names may arrive in lowercase, uppercase, or padded; Silver standardizes case and spacing while Bronze retains the original.

Driver response times vary with stable driver quality as well as demand, and vehicle type has a small effect on ratings. These effects make the driver scatter and vehicle rating chart informative without forcing deterministic outcomes. A four-day synthetic importer incident raises invalid timestamp-text rates; the typed event time stays valid.
