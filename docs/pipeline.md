# Urban Mobility pipeline

The source intentionally produces incomplete and inconsistent operational data. This is a test contract, not a generator defect: Bronze preserves the observation, Silver types and classifies it, and Gold applies explicit analytical rules.

## Runtime order

Run commands from any directory; each wrapper resolves the repository root.

1. Set `ENV`, `DB_PASSWORD`, `POSTGRES_JAR` and, for GDPR, `GDPR_HASH_KEY`.
2. Run migrations `000`, `001`, `002`, then `003`.
3. Generate or update OLTP data.
4. Run all Bronze jobs.
5. Run Silver dimensions first (especially vehicles), then trips, payments and ratings. Trips use the current vehicle dimension to validate the driver/vehicle pair.
6. Build Gold static dimensions: date, payment method and zone.
7. Build Gold snapshot, history and SCD3 dimensions.
8. Build `fact_trips`, then `fact_payments` and `fact_ratings` (`fact_ratings` validates `trip_key` against the freshly rebuilt `fact_trips`).
9. Build daily aggregates.
10. Run GDPR propagation after OLTP erasure requests and before publishing downstream extracts.
11. Run Bronze and Silver retention on the agreed operational schedule.

All Spark wrappers use Delta `3.1.0`, matching Spark `3.5.x`. JDBC jobs require an explicit readable `POSTGRES_JAR`; the repository does not assume a user-specific path.

## Required configuration

- `ENV`: isolated data environment, default `dev`.
- `DATA_ROOT`: lake root, default `data`.
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`: PostgreSQL access.
- `POSTGRES_JAR`: PostgreSQL JDBC jar for Bronze and GDPR wrappers.
- `GDPR_HASH_KEY`: private secret of at least 16 characters. There is no predictable fallback.

Optional Spark tuning is available through `SPARK_LOG_LEVEL`, `SPARK_SHUFFLE_PARTITIONS`, `SPARK_DEFAULT_PARALLELISM` and `SPARK_MAX_PARTITION_BYTES`.

## Re-execution semantics

- Bronze uses a deterministic Delta transaction version derived from the prior watermark. A retry after a committed append does not append the same batch again.
- Silver advances its watermark only after a successful merge; failed attempts preserve `last_success_ts`.
- Gold dimensions and marts are deterministic rebuilds from the current explicit contracts. This also repairs a trip moved to a different date or driver without leaving stale aggregates.
- GDPR reads its timestamp watermark inclusively and excludes only requests with a successful audit completion marker. Requests sharing one timestamp cannot be skipped.

## Retention and physical GDPR purge

Bronze defaults to 14 days and Silver closed SCD2 history to 30 days. Both include zones. Gold is vacuum-only (60-day file window): it never deletes rows because it is rebuilt deterministically from Silver, and its vacuum also caps time-travel depth. Table names and paths are validated to stay inside `DATA_ROOT/ENV`.

Delta updates remove personal data from the current snapshot immediately, but old files remain readable through time travel until `VACUUM`. GDPR therefore vacuums every touched table with `GDPR_VACUUM_HOURS` (default 168). A shorter window is permitted only with both `ENV=dev` and `GDPR_UNSAFE_VACUUM=1`, because active readers may still reference older files. The legal purge window should be configured explicitly for each deployed environment.
