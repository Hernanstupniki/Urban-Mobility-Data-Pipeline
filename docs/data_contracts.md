# Data contracts

## Intentional source quality

The OLTP generator must continue to emit controlled dirty data. Its rates are validated in `[0, 1]`, logged at startup and can be reproduced with `RANDOM_SEED`.

Expected signals include null-like strings, missing optional values, whitespace/case noise, invalid contacts, spelling variants for vehicle types and currencies, semantically duplicated passengers and gateway payment retries, incomplete or outlying distances, distance in an incompatible trip status, temporal inversions and excessive delays, missing and out-of-range coordinates, driver/vehicle mismatch, high-precision numerics and accidental PII in `cancel_note`, rating comments and payment provider references.

PostgreSQL date and numeric columns remain typed and constrained. The generator therefore does not force textual dates, decimal separators or negative numbers into `TIMESTAMPTZ`/`NUMERIC` columns merely to imitate CSV dirt; Silver keeps defensive casts and validity checks for legacy or alternate Bronze sources where those representations can actually occur.

## Bronze

Bronze is append-only raw history partitioned by `load_date`. Source values are not normalized. Every row adds `source_system`, `raw_loaded_at`, `batch_id` and `load_date`.

## Silver

Silver retains normalized values and explicit quality evidence. A changed contractual field creates a new SCD2 version. Hash inputs include identifiers and sensitive free text that previously could change unnoticed: vehicle plate and deletion fields, payment provider reference, rating comment and trip cancellation note.

Trip quality flags are separate and non-null:

- `distance_present_in_invalid_status`
- `completed_missing_distance`
- `start_coordinates_invalid`
- `end_coordinates_invalid`
- `coordinates_missing`
- temporal ordering and distance-outlier flags
- `driver_vehicle_mismatch` and `vehicle_driver_unverifiable`
- measured acceptance/trip durations and explicit long-duration flags
- `cancel_note_contains_potential_pii`, with the detected contact suffix redacted

`has_distance_in_invalid_status` remains as a compatibility summary of the two distance-state signals. Silver never invents a valid coordinate or distance to hide a bad observation.

Passenger emails are normalized before duplicate comparison. `canonical_passenger_id` identifies the stable survivor and `potential_duplicate_passenger` preserves evidence instead of deleting a referenced OLTP identity. Payment rows sharing a non-null gateway reference receive the equivalent `canonical_payment_id`/`duplicate_provider_ref` treatment. Gold remaps trip passenger keys to the canonical passenger and excludes duplicate gateway retries from `fact_payments`.

Known vehicle-type aliases (`saloon`, `hatch back`, `motorcycle`, `moto`, `bike`) and common USD spellings are canonicalized. Their normalization flags distinguish a repaired source value from one that arrived clean. Invalid or unknown categories remain flagged rather than guessed.

Potential PII is detected conservatively using email, phone and explicit `contact:` patterns. Silver records a non-null evidence flag before redacting the risky portion. Gold retains the flag but omits the sensitive free-text source columns.

Missing fares remain null in Silver. Gold adds `fare_amount_analytical` using the current valid-fare median and records `fare_amount_was_imputed`; the original `fare_amount` is retained, and aggregates expose original and analytical totals separately so imputation never becomes invisible.

Every SCD2 table must have one current row per natural key, non-overlapping `[valid_from, valid_to)` intervals and a hash that excludes ingestion-only `batch_id`.

## Gold

Gold uses explicit projections rather than copying every Silver column.

- Snapshot dimensions contain the current contractual attributes.
- History dimensions mirror every Silver SCD2 version, including intermediate versions between Gold runs.
- SCD3 contains only meaningful `prev_*` business attributes and derives them from the immediate predecessor in complete Silver history.
- Facts omit accidental-PII fields (`cancel_note`, `provider_ref`) and contain validated conformed keys, using `0` for unknown dimension members.
- Daily aggregates are rebuilt from the fact snapshot, so corrections that move a trip between a date or driver remove the old contribution and add the new one.

Money remains decimal in facts and aggregates. Quality booleans are non-null where defined.

## GDPR

Passenger PII, driver PII and vehicle plates are anonymized in every available Bronze, Silver and Gold dimension version. Driver erasure derives all associated vehicle IDs. Accidental PII is nulled from trips, ratings and payments; legacy Gold fact columns are scrubbed if they still exist. Gold rebuilds do not reintroduce those free-text identifiers.

An audit completion marker is mandatory before the watermark advances. Unsupported subjects, missing required target tables, audit failures and purge failures cause a non-zero job failure and preserve the processing boundary.
