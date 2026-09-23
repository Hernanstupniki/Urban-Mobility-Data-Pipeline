# Urban Mobility — Power BI layer (PBIP)

Serving-layer consumption stage of the pipeline:

```
OLTP -> Bronze -> Silver (SCD2) -> Gold (star + hist + temporal lookups) -> PostgreSQL Analytics (mobility_dw.reporting) -> Power BI
```

This folder is a **Power BI Project (PBIP)** in open text format: semantic
model as **TMDL**, report as **PBIR**. It is versionable and diffable in Git.

## Contents

```text
bi/
├── UrbanMobility.pbip                  # entry point: open this in Power BI Desktop
├── UrbanMobility.SemanticModel/        # TMDL definition (12 tables, star schema)
├── UrbanMobility.Report/               # PBIR definition (4 pages, 39 visuals) + UrbanLight theme
├── validation/
│   ├── check_model.py                  # TMDL names/reserved words/refs/duplicates/single-direction
│   ├── check_report.py                 # every visual query resolves against the model; layout collisions
│   ├── validate_schemas.py             # every PBIR/PBIP file vs the OFFICIAL Microsoft json-schemas ($schema)
│   ├── gen_report.py                   # the generator that produced the report files (spec as code)
│   ├── fix_tmdl_names.py / fix_tmdl_format.py  # idempotent TMDL normalizers (Desktop-exact style)
│   ├── expected_metrics.sql            # ground truth of every KPI (no filters), straight from PostgreSQL
│   ├── run_expected_metrics.sh         # executes it against the postgres-analytics container
│   ├── kpi_validation.sql              # SQL↔DAX ground truth for 5 filtered slicer states (S1..S5)
│   └── run_kpi_validation.sh           # executes the slice validation
└── .gitignore
```

## How to open

1. Power BI Desktop ≥ 2.13x (this was authored and validated against 2.157).
2. Open `bi/UrbanMobility.pbip`.
3. On first refresh Power BI asks for database credentials — they live only in
   `infra/airflow/.env` (`ANALYTICS_DB_USER=analytics`, `ANALYTICS_DB_PASSWORD`):
   - Data source resolution dialog → Database → **User** `analytics` + the .env password.
   - Server `localhost:5433`, database `mobility_dw` are already in the M code.
   - The password is never stored in this folder, the model or Git.
4. On first refresh Desktop also asks to approve a native query for fact_trips.
   It is a read-only SELECT on reporting.fact_trips that computes four
   UTC−3 columns in PostgreSQL for a fast import. Choose **Run**.
5. Close and reopen after the credential prompt so Desktop applies everything.

The analytics Postgres must be running: `docker compose --profile spark-cluster up -d`
from `infra/airflow/` (service `postgres-analytics`, host port 5433).

## Semantic model (star, v2)

Facts (grain validated against PostgreSQL): `fact_trips` (trip_id),
`fact_payments` (payment_id; trip reuses exist → not 1:1), `fact_ratings`
(rating_id, 1 per trip).

Dimensions: `dim_date`, `dim_payment_method`, `Zone` (pickup), `Dropoff Zone`
(role-playing copy of reporting.dim_zone, no source change), `dim_driver`,
`dim_passenger`, `dim_vehicle`, plus the hidden **`Trip`** bridge.

**`Trip` bridge (conformance):** a hidden, trip-grain table holding
`trip_id` + every FK (passenger/driver/vehicle/pickup/dropoff/date keys) built
in M from `reporting.fact_trips`. `fact_trips` 1:*→ `Trip` and dims 1:*→
`fact_payments`/`fact_ratings` route **through** `Trip`, so a zone/driver/
vehicle slicer filters trips *and* payments/ratings coherently, each
dim→fact pair has exactly one filter path, and **every relationship in the
model is many-to-one single-direction** (the earlier driver↔vehicle
`bothDirections` hack and the fact→fact inactive relationships are gone).

**SCD2 / temporal keys (Gold contract):** the serving facts expose two key
families per SCD2 entity:
- `driver_key`, `passenger_key`, `vehicle_key` — business/current keys →
  join the current-view dims (what this model uses).
- `driver_skey`, `passenger_skey`, `vehicle_skey` (+ ratings) — surrogate keys
  resolved by Gold temporal lookup (`event ∈ [valid_from, valid_to)` against
  `dim_*_hist`). Point-in-time state before the 2026-09-20 bootstrap is
  unknown by contract (no retroactive attribution): skey `0` = unknown.
  Coverage today is therefore ~0 for bootstrap-era facts and grows with every
  post-bootstrap event; this is measured, not hidden.

**Decision: `dim_*_hist` is NOT imported into Power BI.** Exposing it would
duplicate driver/vehicle/passenger slicers while 99.98% of current facts
resolve to the unknown member — noise, not history. The current-view star
plus the Trip bridge is the simplest model with correct behaviour today.
To switch to point-in-time later: import `dim_*_hist` (already published
ready if needed), repoint `Trip` to the `*_skey` columns and hide the
current dims — one edge change per dimension, no parallel slicers.

**Timezone:** `reporting.*` timestamps are **UTC** (`request_date_key` /
`dim_date` are UTC-calendar). The operational zone is America/Argentina/
Buenos_Aires (fixed UTC−3 since 2019, no DST), so the fact_trips M partition imports PostgreSQL-derived local
columns: `started_hour` (local, the axis of hour-of-day visuals),
`requested_local_hour` (local, use for rush-hour filters: 7–9 ∪ 17–20),
`request_local_date` (local calendar day, hidden grain) and `started_local`
(technical). Day-level trends via `dim_date` remain **UTC-calendar** by
design; that caveat is documented on the Data Health page.

Unknown members: key `0` is materialized in the dims (`UNKNOWN` driver, etc.)
so unassigned trips group cleanly and are interpreted on Data Health as
legitimate domain behaviour, never as errors.

**PII policy:** `full_name`, `email`, `phone`, `license_number`, `plate_number`
and comment free-text are **not imported at all** (excluded in each M
`SelectColumns`). Entities are analysed by id/keys only; GDPR status uses the
`is_deleted` flag. Compliance visibility survives; PII never enters BI.

All measures live in the hidden **`_Measures`** table (folders `Operations`,
`Revenue`, `Experience`, `Data Quality`, `Compliance`, `Technical`). The
literal name `Measures` is rejected by Power BI's model schema validator
(reserved keyword), so the conventional `_Measures` is used. No hardcoded
constants: everything recomputes under filters. Notable measure decisions:
- `GMV` = real fares only; `Analytical GMV` includes median-imputed fares;
  the difference is its own measure (`Imputed Fare Amount`).
- Delays use **median / P90** (outliers make the mean misleading).
- `Revenue Leakage Trips/Amount` = completed trips with no `paid` payment.
- `Duplicate Payment Exposure` = 0 by design: Gold already excludes duplicate
  gateway retries; the measure is a regression canary, not "overcharge"
  (folder `Technical`).
- `Last Gold Load` = Delta build freshness (folder `Technical`).
- `Data Trust Rate = 1 - DQ Issue Rate`, an internal metric over explicit
  Silver flags (documented as such on the page).

## Report pages

| Page | Decision question |
|---|---|
| Operations | Where and when is demand highest, and where do riders wait longest? |
| Revenue | Where is value generated, collected, and leaked? |
| Experience | How do delay, vehicle type, and cancellation reasons relate to experience? |
| Data Health & Compliance | Can the metrics be trusted, and are erasures visible? |

Theme UrbanLight uses a light canvas, consistent cards, and a restrained teal accent.

## Phase 6 report layout

The report has 39 visuals across four 1280 × 720 pages, down from 57.
Each page uses one date slicer, four primary KPI cards, a clear title,
and larger native Power BI visuals. Zone, vehicle, and method breakdowns
can be selected directly in their charts or filtered through the filter pane.

| Page | KPI row | Main analytical views |
|---|---|---|
| Operations | Trips, completion rate, cancellation rate, P90 acceptance delay | Zone × local request hour Matrix heatmap, hourly demand, zone delay |
| Revenue | GMV, collected amount, collection rate, leakage amount | Zone GMV, payment method × status, zone leakage, driver GMV |
| Experience | Average rating, rating count, P90 acceptance delay, cancellation rate | Driver-level delay vs rating, rating distribution, vehicle rating, cancellation reasons |
| Data Health | Trust rate, issue trips, imputed fare rate, erased subjects | DQ trend, erasure/trust matrix, Gold freshness, quality impact note |

The report generator is the source of truth for these visuals. Run
python3 bi/validation/gen_report.py before validation. The original
57-visual inventory and the KEEP/REMOVE/REPLACE/NEW decisions are in
bi/validation/phase6_visual_inventory.csv and
bi/validation/phase6_visual_changes.csv.

## Filter semantics (what each selection legitimately moves)

| Filter source (column) | Operations measures | Revenue measures | Experience measures | Data Health measures |
|---|---|---|---|---|
| `dim_date[date]` (UTC calendar) | all trip KPIs | payments KPIs (direct `payment_date_key`) | ratings KPIs (direct `rating_date_key`) | all flag/DQ counts |
| `Zone[city]` / `Zone[zone_name]` (pickup) | trip KPIs + scorecard rows | via `Trip` bridge (payments of filtered trips) | via `Trip` bridge | DQ counts of filtered trips |
| `dim_vehicle[vehicle_type]` | trip KPIs | via `Trip` bridge | ratings KPIs via `Trip` bridge | DQ counts |
| `fact_payments[status]` (filter pane) | — (payments-only column) | payment KPIs only | — | payment-side DQ only |
| visual filter `fact_trips[requested_local_hour] ∈ {7,8,17,18,19}` = RUSH | trip KPIs, delay KPIs | via `Trip` bridge | via `Trip` bridge | DQ counts |

Rules encoded above: every measure group must respond to every dimension via
exactly **one** filter path (no bidirectional ambiguity), and payment/rating
KPIs never leak unfiltered zone/vehicle selections (that is what `Trip`
guarantees).

## Validation (SQL ground truth vs dashboard)

### Global — `bash bi/validation/run_expected_metrics.sh`
Baseline 2026-09-22 (post-SCD-rebuild) with **no filters on**, Power BI must reproduce:

| Measure | Value | Measure | Value |
|---|---|---|---|
| Trips | 48,000 | CompletionRate | 89.5% |
| Completed | 42,982 | CollectionRate | 80.1% |
| Cancelled | 5,003 | BillingGapPct | 11.69% |
| Active | 15 | DataTrustRate | 78.0% |
| MedianDelayMin | 5.1 | ImputedFareRate | 17.9% |
| P90DelayMin | 20.9 | AverageRating | 4.03 |
| GMV | $948,774 | FiveStarShare | 32.3% |
| AnalyticalGMV | $1,120,471 | DQIssueTrips | 10,566 |
| CollectedAmount | $897,541 | RevenueLeakageTrips | 9,703 |
| BilledAmount | $1,120,737 | RevenueLeakageAmount | $192,027 |
| DuplicatePaymentExposure | $0 | RatingCount | 25,669 |
| UnknownDriverTrips | 0 | PIIRecordsRedacted | 2,053 |
| ErasedSubjects | 5 | LastGoldLoad | 2026-09-22 04:14 UTC |

(Active = open-trip snapshot count at Gold build time; it is *not*
CompletionRate's complement — cancelled+completed+requested∪accepted states,
so 48,000 − 42,982 − 5,003 = 15. Interpreted as "currently in flight".)

### Filtered slices — `bash bi/validation/run_kpi_validation.sh`
Reproduce each slicer state in Desktop (filter pane / slicer selection) and
compare. Rush hour = local 7–9 ∪ 17–20 ⇒ filter `requested_local_hour`
∈ {7,8,17,18,19}.

| Slice | Trips | CompletionRate | Rating | Delay/amount |
|---|---|---|---|---|
| S1 `date=2026-09-19` (UTC) | 534 | 90.1% | 4.18 | P90 14.8 min |
| S2 `Zone = Manhattan` (pickup) | 3,509 | 89.2% | — | GMV $63,605; avg duration 27.2 min |
| S3 RUSH (local 7-9/17-20) | 16,496 | cancel 10.4% | — | median 6.9 / P90 26.6 min |
| S4 `payment status paid ∩ Card` | — | — | — | collected $383,332 / billed $532,042 = 72.0% |
| S5 `driver_id = 152` (busiest) | 120 | 90.0% | 4.05 | P90 19.4 min |

Structural checks (headless): `python3 bi/validation/check_model.py`,
`check_report.py`, `validate_schemas.py`, `gen_report.py`.

After opening the .pbip, verify in Power BI: totals above match; date/zone/
driver/vehicle slicers change numbers consistently per the matrix; no double
counting (matrix `Zone scorecard` trips sum equals `Trips`); no
ambiguous-filter warnings (every filter path is single — `Trip` bridge makes
dim→payments/ratings unique).

## Known limitations & technical debt

1. **Temporal sparsity** (fixed in FASE 3): the generator now seeds a rolling
   90-day window; long time-series exist. Hour-of-day visuals use PostgreSQL-derived
   local columns; `dim_date` trends are UTC-calendar (documented caveat).
2. **Publishing freshness** (`control.publish_state`) is not yet exposed as a
   BI-visible table; the page approximates freshness with `Last Gold Load`
   (Delta build time, folder `Technical`). Debt: publish `control.publish_state`
   into `reporting`; then add GDPR processing-time/SLA measures.
3. **Point-in-time BI view** is future-ready, not active: `*_skey` columns are
   published and populated post-bootstrap; `dim_*_hist` is deliberately not
   imported (see SCD2 section for the one-edge-change path to enable it).
4. **Report visuals were authored as PBIR text** (validated structurally and
   by reference-checking, but not rendered headlessly). If Desktop flags any
   non-blocking visual warning after opening, regenerating via
   `bi/validation/gen_report.py` + a save fixes/normalises it.
5. Optional polish not included: page-navigation buttons, drillthrough
   trip-detail pages, field parameters for dynamic axis switching.
