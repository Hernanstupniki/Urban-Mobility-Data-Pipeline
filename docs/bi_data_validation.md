# BI Data Validation — Before vs After (generator redesign)

Date: 2026-09-21. Scope: FASE 3 deliverable — evidence that the synthetic
dataset now carries plausible business structure while keeping deliberate,
INDEPENDENT data-quality noise, full incremental semantics and end-to-end
reconciliation OLTP → Bronze → Silver → Gold → reporting.

Baseline (BEFORE): `docs/data_profile_for_bi.md`. All AFTER numbers below are
reproducible with the scripts in `tmp/` (kept out of git) / the queries quoted
here; the environment after a controlled dev rebootstrap + 2 pipeline runs.

## 1. Temporality

| | BEFORE | AFTER |
|---|---|---|
| distinct dates | **5** | **90** |
| trips/day | 5 batches of 3k-10k | min 379 · p25 453 · p50 488 · p75 527 · max 817 (event days) · sd 80 |
| day-of-week signal | artifact | Mon 6,636 … Fri 6,797, Sat 6,325 — weekday > weekend per design (fewer weekend *days* in window) |
| hour-of-day | 6 hours used | full 24h with weekday AM/PM peaks and weekend night-shift curves |
| future timestamps | n/a | impossible (last-day clamp) |

⚠️ BI note: `reporting.*` stores UTC instants. Hour-of-day visuals must apply
the business timezone (`requested_at AT TIME ZONE 'UTC' AT TIME ZONE
'America/Argentina/Buenos_Aires'`) or a model column; in UTC the peaks appear
shifted (10-11 and 20-21 = local 7-9 and 17-20). This is presentation, not data.

## 2. Statuses (defensible semantics)

| | BEFORE | AFTER |
|---|---|---|
| completed / cancelled (of **terminal**) | 41.0% / 41.1% | **89.5% / 10.5%** |
| active snapshot (requested/accepted/started) | 18% spread everywhere | 15 trips (0.03%), all on the last days — realistic |

Terminal ratios are what BI should show; "Active Trips" is a snapshot KPI, and
it is now tiny because status progression only finalizes trips older than 2
days (recent open requests stay open, like reality).

## 3. Zone demand differentiation

| | BEFORE | AFTER |
|---|---|---|
| trip-count CV across 21 zones | **2.3%** (flat) | **34.8%** (min 1,114 · max 3,288) |
| role signatures | none | downtown: most trips, short km, high delay; airport/residential: longer km; nightlife: night-weighted |

Top zones: Manhattan 3,288 · Loop 3,213 · SF Downtown 3,167. Bottom: ~1,100.
Per-zone fare/delay/km now vary consistently with role (see §queries).

## 4. Business correlations (with DQ noise ON)

| relation | BEFORE | AFTER all rows | AFTER clean* |
|---|---|---|---|
| fare ↔ distance | −0.004 | 0.68 | **0.948** |
| duration ↔ distance | 0.02 | 0.197 | **0.834** |
| delay rush vs off-peak (local tz) | inverted/noise | — | p50 **5.6 vs 3.9**, p90 **18.1 vs 13.2** min |
| driver avg-delay ↔ avg-rating (≥30 ratings, 525 drivers) | 0.03 | — | **−0.109** (right sign; attenuated by driver-quality confounding and noise — by design, not a knob) |
| payment status ↔ method | identical thirds | — | wallet paid 84.4% · cash 91.8% · card 72.0%; overall collection **80.1%** |
| rating distribution | uniform 19.9-20.8% each | — | 0.5 / 5.2 / 17.3 / **44.8 / 32.3** → mean **4.03** (high-skewed, realistic) |

\* clean = rows without any DQ/contamination flag. The gap all-vs-clean
(0.197 → 0.834 on duration) is itself the demonstration that the DQ channels
are what decorrelate the raw data — exactly the story the Data Health page
should tell.

## 5. Synthetic events (visible, bounded)

Baseline weekday ~420-530 trips/day. Event days from the generated calendar:

| event | days | trips | delay p50 | fare avg |
|---|---|---|---|---|
| marathon (days 28-29) | Jul 22-23 | 665 / 690 | 6.5 / 5.7 | 26.6 / 27.4 |
| rain storm (45-49) | Aug 9-11 | 563 / 622 / 527 | 5.8 / 7.1 / 8.0 | **27.7 / 31.7 / 29.2** (surge) |
| festival (62-64) | Aug 25-26 | **738 / 817** | 5.8 / 6.1 | 28.2 / 28.1 |

Events move the metrics without dominating the 90 days (≤1.7× baseline, 5 of
90 days each).

## 6. Data quality: independence (the FASE 3 correction)

### 6.1 Distribution of anomaly kinds (monotonically decreasing = independent, not stacked)

Trips (8 flag channels, 48,000 rows):

| flags/row | trips | % |
|---|---|---|
| 0 | 30,775 | **64.1** |
| 1 | 14,561 | 30.3 |
| 2 | 2,491 | 5.2 |
| 3 | 168 | 0.35 |
| 4 | 5 | 0.01 |

Passengers (13 text channels, 2,161 rows): **52.0 / 32.8 / 11.1 / 4.1%** for
0/1/2/3+. Lower clean-% is expected (more channels per row); the shape is the
same monotonically-decreasing Poisson-binomial shape.

### 6.2 Configured vs observed (global magnitudes preserved)

| channel | configured | observed |
|---|---|---|
| coordinates missing | 10% | 9.85% |
| completed w/o ended_at | 2% (of completed) | 1.91% |
| distance outlier | 3% (of completed) | 3.02% |
| fare NULL → imputed | 18% | 17.95% |
| driver/vehicle mismatch | 2% | 1.89% |
| delay outlier | 2% corrupt + real rush tail | 6.0% (>60min) — composition explained, not a bug |

### 6.3 Co-occurrence = coincidence (outside the incident) and 100% correlated (inside)

- Name uppercase × name padded (all rows, incident cohort excluded): observed
  joint **4** vs expected-under-independence **3.1** → no artificial coupling.
- **Documented incident window** (`importer_case_whitespace`, passengers
  created 2026-08-13..16, 82 rows): 34/34 uppercase rows are ALSO padded —
  correlation there is intentional, single, dated, and the only non-independent
  channel in the generator.

### 6.4 SCD2 merge-key fix holds after regeneration

Silver trips: 48,000 versions = 48,000 current = 48,000 distinct ids, **0**
current-duplicates. Silver ratings: 25,669 = 25,669, **0** duplicate current
`rating_id`s (the pre-fix generator had 4,566).

## 7. Incremental run proof (second DAG after +3,000 trips)

Bronze `load` events of the incremental run (from Airflow task logs):

| entity | rows ingested |
|---|---|
| zones | **NO_DATA** |
| trips | **3,000** |
| payments | 2,719 |
| ratings | 1,611 |
| passengers | 276 (updates+new) |
| drivers | 84 · vehicles 25 |

Bronze trips went 8→9 Delta commits, i.e. exactly one delta append — the 45,000
previous rows were **not** re-read. Silver/Gold kept their designed semantics
(SCD2 merge; deterministic Gold rebuild). No duplicates anywhere (see 6.4).

## 8. End-to-end reconciliation

| layer | value |
|---|---|
| OLTP trips | 48,000 |
| Bronze distinct trip_id | 48,000 |
| Silver current | 48,000 |
| Gold fact_trips | 48,000 |
| reporting.fact_trips | 48,000 (distinct 48,000) |
| ratings | OLTP 25,669 = gold = reporting |
| payments | OLTP 43,352 − **389** duplicate provider_refs (contract-excluded) = 42,963 = gold = reporting ✓ exact |
| publish_state | 11/11 tables, fresh batches |
| publish_log FAILs | 4 total, ALL from 2026-09-20 (pre-fix, historical); zero since |

## 9. Known limitations / deliberate non-goals

- Driver-level rating↔delay correlation is modest (−0.11): intentional —
  driver quality confounds it; we did not tune to a target coefficient.
- `reporting.*` timestamps are UTC (BI presentation note in §1).
- Fare imputation (18%) is a *policy*: Gold exposes both original and
  analytical measures separately; the dashboard must never blend them.
- GDPR SLA processing-time metrics still not available in serving (needs
  `control.publish_state`/`gdpr_audit` exposure) — tracked as model debt.
- Payment "duplicate exposure" is zero in reporting BY DESIGN (dedup happens
  in Gold); the raw duplicate count (389) is visible only via this document
  and `control.publish_state` reconciliation.

## 10. Queries

All numbers above are reproducible with:
- OLTP/reporting: the SQL embedded in `tmp/dq_final.sh` and `tmp/post_profile.sql`
  (this doc quotes every result; scripts are dev-scratch, not product code).
- Lake checks: `tmp/after_analysis2.py` (Spark).
- Pipeline totals: `bi/validation/run_expected_metrics.sh` + `psql` counts.
