# Data profile for BI — Urban Mobility (baseline, before generator redesign)

Snapshot date: 2026-09-21. Source: `mobility_dw.reporting.*` (serving layer,
reconciled with Gold). ~43,001 trips / 34,452 payments / 5,195 ratings.
This is the historical pre-redesign baseline that justified the generator redesign. It does not describe the current synthetic dataset.
All numbers below are reproducible with the SQL in each section.

## Executive summary

The pipeline is correct — Bronze/Silver/Gold faithfully carry whatever the
generator produces. The problem is the generator: nearly every business
variable is drawn from a **uniform distribution independent of context**, so
the warehouse is statistically flat. That is why the dashboard has no story.

Confirmed symptoms (see evidence):

| Symptom | Measured | Why (code) |
|---|---|---|
| ~5 dates only | 43,001 trips on **5 dates / 6 hours** | `requested_at = datetime.now()` per batch run |
| completed ≈ cancelled | 41.0% vs 41.1% | `random.choice([...5 statuses])` + 50/50 flip |
| zones look identical | trip-count **CV = 2.3%** | `random.choice(zone_ids)` uniform |
| fare vs distance dead | corr = **-0.004** | `fare = uniform(5,80)` independent of km |
| duration vs distance dead | corr = **0.02** | `end_lag = uniform(5,40)` independent of km |
| rating vs delay dead | corr = **0.03** | `score = randint(1,5)` uniform, independent |
| payments uniform 1/3 | paid/failed/pending = 33/33/33 | `random.choice(["paid","failed","pending"])` |
| method has no effect | failure ~33% for cash/card/wallet | status not conditioned on method |
| ratings flat | 19.9–20.8% each star | `randint(1,5)` |

## A. Temporality (the single biggest defect)

```sql
SELECT min(requested_at)::date, max(requested_at)::date,
       count(DISTINCT requested_at::date) fechas,
       count(DISTINCT requested_at::timestamp(0)) ts, count(*)
FROM reporting.fact_trips;
```
| min | max | dates | distinct_ts | trips |
|---|---|---|---|---|
| 2026-02-06 | 2026-09-20 | **5** | **29** | 43,001 |

- **5 dates / 29 distinct timestamps for 43k trips.** Each generator run
  (`datetime.now()`) stamps a whole batch with one instant; 5 runs = 5 dates.
- Day-of-week and hour-of-day are artifacts of *when the script was run*
  (Fri 20k, Sat 10k, Sun 10k, Mon 3k; hours 08/14/15/22/23), not behavior.
- No weekday/weekend signal, no rush hours, no seasonality. `dim_date` has
  288 rows but only 5 connect to trips.
- ⚠ This also makes `request_date_key` and `date_key` in `dim_date` useless for
  trend visuals: a time-axis visual currently shows 5 bars.

## B. Trip status

```sql
SELECT status, count(*), round(100.0*count(*)/sum(count(*)) over(),1) pct
FROM reporting.fact_trips GROUP BY 1 ORDER BY 2 DESC;
```
| status | count | pct |
|---|---|---|
| canceled | 17,667 | 41.1 |
| completed | 17,650 | 41.0 |
| started | 2,629 | 6.1 |
| requested | 2,544 | 5.9 |
| accepted | 2,511 | 5.8 |

- Completion ≈ cancellation is **not plausible** for a mobility platform
  (real apps: ~75–85% completion, ~10–20% cancellation).
- Root cause: `status = random.choice([requested, accepted, started, completed, canceled])`
  (20% each) then `update_trip_statuses` flips unfinished ones with
  `random.choice(["completed","canceled"])` → converges to ~50/50.

## C. Zone demand uniformity

```sql
SELECT min(n), max(n), round(avg(n),1), round(stddev(n),1), round(100*stddev(n)/avg(n),1) cv_pct
FROM (SELECT count(*) n FROM reporting.fact_trips GROUP BY pickup_zone_key) x;
```
| min | max | avg | sd | CV% |
|---|---|---|---|---|
| 1,948 | 2,117 | 2,047.7 | 46.8 | **2.3%** |

- Every one of the 22 zones is within ±5% of the mean. `random.choice(zone_ids)`
  is uniform, and there is no downtown/airport/residential/nightlife role.
- Consequence: "revenue by zone", "demand by zone" visuals are flat noise.
- Per-zone KPIs (fare avg ~42.3–43.1, km avg ~16.7–17.7, completion ~39–43%)
  are identical within sampling error → no zone differentiates behaviour.

## D. Numeric distributions

Acceptance delay (minutes):
| min | p50 | p90 | p99 | max | avg | sd |
|---|---|---|---|---|---|---|
| 1 | 6 | 10 | 1800 | 4320 | 45.1 | 334 |

- p50=6, p90=10 → healthy base is a tight `uniform(1,10)`. The extreme sd/p99
  comes ENTIRELY from injected outliers (`LONG_ACCEPTANCE_DELAY_RATE`,
  `uniform(45,360)`), NOT from a demand/zone/time effect. So "delay" has no
  business variance — only a uniform tail. Mean is meaningless (skewed),
  median is stable across every slice.

Fare and distance:
| fare avg | fare sd | fare min | fare max | km avg | km p50 |
|---|---|---|---|---|---|
| 42.60 | 21.73 | 5.00 | 80.00 | 17.18 | 17.0 |

- fare = `uniform(5,80)`, distance = `uniform(1,30)`: both uniform, both
  independent. Fare range is not anchored to distance at all.

## E. Correlations that SHOULD exist and do NOT

```sql
SELECT corr(fare_amount, actual_distance_km) FROM reporting.fact_trips WHERE ...>0;      -- fare vs km
SELECT corr(trip_duration_minutes, actual_distance_km) FROM reporting.fact_trips ...;    -- duration vs km
WITH r AS (SELECT driver_key, score, percentile_cont(.5) WITHIN GROUP (ORDER BY acceptance_delay_minutes) med_delay ...)
SELECT corr(score, med_delay) FROM r;                                                    -- rating vs delay
```
| relationship | measured | expected in reality |
|---|---|---|
| fare ↔ distance | **-0.004** | strong + (≈0.8–0.9) |
| duration ↔ distance | **0.02** | strong + |
| rating ↔ acceptance delay | **0.03** | moderate - |

These three are the mechanical reason the Experience/Revenue dashboards feel
empty: there is genuinely no signal to show, because the generator did not
encode any causal structure.

## F. Payments

```sql
SELECT status, count(*), round(100.0*count(*)/sum(count(*)) over(),1) FROM reporting.fact_payments GROUP BY 1;
SELECT m.payment_method_name, p.status, count(*) FROM reporting.fact_payments p JOIN reporting.dim_payment_method m USING(payment_method_key) GROUP BY 1,2;
```
| status | pct |
|---|---|
| paid | 33.1 |
| failed | 33.5 |
| pending | 33.5 |

Method × status is essentially 3 equal thirds for cash, card and wallet:
`method = random.choice([...])`, `status = random.choice(["paid","failed","pending"])`
— independent. Payment method has **zero** effect on success; amount
(`uniform(5,80)`, avg 42.5, ~same for all methods) is independent of the trip
fare it should mirror. `Collection Rate ≈ 33%` is therefore a meaningless
"everything is broken" number, not a real payment-health signal.

## G. Ratings

| score | pct |
|---|---|
| 1 | 19.9 |
| 2 | 20.8 |
| 3 | 19.2 |
| 4 | 20.4 |
| 5 | 19.7 |

- Uniform `randint(1,5)`, mean 2.99. Real ride-hail ratings skew high
  (≈4.4–4.7, mostly 4–5). A 5-star share of 19.7% and a 1-star share of 19.9%
  is not credible and produces a flat, uninformative distribution chart.

## H. Data-quality flags (already reasonable, keep)

```sql
SELECT count(*) FILTER (WHERE coordinates_missing) coords, ... FROM reporting.fact_trips;
```
| flag | count | rate |
|---|---|---|
| coordinates_missing | 4,571 | 10.6% |
| completed_but_ended_at_null | 2,432 | 13.8% (of completed) |
| ended_before_started | 62 | 0.14% |
| is_distance_outlier | 136 | 0.3% |
| is_acceptance_delay_outlier | 846 | 2.0% |
| is_trip_duration_outlier | 138 | 0.3% |
| fare_amount_was_imputed | 8,635 | 20.1% |
| driver_vehicle_mismatch | 939 | 2.2% |
| driver_key=0 (unassigned) | 7,590 | 17.6% |
| cancel_note PII | 76 | — |

- The DQ/dirty layer is the ONE part already behaving well (controlled,
  configurable `DIRTY_DATA_RATES`, `RANDOM_SEED` supported). We keep it and
  make the *business* signals realistic around it.
- "Unassigned driver" 17.6% ≈ the 20% `requested` share, which is plausible,
  but will drop naturally once statuses are realistic (fewer stuck requested).

## I. Cardinalities & dimensions

| table | rows |
|---|---|
| dim_zone | 22 |
| dim_driver | 661 (1 is UNKNOWN) |
| dim_vehicle | 661 (1:1 with driver, 1 orphan) |
| dim_passenger | 2,591 (canonical, 1 is key 0) |
| dim_date | 288 |
| vehicle_type | hatchback 231 / sedan 215 / motorbike 214 / (1 null) |

- All fact↔dim FK coverage is 100% (0 orphans confirmed earlier). Star schema
  is structurally sound; only its data is flat.

## What this means for the generator redesign

1. Introduce **temporal structure**: spread `requested_at` over ~90 days with
   day-of-week and hour-of-day intensity (rush hours, nightlife weekends).
2. Give **zones roles** and weights (downtown/airport/residential/business/
   nightlife): different demand, distance, fare, and delay profiles.
3. Model **acceptance delay** from demand/supply/congestion (rush hour &
   high-demand zones → higher delay), with noise.
4. Make **status** realistic (completion ≈ 78–85%), with cancellation
   probability RISING with acceptance delay and demand.
5. **Rating** skews high and DECREASES with delay/bad duration/driver quality;
   introduce latent driver_quality.
6. **Payment**: success rate depends on method (wallet/card/cash), amount
   tracks the trip fare; collection rate becomes a real (~high) number.
7. **Fare** = base + rate×distance + time×duration + surge(hour) + noise;
   correlate with distance by construction.
8. Keep all existing dirty-data rates as a `DQ_RATES` config, keep seed.

## Explicitly artificial/uniform today (fix targets)
- All `random.choice` over equally-weighted small enums (status, method,
  payment status, zone). All `uniform` numerics with no correlation.
- Uniform 1/5 ratings, uniform 1/3 payments, uniform zone demand.

## Features that currently add no signal
- `request_date_key` / hour-of-day / day-of-week (uniform across a few batch
  timestamps) — meaningless until temporality is real.
- Per-zone KPI breakdowns (all zones statistically equal).

## SQL used
This document is generated from the queries embedded in each section
(`reporting.fact_trips`, `reporting.fact_payments`, `reporting.fact_ratings`
and dimensions on `mobility_dw`). Reproduce with
`bi/validation/run_expected_metrics.sh` and the ad-hoc profile queries.
