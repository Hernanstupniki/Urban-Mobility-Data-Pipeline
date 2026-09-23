-- FASE 5 SQL<->DAX ground truth for FILTERED slicer states (global values live
-- in expected_metrics.sql). Run against the serving DB (mobility_dw, 5433):
--   bash bi/validation/run_expected_metrics.sh  (same connection recipe)
-- Each block = one slicer state to reproduce in Power BI Desktop; the DAX
-- measure definitions are the ones in the _Measures table. NOTE timezone:
-- reporting timestamps and dim_date are UTC; rush hour is LOCAL (UTC-3):
--   requested_local_hour IN (7,8,17,18,19)  <=>  local 07:00-09:59 / 17:00-19:59.

-- ============ SLICE 1: single date dim_date[date]=2026-09-19 (UTC calendar) =====
SELECT 'S1 date=20260919' slice, 'Trips' k, count(*)::text v FROM reporting.fact_trips WHERE request_date_key=20260919
UNION ALL SELECT 'S1', 'CompletionRate', (round(100.0*count(*) FILTER (WHERE status='completed')/NULLIF(count(*),0),1))::text FROM reporting.fact_trips WHERE request_date_key=20260919
UNION ALL SELECT 'S1', 'GMV', (round(sum(fare_amount) FILTER (WHERE fare_amount>0)))::text FROM reporting.fact_trips WHERE request_date_key=20260919
UNION ALL SELECT 'S1', 'P90AcceptanceDelay', (round(percentile_cont(0.9) WITHIN GROUP (ORDER BY acceptance_delay_minutes)::numeric,1))::text FROM reporting.fact_trips WHERE request_date_key=20260919 AND acceptance_delay_minutes IS NOT NULL
UNION ALL SELECT 'S1', 'AverageRating', (round(avg(r.score) FILTER (WHERE NOT r.score_invalid),2))::text FROM reporting.fact_ratings r JOIN reporting.fact_trips t ON r.trip_key=t.trip_id WHERE t.request_date_key=20260919

-- ============ SLICE 2: pickup zone = Manhattan (top zone, dim_zone.zone_name) =
UNION ALL
SELECT 'S2 zone=Manhattan', 'Trips', count(*)::text FROM reporting.fact_trips t JOIN reporting.dim_zone z ON t.pickup_zone_key=z.zone_id WHERE z.zone_name='Manhattan'
UNION ALL
SELECT 'S2', 'CompletionRate', (round(100.0*count(*) FILTER (WHERE t.status='completed')/NULLIF(count(*),0),1))::text FROM reporting.fact_trips t JOIN reporting.dim_zone z ON t.pickup_zone_key=z.zone_id WHERE z.zone_name='Manhattan'
UNION ALL
SELECT 'S2', 'AvgTripDurationMin', (round(avg(t.trip_duration_minutes) FILTER (WHERE NOT t.is_trip_duration_outlier)::numeric,1))::text FROM reporting.fact_trips t JOIN reporting.dim_zone z ON t.pickup_zone_key=z.zone_id WHERE z.zone_name='Manhattan'
UNION ALL
SELECT 'S2', 'GMV', (round(sum(t.fare_amount) FILTER (WHERE t.fare_amount>0)))::text FROM reporting.fact_trips t JOIN reporting.dim_zone z ON t.pickup_zone_key=z.zone_id WHERE z.zone_name='Manhattan'

-- ============ SLICE 3: RUSH hour (LOCAL 7-9 / 17-20, i.e. UTC-3) =
UNION ALL
SELECT 'S3 rush-local', 'Trips', count(*)::text FROM reporting.fact_trips WHERE EXTRACT(HOUR FROM requested_at - interval '3 hours') IN (7,8,17,18,19)
UNION ALL
SELECT 'S3', 'MedianAcceptanceDelay', (round(percentile_cont(0.5) WITHIN GROUP (ORDER BY acceptance_delay_minutes)::numeric,1))::text FROM reporting.fact_trips WHERE EXTRACT(HOUR FROM requested_at - interval '3 hours') IN (7,8,17,18,19) AND acceptance_delay_minutes IS NOT NULL
UNION ALL
SELECT 'S3', 'P90AcceptanceDelay', (round(percentile_cont(0.9) WITHIN GROUP (ORDER BY acceptance_delay_minutes)::numeric,1))::text FROM reporting.fact_trips WHERE EXTRACT(HOUR FROM requested_at - interval '3 hours') IN (7,8,17,18,19) AND acceptance_delay_minutes IS NOT NULL
UNION ALL
SELECT 'S3', 'CancellationRate', (round(100.0*count(*) FILTER (WHERE status IN ('cancelled','canceled'))/NULLIF(count(*),0),1))::text FROM reporting.fact_trips WHERE EXTRACT(HOUR FROM requested_at - interval '3 hours') IN (7,8,17,18,19)

-- ============ SLICE 4: payment method = Card (payments grain) ====
UNION ALL
SELECT 'S4 method=Card', 'CollectedAmount', (round(sum(p.amount) FILTER (WHERE p.status='paid')))::text FROM reporting.fact_payments p JOIN reporting.dim_payment_method m ON p.payment_method_key=m.payment_method_key WHERE lower(m.payment_method_name)='card'
UNION ALL
SELECT 'S4', 'BilledAmount', (round(sum(p.amount)))::text FROM reporting.fact_payments p JOIN reporting.dim_payment_method m ON p.payment_method_key=m.payment_method_key WHERE lower(m.payment_method_name)='card'
UNION ALL
SELECT 'S4', 'CollectionRate', (round(100.0*sum(p.amount) FILTER (WHERE p.status='paid')/NULLIF(sum(p.amount),0),1))::text FROM reporting.fact_payments p JOIN reporting.dim_payment_method m ON p.payment_method_key=m.payment_method_key WHERE lower(m.payment_method_name)='card'

-- ============ SLICE 5: single driver (busiest driver by trips) ==
UNION ALL
SELECT 'S5 driver=' || d.driver_id, 'Trips', count(*)::text FROM reporting.fact_trips t JOIN (SELECT driver_key FROM reporting.fact_trips WHERE driver_key<>0 GROUP BY driver_key ORDER BY count(*) DESC LIMIT 1) top ON t.driver_key=top.driver_key JOIN reporting.dim_driver d ON d.driver_id=t.driver_key GROUP BY d.driver_id
UNION ALL
SELECT 'S5', 'CompletionRate', (round(100.0*count(*) FILTER (WHERE t.status='completed')/NULLIF(count(*),0),1))::text FROM reporting.fact_trips t JOIN (SELECT driver_key FROM reporting.fact_trips WHERE driver_key<>0 GROUP BY driver_key ORDER BY count(*) DESC LIMIT 1) top ON t.driver_key=top.driver_key
UNION ALL
SELECT 'S5', 'AverageRating', (round(avg(r.score) FILTER (WHERE NOT r.score_invalid),2))::text FROM reporting.fact_ratings r JOIN (SELECT driver_key FROM reporting.fact_trips WHERE driver_key<>0 GROUP BY driver_key ORDER BY count(*) DESC LIMIT 1) top ON r.driver_key=top.driver_key
UNION ALL
SELECT 'S5', 'P90AcceptanceDelay', (round(percentile_cont(0.9) WITHIN GROUP (ORDER BY t.acceptance_delay_minutes)::numeric,1))::text FROM reporting.fact_trips t JOIN (SELECT driver_key FROM reporting.fact_trips WHERE driver_key<>0 GROUP BY driver_key ORDER BY count(*) DESC LIMIT 1) top ON t.driver_key=top.driver_key WHERE t.acceptance_delay_minutes IS NOT NULL
ORDER BY 1, 2;
