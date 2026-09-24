-- Expected KPI values, computed straight from the serving layer (ground truth
-- to reconcile Power BI totals against). Run: see bi/README.md "Validation".
-- Every number here must match the corresponding dashboard value with no filters.

SELECT 'Trips'                 k, count(*)::text v FROM reporting.fact_trips
UNION ALL SELECT 'Completed',   count(*) FILTER (WHERE status='completed')::text FROM reporting.fact_trips
UNION ALL SELECT 'Cancelled',   count(*) FILTER (WHERE status IN ('cancelled','canceled'))::text FROM reporting.fact_trips
UNION ALL SELECT 'Active',      (count(*) - count(*) FILTER (WHERE status='completed') - count(*) FILTER (WHERE status IN ('cancelled','canceled')))::text FROM reporting.fact_trips
UNION ALL SELECT 'CompletionRate', round(100.0*count(*) FILTER (WHERE status='completed')/count(*),1)||'%' FROM reporting.fact_trips
UNION ALL SELECT 'MedianDelayMin', round((percentile_cont(0.5) WITHIN GROUP (ORDER BY acceptance_delay_minutes))::numeric,1)::text FROM reporting.fact_trips
UNION ALL SELECT 'P90DelayMin', round((percentile_cont(0.9) WITHIN GROUP (ORDER BY acceptance_delay_minutes))::numeric,1)::text FROM reporting.fact_trips
UNION ALL SELECT 'GMV', round(sum(fare_amount) FILTER (WHERE fare_amount>0))::text FROM reporting.fact_trips
UNION ALL SELECT 'AnalyticalGMV', round(sum(fare_amount_analytical))::text FROM reporting.fact_trips
UNION ALL SELECT 'BilledAmount', round(sum(amount))::text FROM reporting.fact_payments
UNION ALL SELECT 'CollectedAmount', round(sum(amount) FILTER (WHERE status='paid'))::text FROM reporting.fact_payments
UNION ALL SELECT 'CollectionRate', round(100.0*sum(amount) FILTER (WHERE status='paid')/sum(amount),1)::text||'%' FROM reporting.fact_payments
UNION ALL SELECT 'RevenueLeakageTrips', count(*)::text FROM (
    SELECT t.trip_id FROM reporting.fact_trips t
    WHERE t.status='completed'
      AND NOT EXISTS (SELECT 1 FROM reporting.fact_payments p
                      WHERE p.trip_id=t.trip_id AND p.status='paid')) x
UNION ALL SELECT 'RevenueLeakageAmount', round(sum(fare_amount))::text FROM (
    SELECT t.trip_id, t.fare_amount FROM reporting.fact_trips t
    WHERE t.status='completed'
      AND NOT EXISTS (SELECT 1 FROM reporting.fact_payments p
                      WHERE p.trip_id=t.trip_id AND p.status='paid')) x
UNION ALL SELECT 'DuplicatePaymentExposure', coalesce(sum(amount) FILTER (WHERE duplicate_provider_ref),0)::text FROM reporting.fact_payments
UNION ALL SELECT 'BillingGapPct', round((100.0*(sum(actual_distance_km) FILTER (WHERE actual_distance_km>0 AND estimated_distance_km>0)-sum(estimated_distance_km) FILTER (WHERE actual_distance_km>0 AND estimated_distance_km>0))/sum(estimated_distance_km) FILTER (WHERE estimated_distance_km>0))::numeric,2)::text||'%' FROM reporting.fact_trips
UNION ALL SELECT 'RatingCount', count(*)::text FROM reporting.fact_ratings
UNION ALL SELECT 'AverageRating', round(avg(score) FILTER (WHERE NOT score_invalid),2)::text FROM reporting.fact_ratings
UNION ALL SELECT 'FiveStarShare', round(100.0*count(*) FILTER (WHERE score=5 AND NOT score_invalid)/count(*) FILTER (WHERE NOT score_invalid),1)::text||'%' FROM reporting.fact_ratings
UNION ALL SELECT 'DQIssueTrips', count(*) FILTER (WHERE coordinates_missing OR completed_but_ended_at_null OR ended_before_started OR driver_vehicle_mismatch OR is_distance_outlier OR is_acceptance_delay_outlier OR is_trip_duration_outlier OR requested_at_source_invalid)::text FROM reporting.fact_trips
UNION ALL SELECT 'DataTrustRate', round(100.0*(1-count(*) FILTER (WHERE coordinates_missing OR completed_but_ended_at_null OR ended_before_started OR driver_vehicle_mismatch OR is_distance_outlier OR is_acceptance_delay_outlier OR is_trip_duration_outlier OR requested_at_source_invalid)::numeric/count(*)),1)::text||'%' FROM reporting.fact_trips
UNION ALL SELECT 'ImputedFareRate', round(100.0*count(*) FILTER (WHERE fare_amount_was_imputed)/count(*),1)::text||'%' FROM reporting.fact_trips
UNION ALL SELECT 'DateFormatRepairs', count(*) FILTER (WHERE requested_at_was_normalized)::text FROM reporting.fact_trips
UNION ALL SELECT 'InvalidSourceDates', count(*) FILTER (WHERE requested_at_source_invalid)::text FROM reporting.fact_trips
UNION ALL SELECT 'UnknownDriverTrips', count(*) FILTER (WHERE driver_key=0)::text FROM reporting.fact_trips
UNION ALL SELECT 'PIIRecordsRedacted', ((SELECT count(*) FROM reporting.fact_trips WHERE cancel_note_contains_potential_pii)+(SELECT count(*) FROM reporting.fact_ratings WHERE comment_contains_potential_pii)+(SELECT count(*) FROM reporting.fact_payments WHERE provider_ref_contains_potential_pii))::text
UNION ALL SELECT 'ErasedSubjects', ((SELECT count(*) FROM reporting.dim_passenger WHERE is_deleted)+(SELECT count(*) FROM reporting.dim_driver WHERE is_deleted)+(SELECT count(*) FROM reporting.dim_vehicle WHERE is_deleted))::text
UNION ALL SELECT 'LastGoldLoad', max(dwh_loaded_at)::text FROM reporting.fact_trips
ORDER BY 1;
