-- ════════════════════════════════════════════════════════════════════════
--
-- ════════════════════════════════════════════════════════════════════════

SELECT check_name, toString(measured) AS measured, expected,
       if(ok, 'PASS', 'FAIL') AS status
FROM
(
    SELECT '01 dim_destination contains all six destinations' AS check_name,
           toFloat64(count()) AS measured, '= 6' AS expected, count() = 6 AS ok
    FROM tourism.dim_destination FINAL
    UNION ALL
    SELECT '02 dim_airport contains all 12 airports with time zones',
           toFloat64(countIf(timezone LIKE '%/%')), '= 12', countIf(timezone LIKE '%/%') = 12
    FROM tourism.dim_airport FINAL
    UNION ALL
    SELECT '03 dim_airport has BTS metrics (arrivals_per_day > 0)',
           toFloat64(countIf(arrivals_per_day > 0)), '= 12', countIf(arrivals_per_day > 0) = 12
    FROM tourism.dim_airport FINAL

    UNION ALL
    SELECT '04 baseline has all 12 × 7 × 96 slots',
           toFloat64(count()), '= 8064', count() = 8064
    FROM tourism.baseline_airport_slot FINAL
    UNION ALL
    SELECT '05 baseline p95 ≥ p50 in every slot',
           toFloat64(countIf(baseline_p95 < baseline_p50)), '= 0 invalid slots', countIf(baseline_p95 < baseline_p50) = 0
    FROM tourism.baseline_airport_slot FINAL
    UNION ALL
    SELECT '06 15-minute fact has batch data',
           toFloat64(countIf(source = 'batch')), '> 100000', countIf(source = 'batch') > 100000
    FROM tourism.fact_airport_flow_15min
    UNION ALL
    SELECT '07 window_ts aligned to 15-minute boundaries',
           toFloat64(countIf(toMinute(window_ts) % 15 != 0 OR toSecond(window_ts) != 0)), '= 0',
           countIf(toMinute(window_ts) % 15 != 0 OR toSecond(window_ts) != 0) = 0
    FROM tourism.fact_airport_flow_15min
    UNION ALL
    SELECT '08 no windows with missing city_id',
           toFloat64(countIf(city_id = '')), '= 0', countIf(city_id = '') = 0
    FROM tourism.fact_airport_flow_15min
    UNION ALL
    SELECT '09 local time offset from UTC within [-10, -4] hours',
           toFloat64(countIf(dateDiff('hour', window_ts, toTimeZone(window_local, 'UTC')) NOT BETWEEN -10 AND -4)),
           '= 0', countIf(dateDiff('hour', window_ts, toTimeZone(window_local, 'UTC')) NOT BETWEEN -10 AND -4) = 0
    FROM tourism.fact_airport_flow_15min
    UNION ALL
    SELECT '10 historical WARNING rate is plausible (< 5 %)',
           round(100 * countIf(flow_severity >= 2) / count(), 2), '< 5 %',
           countIf(flow_severity >= 2) / count() < 0.05
    FROM tourism.fact_airport_flow_15min FINAL WHERE source = 'batch'

    UNION ALL
    SELECT '11 occupancy_pct trong [0, 100]',
           toFloat64(countIf(occupancy_pct < 0 OR occupancy_pct > 100)), '= 0',
           countIf(occupancy_pct < 0 OR occupancy_pct > 100) = 0
    FROM tourism.fact_occupancy_night
    UNION ALL
    SELECT '12 each destination has at least two snapshots',
           toFloat64(min(n)), '≥ 2', min(n) >= 2
    FROM (SELECT city_id, uniqExact(snapshot_date) AS n FROM tourism.fact_occupancy_night GROUP BY city_id)
    UNION ALL
    SELECT '13 booked_guests ≤ capacity_guests',
           toFloat64(countIf(booked_guests > capacity_guests)), '= 0', countIf(booked_guests > capacity_guests) = 0
    FROM tourism.fact_occupancy_night
    UNION ALL
    SELECT '14 windows from April 2026 have matching occupancy',
           round(100 * countIf(occupancy_pct IS NOT NULL) / count(), 2), '≥ 95 %',
           countIf(occupancy_pct IS NOT NULL) / count() >= 0.95
    FROM tourism.v_destination_15min WHERE window_ts >= '2026-04-01'

    -- ── BR10 ───────────────────────────────────────────────────────
    UNION ALL
    SELECT '15 fact_stay has unique review_id',
           toFloat64(count() - uniqExact(review_id)), '= 0', count() = uniqExact(review_id)
    FROM tourism.fact_stay
    UNION ALL
    SELECT '16 dim_guest: is_repeat matches n_trips ≥ 2',
           toFloat64(countIf(is_repeat != (n_trips >= 2))), '= 0', countIf(is_repeat != (n_trips >= 2)) = 0
    FROM tourism.dim_guest
    UNION ALL
    SELECT '17 dim_guest: guest count equals unique reviewers in fact_stay',
           toFloat64((SELECT count() FROM tourism.dim_guest) - (SELECT uniqExact(reviewer_id) FROM tourism.fact_stay)),
           '= 0 difference',
           (SELECT count() FROM tourism.dim_guest) = (SELECT uniqExact(reviewer_id) FROM tourism.fact_stay)
    UNION ALL
    SELECT '18 cohort: retention trong [0, 100]',
           toFloat64(countIf(retention_pct > 100 OR cumulative_return_pct > 100)), '= 0',
           countIf(retention_pct > 100 OR cumulative_return_pct > 100) = 0
    FROM tourism.mart_br10_cohort
    UNION ALL
    SELECT '19 cohort: cumulative retention never decreases by month',
           toFloat64(countIf(d < 0)), '= 0', countIf(d < 0) = 0
    FROM (SELECT cumulative_returned - lagInFrame(cumulative_returned, 1, 0)
                 OVER (PARTITION BY cohort_month ORDER BY month_offset
                       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS d
          FROM tourism.mart_br10_cohort)

    UNION ALL
    SELECT '20 latest run of every job is SUCCESS',
           toFloat64(countIf(last_status = 'FAILED')), '= 0 failed jobs', countIf(last_status = 'FAILED') = 0
    FROM tourism.v_pipeline_status WHERE last_status != 'STARTED' OR job_name LIKE 'br9_05%'
)
ORDER BY check_name
FORMAT PrettyCompactMonoBlock;
