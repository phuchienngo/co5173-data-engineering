-- ════════════════════════════════════════════════════════════════════════
--
--
--       occ_watch 70 · occ_warning 85 · occ_critical 95 · occ_horizon_nights 90
--       ./scripts/init_clickhouse.sh --only 14_br9_serving.sql
--
-- ════════════════════════════════════════════════════════════════════════


-- ────────────────────────────────────────────────────────────────────
--
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW tourism.v_occupancy_city_night AS
SELECT
    city_id,
    night_date,
    argMax(snap,      tuple(in_h, snap))  AS snapshot_date,
    argMax(ahead,     tuple(in_h, snap))  AS nights_ahead,
    argMax(in_h,      tuple(in_h, snap))  AS in_horizon,
    argMax(cap,       tuple(in_h, snap))  AS capacity_guests,
    argMax(booked,    tuple(in_h, snap))  AS booked_guests,
    argMax(occ,       tuple(in_h, snap))  AS occupancy_pct,
    multiIf(occupancy_pct >= 95, 3, occupancy_pct >= 85, 2, occupancy_pct >= 70, 1, 0) AS occ_severity
FROM
(
    SELECT
        city_id,
        night_date,
        snapshot_date                          AS snap,
        any(nights_ahead)                      AS ahead,
        toUInt8(ahead < 90)                    AS in_h,
        sum(capacity_guests)                   AS cap,
        sum(booked_guests)                     AS booked,
        round(100.0 * booked / nullIf(cap, 0), 1) AS occ
    FROM tourism.fact_occupancy_night FINAL
    WHERE nights_ahead < 120
    GROUP BY city_id, night_date, snapshot_date
)
GROUP BY city_id, night_date;


-- ────────────────────────────────────────────────────────────────────
--
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW tourism.v_city_flow_15min AS
SELECT
    c_city                                          AS city_id,
    c_win                                           AS window_ts,
    min(c_local)                                    AS window_local,
    sum(c_arr)                                      AS arrivals,
    sum(c_dep)                                      AS departures,
    sum(c_delayed)                                  AS delayed_arrivals,
    sum(c_cancel)                                   AS cancelled,
    round(sum(c_cap), 1)                            AS capacity,
    round(100.0 * sum(c_arr) / nullIf(sum(c_cap), 0), 1) AS city_load_pct,
    max(c_sev)                                      AS flow_severity,
    argMax(c_airport, tuple(c_sev, c_load))         AS hottest_airport,
    groupArray(c_airport)                           AS airports,
    any(c_source)                                   AS source
FROM
(
    SELECT city_id AS c_city, window_ts AS c_win, window_local AS c_local,
           arrivals AS c_arr, departures AS c_dep, delayed_arrivals AS c_delayed,
           cancelled AS c_cancel, capacity AS c_cap, flow_severity AS c_sev,
           load_pct AS c_load, airport AS c_airport, source AS c_source
    FROM tourism.fact_airport_flow_15min FINAL
)
GROUP BY c_city, c_win;


-- ────────────────────────────────────────────────────────────────────
--
--
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW tourism.v_destination_15min AS
SELECT
    f.city_id                                    AS city_id,
    d.city_name                                  AS city_name,
    f.window_ts                                  AS window_ts,
    f.window_local                               AS window_local,
    toDate(f.window_local - INTERVAL 6 HOUR)     AS night_date,
    f.arrivals                                   AS arrivals,
    f.capacity                                   AS flow_capacity,
    f.city_load_pct                              AS city_load_pct,
    f.delayed_arrivals                           AS delayed_arrivals,
    f.cancelled                                  AS cancelled,
    f.flow_severity                              AS flow_severity,
    f.hottest_airport                            AS hottest_airport,
    o.occupancy_pct                              AS occupancy_pct,
    o.in_horizon                                 AS occ_in_horizon,
    o.snapshot_date                              AS occ_snapshot_date,
    ifNull(o.occ_severity, 0)                    AS occ_severity,
    least(4, greatest(f.flow_severity, ifNull(o.occ_severity, 0))
             + if(f.flow_severity >= 2 AND ifNull(o.occ_severity, 0) >= 2, 1, 0)) AS combined_severity,
    ['NORMAL', 'WATCH', 'WARNING', 'CRITICAL', 'EMERGENCY'][combined_severity + 1] AS combined_level,
    f.source                                     AS source
FROM tourism.v_city_flow_15min AS f
LEFT JOIN tourism.v_occupancy_city_night AS o
       ON o.city_id = f.city_id AND o.night_date = toDate(f.window_local - INTERVAL 6 HOUR)
LEFT JOIN (SELECT city_id, city_name FROM tourism.dim_destination FINAL) AS d
       ON d.city_id = f.city_id
SETTINGS join_use_nulls = 1;


-- ────────────────────────────────────────────────────────────────────
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW tourism.v_destination_live AS
SELECT *
FROM tourism.v_destination_15min
WHERE (city_id, window_ts) IN
(
    SELECT city_id, max(window_ts) FROM tourism.fact_airport_flow_15min FINAL GROUP BY city_id
);


-- ────────────────────────────────────────────────────────────────────
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW tourism.v_destination_alerts AS
SELECT *
FROM
(
    SELECT
        *,
        lagInFrame(combined_severity, 1, 0) OVER (PARTITION BY city_id ORDER BY window_ts
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prev_severity
    FROM tourism.v_destination_15min
)
WHERE combined_severity >= 3 AND prev_severity < 3;


-- ────────────────────────────────────────────────────────────────────
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW tourism.v_neighbourhood_next30 AS
SELECT
    o.city_id                AS city_id,
    o.neighbourhood          AS neighbourhood,
    any(n.neighbourhood_group) AS neighbourhood_group,
    any(n.latitude)          AS latitude,
    any(n.longitude)         AS longitude,
    o.snapshot_date          AS snapshot_date,
    max(o.capacity_guests)   AS guest_capacity,
    round(100.0 * sum(o.booked_guests) / nullIf(sum(o.capacity_guests), 0), 1) AS occupancy_pct_next30
FROM tourism.fact_occupancy_night AS o FINAL
INNER JOIN tourism.dim_neighbourhood AS n FINAL
        ON n.city_id = o.city_id AND n.neighbourhood = o.neighbourhood
WHERE o.nights_ahead < 30
  AND (o.city_id, o.snapshot_date) IN
      (SELECT city_id, max(snapshot_date) FROM tourism.fact_occupancy_night GROUP BY city_id)
GROUP BY o.city_id, o.neighbourhood, o.snapshot_date;
