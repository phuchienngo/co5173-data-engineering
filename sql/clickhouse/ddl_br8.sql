CREATE TABLE IF NOT EXISTS dim_br8_places
(
    place_id String,
    place_name String,
    category String,
    region_id String,
    city_name String,
    country_name String,
    latitude Nullable(Float64),
    longitude Nullable(Float64)
)
ENGINE = MergeTree ORDER BY place_id;

-- One logical visit per user/place/UTC day. Retries and replay do not add visits.
CREATE TABLE IF NOT EXISTS fact_br8_visits
(
    event_id String,
    user_id UInt64,
    place_id String,
    visit_date Date,
    visit_time DateTime64(6, 'UTC'),
    raw_checkins UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY (place_id, user_id, visit_date);

CREATE VIEW IF NOT EXISTS v_br8_user_retention AS
SELECT place_id, user_id,
       count() AS visit_count,
       min(visit_date) AS first_visit,
       max(visit_date) AS last_visit,
       arraySort(groupArray(visit_date)) AS visit_days,
       toUInt8(visit_count >= 2) AS has_returned
FROM fact_br8_visits FINAL
GROUP BY place_id, user_id;

CREATE VIEW IF NOT EXISTS v_br8_place_retention AS
SELECT r.place_id, d.place_name, d.category, d.city_name, d.country_name,
       count() AS unique_visitors,
       countIf(r.has_returned = 1) AS returning_visitors,
       sum(r.visit_count) AS total_visits,
       round(100.0 * returning_visitors / unique_visitors, 2) AS repeat_visit_rate,
       min(r.first_visit) AS first_observed_date,
       max(r.last_visit) AS last_observed_date
FROM v_br8_user_retention AS r
INNER JOIN dim_br8_places AS d ON r.place_id = d.place_id
GROUP BY r.place_id, d.place_name, d.category, d.city_name, d.country_name;

-- Contributions occur once at the first day, and once at the second day.
CREATE VIEW IF NOT EXISTS v_br8_daily_retention AS
SELECT place_id, visit_date,
       sum(new_visitors) OVER w AS unique_visitors,
       sum(new_returning) OVER w AS returning_visitors,
       round(100.0 * returning_visitors / unique_visitors, 2) AS repeat_visit_rate
FROM
(
    SELECT place_id, visit_date,
           sum(new_visitors) AS new_visitors,
           sum(new_returning) AS new_returning
    FROM
    (
        SELECT place_id, first_visit AS visit_date, toUInt64(1) AS new_visitors,
               toUInt64(0) AS new_returning FROM v_br8_user_retention
        UNION ALL
        SELECT place_id, visit_days[2] AS visit_date, toUInt64(0) AS new_visitors,
               toUInt64(1) AS new_returning FROM v_br8_user_retention WHERE has_returned = 1
    )
    GROUP BY place_id, visit_date
)
WINDOW w AS (PARTITION BY place_id ORDER BY visit_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
