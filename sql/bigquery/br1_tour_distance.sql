-- BR1 Tour route: BigQuery GIS equivalent of the Spark distance stage (jobs/br1_tour_route/job.py).
-- Not executed in this repo. Shows the ST_DISTANCE mapping: Spark haversine(EARTH_KM=6371.0088) ~ ST_DISTANCE on GEOGRAPHY.
-- Fare per leg = 11000 + 15000 * km, same as FARE_FLAG_VND / FARE_PER_KM_VND in the job.

-- 1) All ordered pairs within a city (stage dist_matrix).
SELECT
  a.city,
  a.osm_id AS from_id,
  b.osm_id AS to_id,
  ST_DISTANCE(ST_GEOGPOINT(a.lon, a.lat), ST_GEOGPOINT(b.lon, b.lat)) / 1000 AS dist_km,
  ROUND(11000 + 15000 * ST_DISTANCE(ST_GEOGPOINT(a.lon, a.lat), ST_GEOGPOINT(b.lon, b.lat)) / 1000) AS fare_vnd
FROM `project.tour.dim_landmark` AS a
JOIN `project.tour.dim_landmark` AS b USING (city)
WHERE a.osm_id != b.osm_id;

-- 2) Cumulative cost per tour request from the chosen legs (stage solve, leg table).
-- Assumes project.tour.fact_tour_leg holds one row per leg with leg_fare_vnd and ticket_vnd of the arrival stop.
SELECT
  request_id,
  seq,
  to_name,
  SUM(leg_fare_vnd + ticket_vnd) OVER (PARTITION BY request_id ORDER BY seq) AS cum_cost_vnd
FROM `project.tour.fact_tour_leg`
ORDER BY request_id, seq;
