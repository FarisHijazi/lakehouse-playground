-- =============================================================================
-- Module 01: Solutions
-- =============================================================================
-- Solutions for all exercises in exercises.md.
-- Try to solve them yourself first!
-- =============================================================================


-- =============================================================================
-- Exercise 3a: Revenue by Borough
-- =============================================================================
-- Join yellow_taxi_trips to taxi_zones to get borough names.
-- SUM(total_amount) gives total revenue; COUNT(*) gives trip volume.

SELECT
    z.borough,
    COUNT(*)                                AS total_trips,
    ROUND(SUM(t.total_amount)::numeric, 2)  AS total_revenue,
    ROUND(AVG(t.fare_amount)::numeric, 2)   AS avg_fare
FROM yellow_taxi_trips t
JOIN taxi_zones z ON z.location_id = t.pu_location_id
GROUP BY z.borough
ORDER BY total_revenue DESC;


-- =============================================================================
-- Exercise 3b: Hourly Trip Patterns
-- =============================================================================
-- Extract the hour from the pickup timestamp, then compute average daily trips.
-- This reveals peak demand hours for taxi operations.

SELECT
    EXTRACT(HOUR FROM tpep_pickup_datetime)   AS hour_of_day,
    COUNT(*) / COUNT(DISTINCT DATE(tpep_pickup_datetime)) AS avg_daily_trips,
    ROUND(AVG(fare_amount)::numeric, 2)       AS avg_fare,
    ROUND(AVG(tip_amount)::numeric, 2)        AS avg_tip
FROM yellow_taxi_trips
GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
ORDER BY hour_of_day;


-- =============================================================================
-- Exercise 3c: Weather Impact on Taxi Demand
-- =============================================================================
-- Join trips to daily_weather by pickup date. Compare trip volume
-- on rainy vs dry days, snow vs no snow, cold vs warm.

WITH daily_trips AS (
    SELECT
        DATE(tpep_pickup_datetime)              AS trip_date,
        COUNT(*)                                AS trip_count,
        ROUND(AVG(fare_amount)::numeric, 2)     AS avg_fare
    FROM yellow_taxi_trips
    GROUP BY DATE(tpep_pickup_datetime)
)
SELECT
    CASE
        WHEN w.precipitation_in > 0.1 THEN 'Rainy'
        ELSE 'Dry'
    END                                         AS weather_condition,
    COUNT(*)                                    AS num_days,
    ROUND(AVG(dt.trip_count)::numeric, 0)       AS avg_daily_trips,
    ROUND(AVG(dt.avg_fare)::numeric, 2)         AS avg_fare
FROM daily_trips dt
JOIN daily_weather w ON w.date = dt.trip_date
GROUP BY CASE WHEN w.precipitation_in > 0.1 THEN 'Rainy' ELSE 'Dry' END
ORDER BY weather_condition;


-- =============================================================================
-- Exercise 3d: Tipping Analysis by Payment Type
-- =============================================================================
-- Tip percentage = tip_amount / fare_amount. Join with payment_types for names.
-- Cash tips are NOT recorded (show as $0) -- a classic data engineering gotcha.

SELECT
    pt.payment_type_name,
    COUNT(*)                                                    AS total_trips,
    ROUND(AVG(t.tip_amount)::numeric, 2)                        AS avg_tip,
    ROUND(
        100.0 * AVG(
            CASE WHEN t.fare_amount > 0 THEN t.tip_amount / t.fare_amount END
        )::numeric,
        2
    )                                                           AS avg_tip_pct
FROM yellow_taxi_trips t
JOIN payment_types pt ON pt.payment_type_id = t.payment_type
GROUP BY pt.payment_type_name
ORDER BY avg_tip_pct DESC;


-- =============================================================================
-- Exercise 3e: Airport Trip Analysis
-- =============================================================================
-- Analyze trips to/from JFK (132), LaGuardia (138), and Newark (1).
-- Compare average fares, distances, and tip amounts across airports.

SELECT
    z.zone                                          AS airport,
    COUNT(*) FILTER (WHERE t.pu_location_id = z.location_id) AS pickups,
    COUNT(*) FILTER (WHERE t.do_location_id = z.location_id) AS dropoffs,
    ROUND(AVG(t.fare_amount)::numeric, 2)           AS avg_fare,
    ROUND(AVG(t.tip_amount)::numeric, 2)            AS avg_tip,
    ROUND(AVG(t.total_amount)::numeric, 2)          AS avg_total,
    ROUND(AVG(t.trip_distance)::numeric, 2)         AS avg_distance
FROM yellow_taxi_trips t
JOIN taxi_zones z ON z.location_id IN (1, 132, 138)
    AND (t.pu_location_id = z.location_id OR t.do_location_id = z.location_id)
GROUP BY z.zone, z.location_id
ORDER BY pickups DESC;


-- =============================================================================
-- Exercise 3f: Uber vs Lyft Comparison
-- =============================================================================
-- Using fhv_trips, compare Uber (HV0003) vs Lyft (HV0005) on key metrics.
-- Shared ride percentage shows platform strategy differences.

SELECT
    b.app_company,
    COUNT(*)                                                AS total_trips,
    ROUND(AVG(f.trip_miles)::numeric, 2)                    AS avg_miles,
    ROUND(AVG(f.trip_time / 60.0)::numeric, 1)             AS avg_minutes,
    ROUND(AVG(f.base_passenger_fare)::numeric, 2)           AS avg_passenger_fare,
    ROUND(AVG(f.tips)::numeric, 2)                          AS avg_tips,
    ROUND(AVG(f.driver_pay)::numeric, 2)                    AS avg_driver_pay,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE f.shared_request_flag = 'Y') / COUNT(*),
        2
    )                                                       AS shared_ride_pct
FROM fhv_trips f
JOIN fhv_bases b ON b.base_license_num = f.hvfhs_license_num
WHERE f.hvfhs_license_num IN ('HV0003', 'HV0005')
GROUP BY b.app_company
ORDER BY total_trips DESC;


-- =============================================================================
-- Exercise 4c: Composite Index for Dashboard Query
-- =============================================================================
-- The query filters on pickup datetime and payment_type, then groups by location.
-- A composite index on (tpep_pickup_datetime, payment_type) helps the WHERE clause.
-- Including pu_location_id enables an index-only scan for the GROUP BY.

CREATE INDEX idx_yellow_dt_payment_location
    ON yellow_taxi_trips(tpep_pickup_datetime, payment_type, pu_location_id);

-- Verify with:
-- EXPLAIN ANALYZE
-- SELECT pu_location_id, COUNT(*) as trips, SUM(total_amount) as revenue
-- FROM yellow_taxi_trips
-- WHERE tpep_pickup_datetime >= '2023-01-01'
--   AND tpep_pickup_datetime < '2023-02-01'
--   AND payment_type = 1
-- GROUP BY pu_location_id
-- ORDER BY revenue DESC
-- LIMIT 20;


-- =============================================================================
-- Exercise 5a: Daily Trip Summary View
-- =============================================================================

CREATE OR REPLACE VIEW v_daily_trip_summary AS
WITH yellow_daily AS (
    SELECT
        DATE(tpep_pickup_datetime)               AS day,
        COUNT(*)                                 AS trips,
        SUM(total_amount)                        AS revenue,
        AVG(fare_amount)                         AS avg_fare,
        AVG(CASE WHEN payment_type = 1 AND fare_amount > 0
            THEN tip_amount / fare_amount END)   AS avg_tip_pct,
        AVG(trip_distance)                       AS avg_distance
    FROM yellow_taxi_trips
    GROUP BY DATE(tpep_pickup_datetime)
),
green_daily AS (
    SELECT
        DATE(lpep_pickup_datetime)               AS day,
        COUNT(*)                                 AS trips,
        SUM(total_amount)                        AS revenue,
        AVG(fare_amount)                         AS avg_fare,
        AVG(trip_distance)                       AS avg_distance
    FROM green_taxi_trips
    GROUP BY DATE(lpep_pickup_datetime)
)
SELECT
    y.day,
    COALESCE(y.trips, 0) + COALESCE(g.trips, 0)       AS total_trips,
    ROUND((COALESCE(y.revenue, 0) + COALESCE(g.revenue, 0))::numeric, 2) AS total_revenue,
    ROUND(y.avg_fare::numeric, 2)                       AS avg_yellow_fare,
    ROUND((y.avg_tip_pct * 100)::numeric, 2)            AS avg_tip_pct_credit,
    ROUND(y.avg_distance::numeric, 2)                   AS avg_distance
FROM yellow_daily y
LEFT JOIN green_daily g ON g.day = y.day
ORDER BY y.day;


-- =============================================================================
-- Exercise 5b: Zone Performance View
-- =============================================================================

CREATE OR REPLACE VIEW v_zone_performance AS
WITH zone_stats AS (
    SELECT
        pu_location_id                            AS location_id,
        COUNT(*)                                  AS total_pickups,
        ROUND(AVG(fare_amount)::numeric, 2)       AS avg_fare,
        ROUND(AVG(tip_amount)::numeric, 2)        AS avg_tip,
        ROUND(AVG(trip_distance)::numeric, 2)     AS avg_distance,
        MODE() WITHIN GROUP (ORDER BY payment_type) AS most_common_payment_type
    FROM yellow_taxi_trips
    GROUP BY pu_location_id
),
dropoff_stats AS (
    SELECT
        do_location_id                            AS location_id,
        COUNT(*)                                  AS total_dropoffs
    FROM yellow_taxi_trips
    GROUP BY do_location_id
)
SELECT
    z.borough,
    z.zone,
    zs.total_pickups,
    COALESCE(ds.total_dropoffs, 0)                AS total_dropoffs,
    zs.avg_fare,
    zs.avg_tip,
    pt.payment_type_name                          AS most_common_payment,
    zs.avg_distance
FROM zone_stats zs
JOIN taxi_zones z ON z.location_id = zs.location_id
LEFT JOIN dropoff_stats ds ON ds.location_id = zs.location_id
LEFT JOIN payment_types pt ON pt.payment_type_id = zs.most_common_payment_type
ORDER BY zs.total_pickups DESC;


-- =============================================================================
-- Exercise 5c: Hourly Demand View
-- =============================================================================

CREATE OR REPLACE VIEW v_hourly_demand AS
SELECT
    DATE(tpep_pickup_datetime)                                      AS day,
    EXTRACT(HOUR FROM tpep_pickup_datetime)::int                    AS hour_of_day,
    COUNT(*)                                                        AS trip_count,
    ROUND(AVG(fare_amount)::numeric, 2)                             AS avg_fare,
    CASE WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0, 6)
         THEN 'weekend' ELSE 'weekday' END                         AS day_type,
    CASE
        WHEN EXTRACT(DOW FROM tpep_pickup_datetime) NOT IN (0, 6)
             AND EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 7 AND 9
        THEN true
        WHEN EXTRACT(DOW FROM tpep_pickup_datetime) NOT IN (0, 6)
             AND EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 16 AND 19
        THEN true
        ELSE false
    END                                                             AS is_rush_hour
FROM yellow_taxi_trips
GROUP BY DATE(tpep_pickup_datetime),
         EXTRACT(HOUR FROM tpep_pickup_datetime),
         EXTRACT(DOW FROM tpep_pickup_datetime)
ORDER BY day, hour_of_day;


-- =============================================================================
-- Exercise 5d: Data Quality View
-- =============================================================================

CREATE OR REPLACE VIEW v_data_quality_issues AS
SELECT
    'negative_fare'           AS issue_type,
    COUNT(*)                  AS issue_count
FROM yellow_taxi_trips
WHERE fare_amount < 0

UNION ALL

SELECT
    'zero_distance_with_fare' AS issue_type,
    COUNT(*)                  AS issue_count
FROM yellow_taxi_trips
WHERE trip_distance = 0 AND fare_amount > 10

UNION ALL

SELECT
    'null_passenger_count'    AS issue_type,
    COUNT(*)                  AS issue_count
FROM yellow_taxi_trips
WHERE passenger_count IS NULL OR passenger_count = 0

UNION ALL

SELECT
    'pickup_after_dropoff'    AS issue_type,
    COUNT(*)                  AS issue_count
FROM yellow_taxi_trips
WHERE tpep_pickup_datetime > tpep_dropoff_datetime

UNION ALL

SELECT
    'extreme_total_amount'    AS issue_type,
    COUNT(*)                  AS issue_count
FROM yellow_taxi_trips
WHERE total_amount > 500

UNION ALL

SELECT
    'unknown_rate_code'       AS issue_type,
    COUNT(*)                  AS issue_count
FROM yellow_taxi_trips
WHERE rate_code_id = 99;
