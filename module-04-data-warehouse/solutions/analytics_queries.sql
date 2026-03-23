-- =============================================================================
-- Module 04: Analytics Queries -- Solutions
-- =============================================================================
-- These queries run against the star schema in warehouse.duckdb.
-- Open the warehouse with:  duckdb warehouse.duckdb
--
-- Each query corresponds to an exercise in exercises.md (Exercise 5).
-- =============================================================================


-- =============================================================================
-- Exercise 5a: Revenue by Borough and Zone
-- =============================================================================
-- For each borough, find the top 10 pickup zones by total fare revenue.
-- Uses a window function (RANK) to pick the top zones within each borough.
-- =============================================================================

WITH zone_revenue AS (
    SELECT
        z.borough,
        z.zone,
        COUNT(*)                              AS trip_count,
        ROUND(SUM(f.total_amount), 2)         AS total_revenue,
        ROUND(AVG(f.fare_amount), 2)          AS avg_fare,
        ROUND(AVG(f.tip_amount), 2)           AS avg_tip
    FROM fact_yellow_trips f
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    GROUP BY z.borough, z.zone
),
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY borough
            ORDER BY total_revenue DESC
        ) AS rank_in_borough
    FROM zone_revenue
)
SELECT
    borough,
    zone,
    trip_count,
    total_revenue,
    avg_fare,
    avg_tip,
    rank_in_borough
FROM ranked
WHERE rank_in_borough <= 10
ORDER BY borough, rank_in_borough;


-- =============================================================================
-- Exercise 5b: Trip Patterns by Hour and Day
-- =============================================================================
-- Heatmap-ready dataset: trip volume by hour of day and day of week.
-- This tells the business when demand peaks so they can optimise fleet
-- allocation and surge pricing.
-- =============================================================================

SELECT
    d.day_of_week,
    d.day_name,
    EXTRACT(HOUR FROM f.pickup_datetime)::INTEGER AS hour_of_day,
    COUNT(*)                                       AS total_trips,
    ROUND(AVG(f.fare_amount), 2)                   AS avg_fare,
    ROUND(AVG(f.trip_distance), 2)                 AS avg_distance
FROM fact_yellow_trips f
JOIN dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.day_of_week, d.day_name, hour_of_day
ORDER BY d.day_of_week, hour_of_day;


-- =============================================================================
-- Exercise 5c: Weather Impact on Trip Volume
-- =============================================================================
-- Join trips to daily weather data. Compare trip counts, fares, and tips
-- across weather categories (Snow, Rain, Clear).
--
-- Hypothesis: bad weather increases demand (people avoid walking) but may
-- also decrease supply (fewer drivers), leading to higher fares.
-- =============================================================================

WITH daily_trips AS (
    SELECT
        CAST(f.pickup_datetime AS DATE) AS trip_date,
        COUNT(*)                         AS trip_count,
        ROUND(AVG(f.fare_amount), 2)     AS avg_fare,
        ROUND(AVG(f.tip_amount), 2)      AS avg_tip,
        ROUND(AVG(f.trip_distance), 2)   AS avg_distance
    FROM fact_yellow_trips f
    GROUP BY CAST(f.pickup_datetime AS DATE)
)
SELECT
    w.weather_category,
    COUNT(*)                                    AS num_days,
    ROUND(AVG(dt.trip_count), 0)                AS avg_daily_trips,
    ROUND(AVG(dt.avg_fare), 2)                  AS avg_fare,
    ROUND(AVG(dt.avg_tip), 2)                   AS avg_tip,
    ROUND(AVG(dt.avg_distance), 2)              AS avg_distance,
    ROUND(AVG(w.temp_avg), 1)                   AS avg_temperature,
    ROUND(AVG(w.precipitation), 2)              AS avg_precipitation
FROM daily_trips dt
JOIN dim_weather w ON dt.trip_date = w.date
GROUP BY w.weather_category
ORDER BY avg_daily_trips DESC;


-- =============================================================================
-- Exercise 5d: Uber vs Lyft vs Taxi Comparison
-- =============================================================================
-- Compare for-hire vehicles (FHV) with yellow and green taxis.
-- FHV data is grouped by base company (Uber, Lyft, etc.).
--
-- Note: FHV trips do not include fare data, so we compare on trip volume
-- and duration only. Yellow/green taxis provide full fare breakdowns.
-- =============================================================================

-- Part 1: Service type summary
WITH yellow_summary AS (
    SELECT
        'Yellow Taxi'                             AS service_type,
        COUNT(*)                                  AS total_trips,
        ROUND(AVG(trip_duration_minutes), 1)      AS avg_duration_min,
        ROUND(AVG(fare_amount), 2)                AS avg_fare
    FROM fact_yellow_trips
),
green_summary AS (
    SELECT
        'Green Taxi'                              AS service_type,
        COUNT(*)                                  AS total_trips,
        ROUND(AVG(trip_duration_minutes), 1)      AS avg_duration_min,
        ROUND(AVG(fare_amount), 2)                AS avg_fare
    FROM fact_green_trips
),
fhv_summary AS (
    SELECT
        COALESCE(b.base_type, 'Unknown')          AS service_type,
        COUNT(*)                                  AS total_trips,
        ROUND(AVG(f.trip_duration_minutes), 1)    AS avg_duration_min,
        NULL::DOUBLE                              AS avg_fare
    FROM fact_fhv_trips f
    LEFT JOIN dim_fhv_bases b ON f.dispatching_base_num = b.base_number
    GROUP BY b.base_type
)
SELECT * FROM yellow_summary
UNION ALL
SELECT * FROM green_summary
UNION ALL
SELECT * FROM fhv_summary
ORDER BY total_trips DESC;

-- Part 2: Top pickup zones by service type
WITH fhv_zones AS (
    SELECT
        COALESCE(b.base_type, 'FHV') AS service_type,
        z.borough,
        z.zone,
        COUNT(*) AS trip_count
    FROM fact_fhv_trips f
    LEFT JOIN dim_fhv_bases b ON f.dispatching_base_num = b.base_number
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    GROUP BY b.base_type, z.borough, z.zone
),
yellow_zones AS (
    SELECT
        'Yellow Taxi' AS service_type,
        z.borough,
        z.zone,
        COUNT(*) AS trip_count
    FROM fact_yellow_trips f
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    GROUP BY z.borough, z.zone
),
all_zones AS (
    SELECT * FROM fhv_zones
    UNION ALL
    SELECT * FROM yellow_zones
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY service_type
            ORDER BY trip_count DESC
        ) AS rn
    FROM all_zones
)
SELECT service_type, borough, zone, trip_count
FROM ranked
WHERE rn <= 10
ORDER BY service_type, rn;


-- =============================================================================
-- Exercise 5e: Tip Analysis by Payment Type
-- =============================================================================
-- Analyse tipping behaviour across payment types.
-- Key insight: cash tips are often unreported, so credit card tips are the
-- most reliable indicator of tipping behaviour.
-- =============================================================================

-- Tip statistics by payment type
SELECT
    pt.payment_type_name,
    COUNT(*)                                           AS trip_count,
    ROUND(AVG(f.tip_amount), 2)                        AS avg_tip,
    ROUND(AVG(
        CASE WHEN f.fare_amount > 0
             THEN f.tip_amount / f.fare_amount * 100
             ELSE 0
        END
    ), 1)                                              AS avg_tip_pct,
    -- Tip distribution: what fraction of trips have any tip at all
    ROUND(
        COUNT(*) FILTER (WHERE f.tip_amount > 0)::DOUBLE
        / COUNT(*) * 100,
        1
    )                                                  AS pct_trips_with_tip,
    ROUND(AVG(f.fare_amount), 2)                       AS avg_fare,
    ROUND(AVG(f.trip_distance), 2)                     AS avg_distance
FROM fact_yellow_trips f
JOIN dim_payment_types pt ON f.payment_type_id = pt.payment_type_id
GROUP BY pt.payment_type_name
ORDER BY trip_count DESC;

-- Tip percentage by distance bucket (credit card only, for reliable tip data)
SELECT
    CASE
        WHEN trip_distance < 1  THEN '0-1 mi'
        WHEN trip_distance < 3  THEN '1-3 mi'
        WHEN trip_distance < 5  THEN '3-5 mi'
        WHEN trip_distance < 10 THEN '5-10 mi'
        WHEN trip_distance < 20 THEN '10-20 mi'
        ELSE '20+ mi'
    END AS distance_bucket,
    COUNT(*)                                  AS trip_count,
    ROUND(AVG(tip_amount), 2)                 AS avg_tip,
    ROUND(AVG(
        CASE WHEN fare_amount > 0
             THEN tip_amount / fare_amount * 100
             ELSE 0
        END
    ), 1)                                     AS avg_tip_pct
FROM fact_yellow_trips
WHERE payment_type_id = 1   -- Credit card only
  AND fare_amount > 0
GROUP BY distance_bucket
ORDER BY
    CASE distance_bucket
        WHEN '0-1 mi'  THEN 1
        WHEN '1-3 mi'  THEN 2
        WHEN '3-5 mi'  THEN 3
        WHEN '5-10 mi' THEN 4
        WHEN '10-20 mi' THEN 5
        ELSE 6
    END;


-- =============================================================================
-- Exercise 5f: Airport Trip Analysis
-- =============================================================================
-- Analyse trips to/from the three major airports:
--   JFK Airport     = zone 132
--   LaGuardia       = zone 138
--   Newark Airport  = zone 1 (EWR)
--
-- Airport trips are a significant revenue source and have distinct patterns
-- (flat-rate fares to JFK, surcharges, peak travel times).
-- =============================================================================

-- Trip volume and revenue by airport and direction
WITH airport_trips AS (
    SELECT
        CASE
            WHEN pickup_location_id IN (132, 138, 1) THEN 'From Airport'
            WHEN dropoff_location_id IN (132, 138, 1) THEN 'To Airport'
        END AS direction,
        CASE
            WHEN pickup_location_id = 132 OR dropoff_location_id = 132 THEN 'JFK'
            WHEN pickup_location_id = 138 OR dropoff_location_id = 138 THEN 'LaGuardia'
            WHEN pickup_location_id = 1   OR dropoff_location_id = 1   THEN 'Newark (EWR)'
        END AS airport,
        fare_amount,
        tip_amount,
        total_amount,
        trip_distance,
        trip_duration_minutes,
        pickup_datetime
    FROM fact_yellow_trips
    WHERE pickup_location_id IN (132, 138, 1)
       OR dropoff_location_id IN (132, 138, 1)
)
SELECT
    airport,
    direction,
    COUNT(*)                                    AS trip_count,
    ROUND(AVG(fare_amount), 2)                  AS avg_fare,
    ROUND(AVG(tip_amount), 2)                   AS avg_tip,
    ROUND(AVG(total_amount), 2)                 AS avg_total,
    ROUND(AVG(trip_distance), 1)                AS avg_distance_mi,
    ROUND(AVG(trip_duration_minutes), 0)        AS avg_duration_min
FROM airport_trips
WHERE direction IS NOT NULL
GROUP BY airport, direction
ORDER BY airport, direction;

-- Peak hours for airport trips
WITH airport_trips AS (
    SELECT
        CASE
            WHEN pickup_location_id = 132 OR dropoff_location_id = 132 THEN 'JFK'
            WHEN pickup_location_id = 138 OR dropoff_location_id = 138 THEN 'LaGuardia'
            WHEN pickup_location_id = 1   OR dropoff_location_id = 1   THEN 'Newark (EWR)'
        END AS airport,
        EXTRACT(HOUR FROM pickup_datetime)::INTEGER AS hour_of_day
    FROM fact_yellow_trips
    WHERE pickup_location_id IN (132, 138, 1)
       OR dropoff_location_id IN (132, 138, 1)
)
SELECT
    airport,
    hour_of_day,
    COUNT(*) AS trip_count
FROM airport_trips
GROUP BY airport, hour_of_day
ORDER BY airport, trip_count DESC;

-- Most common origin zones for JFK-bound trips
SELECT
    z.borough,
    z.zone,
    COUNT(*)                          AS trip_count,
    ROUND(AVG(f.fare_amount), 2)      AS avg_fare,
    ROUND(AVG(f.trip_distance), 1)    AS avg_distance
FROM fact_yellow_trips f
JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
WHERE f.dropoff_location_id = 132   -- JFK
GROUP BY z.borough, z.zone
ORDER BY trip_count DESC
LIMIT 15;


-- =============================================================================
-- Exercise 8a: Peak Hour Analysis by Borough (Weekday vs Weekend)
-- =============================================================================

WITH hourly_trips AS (
    SELECT
        z.borough,
        d.is_weekend,
        EXTRACT(HOUR FROM f.pickup_datetime)::INTEGER AS hour_of_day,
        COUNT(*) AS trip_count
    FROM fact_yellow_trips f
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    JOIN dim_date d ON f.pickup_date_key = d.date_key
    GROUP BY z.borough, d.is_weekend, hour_of_day
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY borough, is_weekend
            ORDER BY trip_count DESC
        ) AS rn
    FROM hourly_trips
)
SELECT
    borough,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    hour_of_day AS peak_hour,
    trip_count
FROM ranked
WHERE rn = 1
ORDER BY borough, day_type;


-- =============================================================================
-- Exercise 8b: Cross-Borough Trip Analysis
-- =============================================================================

WITH cross_borough AS (
    SELECT
        pz.borough AS pickup_borough,
        dz.borough AS dropoff_borough,
        f.fare_amount,
        f.trip_distance,
        f.trip_duration_minutes
    FROM fact_yellow_trips f
    JOIN dim_zones pz ON f.pickup_location_id = pz.location_id AND pz.is_current = true
    JOIN dim_zones dz ON f.dropoff_location_id = dz.location_id AND dz.is_current = true
    WHERE pz.borough != dz.borough
      AND pz.borough != 'Unknown'
      AND dz.borough != 'Unknown'
)
SELECT
    pickup_borough,
    dropoff_borough,
    COUNT(*)                                    AS trip_count,
    ROUND(AVG(fare_amount), 2)                  AS avg_fare,
    ROUND(AVG(trip_distance), 1)                AS avg_distance_mi,
    ROUND(AVG(trip_duration_minutes), 0)        AS avg_duration_min
FROM cross_borough
GROUP BY pickup_borough, dropoff_borough
ORDER BY trip_count DESC
LIMIT 20;


-- =============================================================================
-- Exercise 8c: Weather-Revenue Correlation
-- =============================================================================

WITH daily_revenue AS (
    SELECT
        CAST(pickup_datetime AS DATE) AS trip_date,
        COUNT(*)                       AS trip_count,
        SUM(total_amount)              AS daily_revenue,
        AVG(fare_amount)               AS avg_fare
    FROM fact_yellow_trips
    GROUP BY CAST(pickup_datetime AS DATE)
)
SELECT
    -- Correlation between temperature and revenue
    ROUND(CORR(w.temp_avg, dr.daily_revenue), 4)       AS temp_revenue_corr,
    -- Correlation between precipitation and trip count
    ROUND(CORR(w.precipitation, dr.trip_count), 4)      AS precip_trips_corr,
    -- Average revenue on clear vs rainy vs snowy days
    ROUND(AVG(dr.daily_revenue) FILTER (
        WHERE w.weather_category = 'Clear'
    ), 0)                                                AS avg_revenue_clear,
    ROUND(AVG(dr.daily_revenue) FILTER (
        WHERE w.weather_category = 'Rain'
    ), 0)                                                AS avg_revenue_rain,
    ROUND(AVG(dr.daily_revenue) FILTER (
        WHERE w.weather_category = 'Snow'
    ), 0)                                                AS avg_revenue_snow
FROM daily_revenue dr
JOIN dim_weather w ON dr.trip_date = w.date;
