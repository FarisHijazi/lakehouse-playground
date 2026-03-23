-- =============================================================================
-- Module 04: Window Functions -- Solutions
-- =============================================================================
-- These queries run against the star schema in warehouse.duckdb.
-- Each demonstrates a different window function pattern on NYC taxi data.
--
-- Window functions perform calculations across a set of rows RELATED to the
-- current row, without collapsing them like GROUP BY. The key clauses are:
--   OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)
-- =============================================================================


-- =============================================================================
-- Exercise 7a: Running Total of Daily Revenue by Borough
-- =============================================================================
-- Window: PARTITION BY borough, ORDER BY date
-- Function: SUM() as a running (cumulative) total
--
-- The frame defaults to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- when ORDER BY is specified, giving us a running sum from the first day
-- up to and including the current row's date.
-- =============================================================================

WITH daily_revenue AS (
    SELECT
        z.borough,
        CAST(f.pickup_datetime AS DATE)   AS pickup_date,
        ROUND(SUM(f.total_amount), 2)     AS daily_revenue,
        COUNT(*)                          AS daily_trips
    FROM fact_yellow_trips f
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    WHERE z.borough != 'Unknown'
    GROUP BY z.borough, CAST(f.pickup_datetime AS DATE)
)
SELECT
    borough,
    pickup_date,
    daily_revenue,
    daily_trips,
    -- Running total: cumulative revenue from the first day to this day
    ROUND(
        SUM(daily_revenue) OVER (
            PARTITION BY borough
            ORDER BY pickup_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        2
    ) AS running_total_revenue
FROM daily_revenue
ORDER BY borough, pickup_date;


-- =============================================================================
-- Exercise 7b: Rank Zones by Trip Volume
-- =============================================================================
-- Demonstrates the difference between three ranking functions:
--   RANK()       - gaps after ties     (1, 2, 2, 4)
--   DENSE_RANK() - no gaps after ties  (1, 2, 2, 3)
--   ROW_NUMBER() - always unique       (1, 2, 3, 4)
--
-- Partitioned by borough so each borough has its own ranking.
-- =============================================================================

WITH zone_trips AS (
    SELECT
        z.borough,
        z.zone,
        COUNT(*) AS total_trips
    FROM fact_yellow_trips f
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    WHERE z.borough != 'Unknown'
    GROUP BY z.borough, z.zone
)
SELECT
    borough,
    zone,
    total_trips,
    -- RANK: 1, 2, 2, 4 (skips rank 3 because two zones tied for rank 2)
    RANK()       OVER (PARTITION BY borough ORDER BY total_trips DESC) AS rank_with_gaps,
    -- DENSE_RANK: 1, 2, 2, 3 (no gap after ties)
    DENSE_RANK() OVER (PARTITION BY borough ORDER BY total_trips DESC) AS rank_dense,
    -- ROW_NUMBER: 1, 2, 3, 4 (always unique, ties broken arbitrarily)
    ROW_NUMBER() OVER (PARTITION BY borough ORDER BY total_trips DESC) AS row_num
FROM zone_trips
ORDER BY borough, total_trips DESC;


-- =============================================================================
-- Exercise 7c: 7-Day Moving Average of Trip Distances
-- =============================================================================
-- Window: ORDER BY date, with an explicit frame of 6 preceding rows + current
-- Function: AVG() over a sliding window of 7 days
--
-- Moving averages smooth out daily noise to reveal trends. A 7-day window
-- also smooths out day-of-week effects (weekday vs weekend patterns).
-- =============================================================================

WITH daily_distances AS (
    SELECT
        CAST(pickup_datetime AS DATE) AS trip_date,
        COUNT(*)                       AS daily_trips,
        ROUND(AVG(trip_distance), 3)   AS avg_distance,
        ROUND(SUM(trip_distance), 1)   AS total_distance
    FROM fact_yellow_trips
    GROUP BY CAST(pickup_datetime AS DATE)
)
SELECT
    trip_date,
    daily_trips,
    avg_distance,
    -- 7-day moving average: smooths out day-of-week patterns
    ROUND(
        AVG(avg_distance) OVER (
            ORDER BY trip_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        3
    ) AS moving_avg_distance_7d,
    -- 30-day moving average: reveals longer-term trends
    ROUND(
        AVG(avg_distance) OVER (
            ORDER BY trip_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ),
        3
    ) AS moving_avg_distance_30d
FROM daily_distances
ORDER BY trip_date;


-- =============================================================================
-- Exercise 7d: Month-over-Month Growth Rate by Borough
-- =============================================================================
-- Window function: LAG() to reference the previous month's value
--
-- LAG(column, offset, default) looks back `offset` rows in the partition.
-- Growth rate = (current - previous) / previous * 100
-- =============================================================================

WITH monthly_trips AS (
    SELECT
        z.borough,
        DATE_TRUNC('month', f.pickup_datetime)::DATE AS month,
        COUNT(*)                                      AS trip_count,
        ROUND(SUM(f.total_amount), 2)                 AS total_revenue
    FROM fact_yellow_trips f
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    WHERE z.borough != 'Unknown'
    GROUP BY z.borough, DATE_TRUNC('month', f.pickup_datetime)
)
SELECT
    borough,
    month,
    trip_count,
    total_revenue,
    -- Previous month's trip count
    LAG(trip_count, 1) OVER (
        PARTITION BY borough
        ORDER BY month
    ) AS prev_month_trips,
    -- Month-over-month trip growth as a percentage
    ROUND(
        (trip_count - LAG(trip_count, 1) OVER (
            PARTITION BY borough ORDER BY month
        ))::DOUBLE /
        NULLIF(LAG(trip_count, 1) OVER (
            PARTITION BY borough ORDER BY month
        ), 0) * 100,
        1
    ) AS mom_trip_growth_pct,
    -- Month-over-month revenue growth
    ROUND(
        (total_revenue - LAG(total_revenue, 1) OVER (
            PARTITION BY borough ORDER BY month
        )) /
        NULLIF(LAG(total_revenue, 1) OVER (
            PARTITION BY borough ORDER BY month
        ), 0) * 100,
        1
    ) AS mom_revenue_growth_pct
FROM monthly_trips
ORDER BY borough, month;


-- =============================================================================
-- Exercise 7e: Percent of Total Calculations
-- =============================================================================
-- Window function: SUM() OVER (PARTITION BY ...) for denominator
--
-- For each zone, calculate:
--   1. What % of the borough's total trips does this zone represent?
--   2. What % of the city's total trips does this zone represent?
-- =============================================================================

WITH zone_trips AS (
    SELECT
        z.borough,
        z.zone,
        COUNT(*) AS trip_count
    FROM fact_yellow_trips f
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    WHERE z.borough != 'Unknown'
    GROUP BY z.borough, z.zone
)
SELECT
    borough,
    zone,
    trip_count,
    -- Percentage of borough total
    ROUND(
        trip_count::DOUBLE /
        SUM(trip_count) OVER (PARTITION BY borough) * 100,
        2
    ) AS pct_of_borough,
    -- Percentage of city total
    ROUND(
        trip_count::DOUBLE /
        SUM(trip_count) OVER () * 100,
        2
    ) AS pct_of_city,
    -- Cumulative percentage within borough (for Pareto analysis)
    ROUND(
        SUM(trip_count) OVER (
            PARTITION BY borough
            ORDER BY trip_count DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )::DOUBLE /
        SUM(trip_count) OVER (PARTITION BY borough) * 100,
        2
    ) AS cumulative_pct_of_borough
FROM zone_trips
ORDER BY borough, trip_count DESC;


-- =============================================================================
-- BONUS: Lead/Lag for Comparing Consecutive Periods
-- =============================================================================
-- Compare each day's revenue to the same day last week using LAG(7).
-- This removes day-of-week seasonality from the comparison.
-- =============================================================================

WITH daily_revenue AS (
    SELECT
        CAST(pickup_datetime AS DATE)    AS trip_date,
        COUNT(*)                          AS trip_count,
        ROUND(SUM(total_amount), 2)       AS daily_revenue
    FROM fact_yellow_trips
    GROUP BY CAST(pickup_datetime AS DATE)
)
SELECT
    trip_date,
    trip_count,
    daily_revenue,
    -- Same day last week
    LAG(daily_revenue, 7) OVER (ORDER BY trip_date) AS revenue_last_week,
    -- Week-over-week change
    ROUND(
        (daily_revenue - LAG(daily_revenue, 7) OVER (ORDER BY trip_date)) /
        NULLIF(LAG(daily_revenue, 7) OVER (ORDER BY trip_date), 0) * 100,
        1
    ) AS wow_change_pct,
    -- Next day's revenue (LEAD looks forward)
    LEAD(daily_revenue, 1) OVER (ORDER BY trip_date) AS next_day_revenue
FROM daily_revenue
ORDER BY trip_date;


-- =============================================================================
-- BONUS: NTILE -- Segment Zones into Trip Volume Quartiles
-- =============================================================================
-- NTILE(n) divides ordered rows into n roughly equal buckets.
-- Here we segment zones into 4 quartiles based on total trip count.
-- =============================================================================

WITH zone_trips AS (
    SELECT
        z.borough,
        z.zone,
        COUNT(*) AS total_trips,
        ROUND(SUM(f.total_amount), 2) AS total_revenue
    FROM fact_yellow_trips f
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    WHERE z.borough != 'Unknown'
    GROUP BY z.borough, z.zone
)
SELECT
    NTILE(4) OVER (ORDER BY total_trips) AS quartile,
    COUNT(*)                              AS zone_count,
    MIN(total_trips)                      AS min_trips,
    ROUND(AVG(total_trips), 0)            AS avg_trips,
    MAX(total_trips)                      AS max_trips,
    ROUND(SUM(total_revenue), 0)          AS total_revenue
FROM zone_trips
GROUP BY quartile
ORDER BY quartile;


-- =============================================================================
-- BONUS: FIRST_VALUE / LAST_VALUE -- Busiest and Quietest Hours per Zone
-- =============================================================================

WITH zone_hourly AS (
    SELECT
        z.zone,
        z.borough,
        EXTRACT(HOUR FROM f.pickup_datetime)::INTEGER AS hour_of_day,
        COUNT(*) AS trip_count
    FROM fact_yellow_trips f
    JOIN dim_zones z ON f.pickup_location_id = z.location_id AND z.is_current = true
    WHERE z.borough = 'Manhattan'
    GROUP BY z.zone, z.borough, hour_of_day
)
SELECT DISTINCT
    zone,
    -- Busiest hour for this zone
    FIRST_VALUE(hour_of_day) OVER (
        PARTITION BY zone
        ORDER BY trip_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS peak_hour,
    FIRST_VALUE(trip_count) OVER (
        PARTITION BY zone
        ORDER BY trip_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS peak_hour_trips,
    -- Quietest hour for this zone
    LAST_VALUE(hour_of_day) OVER (
        PARTITION BY zone
        ORDER BY trip_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS quiet_hour,
    LAST_VALUE(trip_count) OVER (
        PARTITION BY zone
        ORDER BY trip_count DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS quiet_hour_trips
FROM zone_hourly
ORDER BY peak_hour_trips DESC
LIMIT 20;
