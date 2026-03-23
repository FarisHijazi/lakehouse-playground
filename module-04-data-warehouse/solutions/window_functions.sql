-- =============================================================================
-- Module 04: Window Functions -- Solutions
-- =============================================================================
-- These queries run against the star schema in warehouse.duckdb.
-- Each demonstrates a different window function pattern.
--
-- Window functions perform calculations across a set of rows RELATED to the
-- current row, without collapsing them like GROUP BY. The key clauses are:
--   OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)
-- =============================================================================


-- =============================================================================
-- Exercise 7a: Running Total of Listens Per Podcast
-- =============================================================================
-- Window: PARTITION BY podcast, ORDER BY date
-- Function: SUM() as a running (cumulative) total
--
-- The frame defaults to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- when ORDER BY is specified, giving us a running sum from the first day
-- up to and including the current row's date.
-- =============================================================================

WITH daily_counts AS (
    -- First aggregate to daily level per podcast
    SELECT
        p.name_en AS podcast_name,
        f.event_date,
        COUNT(*) AS daily_listens
    FROM fact_listens f
    JOIN dim_episodes ep ON f.episode_key = ep.episode_key
    JOIN dim_podcasts p  ON ep.podcast_id = p.podcast_id AND p.is_current = true
    GROUP BY p.name_en, f.event_date
)
SELECT
    podcast_name,
    event_date,
    daily_listens,
    -- Running total: sum of all daily_listens from the beginning up to this date
    SUM(daily_listens) OVER (
        PARTITION BY podcast_name
        ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_listens
FROM daily_counts
ORDER BY podcast_name, event_date;


-- =============================================================================
-- Exercise 7b: Rank Users by Listening Time
-- =============================================================================
-- Demonstrates the difference between three ranking functions:
--   RANK()       - gaps after ties     (1, 2, 2, 4)
--   DENSE_RANK() - no gaps after ties  (1, 2, 2, 3)
--   ROW_NUMBER() - always unique       (1, 2, 3, 4)
--
-- All three produce the same result when there are no ties.
-- =============================================================================

WITH user_listening AS (
    SELECT
        u.user_id,
        u.name,
        ROUND(SUM(f.listened_seconds) / 3600.0, 2) AS total_hours
    FROM fact_listens f
    JOIN dim_users u ON f.user_key = u.user_key
    GROUP BY u.user_id, u.name
)
SELECT
    user_id,
    name,
    total_hours,
    -- RANK: 1, 2, 2, 4 (skips rank 3 because two users tied for rank 2)
    RANK()       OVER (ORDER BY total_hours DESC) AS rank_with_gaps,
    -- DENSE_RANK: 1, 2, 2, 3 (no gap -- next rank after a tie is consecutive)
    DENSE_RANK() OVER (ORDER BY total_hours DESC) AS rank_dense,
    -- ROW_NUMBER: 1, 2, 3, 4 (always unique -- ties broken arbitrarily)
    ROW_NUMBER() OVER (ORDER BY total_hours DESC) AS row_num
FROM user_listening
ORDER BY total_hours DESC
LIMIT 50;


-- =============================================================================
-- Exercise 7c: 7-Day Moving Average of Daily Listens
-- =============================================================================
-- Window: ORDER BY date, with an explicit frame of 6 preceding rows + current
-- Function: AVG() over a sliding window of 7 days
--
-- Moving averages smooth out daily noise to reveal trends. A 7-day window
-- also smooths out day-of-week effects (weekday vs weekend patterns).
-- =============================================================================

WITH daily_totals AS (
    SELECT
        event_date,
        COUNT(*) AS daily_listens
    FROM fact_listens
    GROUP BY event_date
)
SELECT
    event_date,
    daily_listens,
    -- 7-day moving average: average of the current day and the 6 preceding days
    ROUND(
        AVG(daily_listens) OVER (
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        1
    ) AS moving_avg_7d,
    -- For comparison: 30-day moving average for longer-term trend
    ROUND(
        AVG(daily_listens) OVER (
            ORDER BY event_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ),
        1
    ) AS moving_avg_30d
FROM daily_totals
ORDER BY event_date;


-- =============================================================================
-- Exercise 7d: Month-over-Month Growth Rate
-- =============================================================================
-- Window function: LAG() to reference the previous month's value
--
-- LAG(column, offset, default) looks back `offset` rows in the partition.
-- We use it to calculate growth rate:
--   growth_rate = (current_month - previous_month) / previous_month * 100
-- =============================================================================

WITH monthly_hours AS (
    SELECT
        p.name_en AS podcast_name,
        DATE_TRUNC('month', f.event_date)::DATE AS month,
        ROUND(SUM(f.listened_seconds) / 3600.0, 1) AS total_hours
    FROM fact_listens f
    JOIN dim_episodes ep ON f.episode_key = ep.episode_key
    JOIN dim_podcasts p  ON ep.podcast_id = p.podcast_id AND p.is_current = true
    GROUP BY p.name_en, DATE_TRUNC('month', f.event_date)
)
SELECT
    podcast_name,
    month,
    total_hours,
    -- Previous month's hours (LAG looks back 1 row within each podcast partition)
    LAG(total_hours, 1) OVER (
        PARTITION BY podcast_name
        ORDER BY month
    ) AS prev_month_hours,
    -- Month-over-month growth rate as a percentage
    ROUND(
        (total_hours - LAG(total_hours, 1) OVER (
            PARTITION BY podcast_name ORDER BY month
        )) /
        NULLIF(LAG(total_hours, 1) OVER (
            PARTITION BY podcast_name ORDER BY month
        ), 0) * 100,
        1
    ) AS mom_growth_pct
FROM monthly_hours
ORDER BY podcast_name, month;


-- =============================================================================
-- Exercise 7e: Percentile Distribution of Listen Duration
-- =============================================================================
-- Window function: PERCENTILE_CONT() for continuous percentile calculation
--
-- Percentiles show the distribution shape:
--   - If p50 (median) is much lower than p95, the distribution is right-skewed
--     (most users listen briefly, a few listen very long)
--   - If p25 is close to p75, the distribution is tight (most users behave similarly)
-- =============================================================================

SELECT
    p.category,
    COUNT(*) AS total_listens,
    -- 25th percentile: 25% of listens are shorter than this
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY f.listened_seconds), 0)
        AS p25_seconds,
    -- 50th percentile (median): the "typical" listen duration
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.listened_seconds), 0)
        AS p50_median_seconds,
    -- 75th percentile: 75% of listens are shorter than this
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY f.listened_seconds), 0)
        AS p75_seconds,
    -- 95th percentile: captures the long-tail listeners
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.listened_seconds), 0)
        AS p95_seconds,
    -- For context: show the average too (often pulled up by outliers)
    ROUND(AVG(f.listened_seconds), 0) AS avg_seconds
FROM fact_listens f
JOIN dim_episodes ep ON f.episode_key = ep.episode_key
JOIN dim_podcasts p  ON ep.podcast_id = p.podcast_id AND p.is_current = true
GROUP BY p.category
ORDER BY total_listens DESC;


-- =============================================================================
-- BONUS: First and Last Listen Per User (FIRST_VALUE / LAST_VALUE)
-- =============================================================================
-- Demonstrates FIRST_VALUE and LAST_VALUE window functions.
-- For each user, find the first and last podcast they ever listened to.
-- =============================================================================

WITH user_listen_order AS (
    SELECT
        u.user_id,
        u.name AS user_name,
        p.name_en AS podcast_name,
        f.event_date,
        f.event_timestamp,
        -- First podcast this user ever listened to
        FIRST_VALUE(p.name_en) OVER (
            PARTITION BY u.user_id
            ORDER BY f.event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_podcast,
        -- Most recent podcast this user listened to
        LAST_VALUE(p.name_en) OVER (
            PARTITION BY u.user_id
            ORDER BY f.event_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_podcast,
        -- Row number to pick just one row per user
        ROW_NUMBER() OVER (PARTITION BY u.user_id ORDER BY f.event_timestamp) AS rn
    FROM fact_listens f
    JOIN dim_users u     ON f.user_key = u.user_key
    JOIN dim_episodes ep ON f.episode_key = ep.episode_key
    JOIN dim_podcasts p  ON ep.podcast_id = p.podcast_id AND p.is_current = true
)
SELECT
    user_id,
    user_name,
    first_podcast,
    last_podcast,
    -- Did the user's first and last podcast differ? (exploration indicator)
    CASE WHEN first_podcast != last_podcast THEN 'Yes' ELSE 'No' END AS explored_new_shows
FROM user_listen_order
WHERE rn = 1
ORDER BY user_id
LIMIT 30;


-- =============================================================================
-- BONUS: NTILE -- Segment Users into Listening Quartiles
-- =============================================================================
-- NTILE(n) divides ordered rows into n roughly equal buckets.
-- Here we segment users into 4 quartiles based on total listening time.
-- =============================================================================

WITH user_hours AS (
    SELECT
        u.user_id,
        u.name,
        u.subscription_type,
        ROUND(SUM(f.listened_seconds) / 3600.0, 2) AS total_hours
    FROM fact_listens f
    JOIN dim_users u ON f.user_key = u.user_key
    GROUP BY u.user_id, u.name, u.subscription_type
)
SELECT
    -- NTILE(4) assigns each user to a quartile (1=lowest, 4=highest)
    NTILE(4) OVER (ORDER BY total_hours) AS quartile,
    COUNT(*) AS user_count,
    ROUND(MIN(total_hours), 2) AS min_hours,
    ROUND(AVG(total_hours), 2) AS avg_hours,
    ROUND(MAX(total_hours), 2) AS max_hours
FROM user_hours
GROUP BY quartile
ORDER BY quartile;
