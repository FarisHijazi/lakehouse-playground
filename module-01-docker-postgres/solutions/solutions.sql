-- =============================================================================
-- Module 01: Solutions
-- =============================================================================
-- Solutions for all exercises in exercises.md.
-- Try to solve them yourself first!
-- =============================================================================


-- =============================================================================
-- Exercise 3a: Top 10 Episodes by Total Listen Time
-- =============================================================================
-- Join listening_events to episodes and podcasts to get human-readable names.
-- SUM(listened_seconds) gives total engagement; COUNT(DISTINCT user_id) gives reach.

SELECT
    p.name_en                           AS podcast_name,
    e.title                             AS episode_title,
    SUM(le.listened_seconds)            AS total_listened_seconds,
    COUNT(DISTINCT le.user_id)          AS listener_count
FROM listening_events le
JOIN episodes e ON e.episode_id = le.episode_id
JOIN podcasts p ON p.podcast_id = e.podcast_id
GROUP BY p.name_en, e.title
ORDER BY total_listened_seconds DESC
LIMIT 10;


-- =============================================================================
-- Exercise 3b: Daily Active Listeners (DAL)
-- =============================================================================
-- Truncate timestamps to date, then count distinct users per day.
-- This is the most fundamental engagement metric.

SELECT
    DATE(event_timestamp)               AS day,
    COUNT(DISTINCT user_id)             AS unique_listeners
FROM listening_events
GROUP BY DATE(event_timestamp)
ORDER BY day;


-- =============================================================================
-- Exercise 3c: User Retention - Week 1 vs Week 5
-- =============================================================================
-- Cohort retention: compare early engagement to later engagement.
-- Uses CTEs for clarity -- each step is independently testable.

WITH cohort AS (
    -- Users who signed up in 2022
    SELECT user_id, signup_date
    FROM users
    WHERE signup_date >= '2022-01-01'
      AND signup_date < '2023-01-01'
),
week1_active AS (
    -- Cohort members with at least one listen in their first 7 days
    SELECT DISTINCT c.user_id
    FROM cohort c
    JOIN listening_events le ON le.user_id = c.user_id
    WHERE le.event_timestamp >= c.signup_date
      AND le.event_timestamp < c.signup_date + INTERVAL '7 days'
),
week5_active AS (
    -- Cohort members with at least one listen in days 29-35
    SELECT DISTINCT c.user_id
    FROM cohort c
    JOIN listening_events le ON le.user_id = c.user_id
    WHERE le.event_timestamp >= c.signup_date + INTERVAL '28 days'
      AND le.event_timestamp < c.signup_date + INTERVAL '35 days'
)
SELECT
    (SELECT COUNT(*) FROM cohort)        AS cohort_size,
    (SELECT COUNT(*) FROM week1_active)  AS week1_listeners,
    (SELECT COUNT(*) FROM week5_active)  AS week5_listeners,
    ROUND(
        100.0 * (SELECT COUNT(*) FROM week5_active) /
        NULLIF((SELECT COUNT(*) FROM week1_active), 0),
        2
    )                                    AS retention_rate_pct;


-- =============================================================================
-- Exercise 3d: Podcast Completion Rate
-- =============================================================================
-- Completion rate = complete events / total events per podcast.
-- Also include average episode duration to check if shorter episodes complete more.

SELECT
    p.name_en                                         AS podcast_name,
    COUNT(*)                                          AS total_events,
    COUNT(*) FILTER (WHERE le.event_type = 'complete') AS complete_events,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE le.event_type = 'complete') / COUNT(*),
        2
    )                                                 AS completion_rate_pct,
    ROUND(AVG(e.duration_seconds) / 60.0, 1)          AS avg_episode_minutes
FROM listening_events le
JOIN episodes e ON e.episode_id = le.episode_id
JOIN podcasts p ON p.podcast_id = e.podcast_id
GROUP BY p.name_en
ORDER BY completion_rate_pct DESC;


-- =============================================================================
-- Exercise 3e: Revenue by Advertiser
-- =============================================================================
-- Calculate ad performance metrics per advertiser.
-- CTR (click-through rate) is clicks/impressions -- the standard ad metric.

SELECT
    advertiser,
    ROUND(SUM(revenue_sar), 2)                        AS total_revenue_sar,
    COUNT(*) FILTER (WHERE action = 'impression')     AS impressions,
    COUNT(*) FILTER (WHERE action = 'click')          AS clicks,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE action = 'click') /
        NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0),
        2
    )                                                 AS ctr_pct,
    ROUND(
        SUM(revenue_sar) /
        NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0),
        4
    )                                                 AS avg_revenue_per_impression
FROM ad_events
GROUP BY advertiser
ORDER BY total_revenue_sar DESC;


-- =============================================================================
-- Exercise 3f: Platform Distribution Over Time
-- =============================================================================
-- Use DATE_TRUNC to bucket by quarter, then FILTER to count per platform.
-- The percentage calculation shows platform mix shift over time.

SELECT
    TO_CHAR(DATE_TRUNC('quarter', event_timestamp), 'YYYY-"Q"Q') AS quarter,
    COUNT(*)                                                       AS total_events,
    ROUND(100.0 * COUNT(*) FILTER (WHERE platform = 'ios')       / COUNT(*), 1) AS ios_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE platform = 'android')   / COUNT(*), 1) AS android_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE platform = 'web')       / COUNT(*), 1) AS web_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE platform = 'car_play')  / COUNT(*), 1) AS car_play_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE platform = 'smart_speaker') / COUNT(*), 1) AS smart_speaker_pct
FROM listening_events
GROUP BY DATE_TRUNC('quarter', event_timestamp)
ORDER BY quarter;


-- =============================================================================
-- Exercise 4c: Composite Index for Dashboard Query
-- =============================================================================
-- The query filters on event_type and event_timestamp, then groups by episode_id.
-- A composite index on (event_type, event_timestamp) helps the WHERE clause.
-- Including episode_id enables an index-only scan for the GROUP BY.

CREATE INDEX idx_events_type_timestamp_episode
    ON listening_events(event_type, event_timestamp, episode_id);

-- Verify with:
-- EXPLAIN ANALYZE
-- SELECT episode_id, COUNT(*) as plays, SUM(listened_seconds) as total_seconds
-- FROM listening_events
-- WHERE event_type = 'play'
--   AND event_timestamp >= '2023-06-01'
--   AND event_timestamp < '2023-07-01'
-- GROUP BY episode_id
-- ORDER BY total_seconds DESC
-- LIMIT 20;


-- =============================================================================
-- Exercise 5a: Podcast Performance Dashboard View
-- =============================================================================

CREATE OR REPLACE VIEW v_podcast_performance AS
SELECT
    p.podcast_id,
    p.name                                             AS podcast_name_ar,
    p.name_en                                          AS podcast_name_en,
    p.category,
    COUNT(DISTINCT e.episode_id)                       AS episode_count,
    COUNT(le.event_id)                                 AS total_events,
    ROUND(SUM(COALESCE(le.listened_seconds, 0)) / 3600.0, 1) AS total_listened_hours,
    COUNT(DISTINCT le.user_id)                         AS unique_listeners,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE le.event_type = 'complete') /
        NULLIF(COUNT(le.event_id), 0),
        2
    )                                                  AS completion_rate_pct,
    MAX(e.published_at)                                AS most_recent_episode
FROM podcasts p
LEFT JOIN episodes e ON e.podcast_id = p.podcast_id
LEFT JOIN listening_events le ON le.episode_id = e.episode_id
GROUP BY p.podcast_id, p.name, p.name_en, p.category;


-- =============================================================================
-- Exercise 5b: Daily Metrics View
-- =============================================================================

CREATE OR REPLACE VIEW v_daily_metrics AS
WITH daily_listening AS (
    SELECT
        DATE(event_timestamp)              AS day,
        COUNT(DISTINCT user_id)            AS unique_listeners,
        COUNT(*)                           AS total_events,
        ROUND(SUM(listened_seconds) / 3600.0, 1) AS listened_hours
    FROM listening_events
    GROUP BY DATE(event_timestamp)
),
daily_signups AS (
    SELECT
        signup_date                        AS day,
        COUNT(*)                           AS new_users
    FROM users
    WHERE signup_date IS NOT NULL
    GROUP BY signup_date
),
daily_revenue AS (
    SELECT
        DATE(ad_timestamp)                 AS day,
        ROUND(SUM(revenue_sar), 2)         AS ad_revenue_sar
    FROM ad_events
    GROUP BY DATE(ad_timestamp)
)
SELECT
    dl.day,
    dl.unique_listeners,
    dl.total_events,
    dl.listened_hours,
    COALESCE(ds.new_users, 0)              AS new_users,
    COALESCE(dr.ad_revenue_sar, 0)         AS ad_revenue_sar
FROM daily_listening dl
LEFT JOIN daily_signups ds ON ds.day = dl.day
LEFT JOIN daily_revenue dr ON dr.day = dl.day
ORDER BY dl.day;


-- =============================================================================
-- Exercise 5c: User Segments View
-- =============================================================================
-- Segments users by engagement in the last 90 days (relative to max date in data).
-- Finds each user's favorite podcast by listen count.

CREATE OR REPLACE VIEW v_user_segments AS
WITH data_boundary AS (
    SELECT MAX(event_timestamp) AS max_ts FROM listening_events
),
user_activity AS (
    SELECT
        u.user_id,
        u.name,
        COUNT(le.event_id) FILTER (
            WHERE le.event_timestamp >= (SELECT max_ts FROM data_boundary) - INTERVAL '90 days'
        )                                              AS total_events_90d,
        MAX(le.event_timestamp)                        AS last_listen_date
    FROM users u
    LEFT JOIN listening_events le ON le.user_id = u.user_id
    GROUP BY u.user_id, u.name
),
favorite_podcast AS (
    -- For each user, find the podcast with the most listening events
    SELECT DISTINCT ON (le.user_id)
        le.user_id,
        p.name_en                                      AS favorite_podcast
    FROM listening_events le
    JOIN episodes e ON e.episode_id = le.episode_id
    JOIN podcasts p ON p.podcast_id = e.podcast_id
    GROUP BY le.user_id, p.name_en
    ORDER BY le.user_id, COUNT(*) DESC
)
SELECT
    ua.user_id,
    ua.name,
    CASE
        WHEN ua.total_events_90d >= 50 THEN 'Power User'
        WHEN ua.total_events_90d >= 10 THEN 'Regular'
        WHEN ua.total_events_90d >= 1  THEN 'Casual'
        ELSE 'Dormant'
    END                                                AS segment,
    ua.total_events_90d,
    ua.last_listen_date,
    fp.favorite_podcast
FROM user_activity ua
LEFT JOIN favorite_podcast fp ON fp.user_id = ua.user_id;


-- =============================================================================
-- Exercise 5d: CDN Health View
-- =============================================================================

CREATE OR REPLACE VIEW v_cdn_health AS
WITH daily_cdn AS (
    SELECT
        DATE(log_timestamp)                                AS day,
        COUNT(*)                                           AS total_requests,
        COUNT(*) FILTER (WHERE error_type IS NOT NULL)     AS error_count,
        ROUND(AVG(startup_time_ms), 0)                     AS avg_startup_ms,
        ROUND(AVG(rebuffer_ratio)::numeric, 4)             AS avg_rebuffer_ratio,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY startup_time_ms) AS p95_startup_ms
    FROM cdn_logs
    GROUP BY DATE(log_timestamp)
),
worst_node AS (
    -- Per day, which CDN node had the highest error rate?
    SELECT DISTINCT ON (DATE(log_timestamp))
        DATE(log_timestamp)                                AS day,
        cdn_node,
        COUNT(*) FILTER (WHERE error_type IS NOT NULL)     AS node_errors,
        COUNT(*)                                           AS node_total
    FROM cdn_logs
    GROUP BY DATE(log_timestamp), cdn_node
    HAVING COUNT(*) >= 5  -- Minimum sample size to avoid noise
    ORDER BY DATE(log_timestamp),
             COUNT(*) FILTER (WHERE error_type IS NOT NULL)::float / COUNT(*) DESC
)
SELECT
    dc.day,
    dc.total_requests,
    dc.error_count,
    ROUND(100.0 * dc.error_count / dc.total_requests, 2)  AS error_rate_pct,
    dc.avg_startup_ms,
    dc.avg_rebuffer_ratio,
    ROUND(dc.p95_startup_ms::numeric, 0)                   AS p95_startup_ms,
    wn.cdn_node                                            AS worst_cdn_node,
    wn.node_errors                                         AS worst_node_errors
FROM daily_cdn dc
LEFT JOIN worst_node wn ON wn.day = dc.day
ORDER BY dc.day;
