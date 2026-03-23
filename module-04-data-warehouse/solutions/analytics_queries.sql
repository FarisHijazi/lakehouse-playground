-- =============================================================================
-- Module 04: Analytics Queries -- Solutions
-- =============================================================================
-- These queries run against the star schema in warehouse.duckdb.
-- Open the warehouse with:  duckdb warehouse.duckdb
--
-- Each query corresponds to an exercise in exercises.md (Exercise 5).
-- =============================================================================


-- =============================================================================
-- Exercise 5a: Top 10 Podcasts by Total Listening Hours (Per Month)
-- =============================================================================
-- Approach:
--   1. Join fact_listens -> dim_episodes -> dim_podcasts to get podcast names.
--   2. Join to dim_dates to extract year-month.
--   3. Aggregate listened_seconds per podcast per month.
--   4. Use RANK() window function to find the top 10 each month.
-- =============================================================================

WITH monthly_hours AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        p.name_en                             AS podcast_name,
        -- Convert seconds to hours for readability
        ROUND(SUM(f.listened_seconds) / 3600.0, 1) AS total_hours
    FROM fact_listens f
    JOIN dim_episodes ep ON f.episode_key = ep.episode_key
    JOIN dim_podcasts p  ON ep.podcast_id = p.podcast_id AND p.is_current = true
    JOIN dim_dates    d  ON f.date_key    = d.date_key
    GROUP BY d.year, d.month, d.month_name, p.name_en
),
ranked AS (
    SELECT
        *,
        -- RANK() so ties get the same rank; allows >10 rows if there are ties
        RANK() OVER (
            PARTITION BY year, month
            ORDER BY total_hours DESC
        ) AS rank_in_month
    FROM monthly_hours
)
SELECT
    year,
    month,
    month_name,
    podcast_name,
    total_hours,
    rank_in_month
FROM ranked
WHERE rank_in_month <= 10
ORDER BY year, month, rank_in_month;


-- =============================================================================
-- Exercise 5b: User Cohort Retention
-- =============================================================================
-- Approach:
--   1. Define each user's cohort as their signup month (from dim_users).
--   2. For each listening event, calculate how many months after signup it occurred.
--   3. Count distinct active users per cohort per month-offset.
--   4. Divide by cohort size to get retention rate.
--
-- This is a classic cohort retention analysis used by product teams to measure
-- how well they retain users over time.
-- =============================================================================

WITH user_cohorts AS (
    -- Assign each user to their signup month cohort
    SELECT
        user_key,
        DATE_TRUNC('month', signup_date)::DATE AS cohort_month
    FROM dim_users
    WHERE signup_date IS NOT NULL
),
cohort_sizes AS (
    -- Count users per cohort (denominator for retention rate)
    SELECT
        cohort_month,
        COUNT(*) AS cohort_size
    FROM user_cohorts
    GROUP BY cohort_month
),
user_activity AS (
    -- For each user listen, calculate months since signup
    SELECT DISTINCT
        uc.cohort_month,
        f.user_key,
        -- DATEDIFF in months gives us the "months since signup" offset
        DATEDIFF('month', uc.cohort_month, f.event_date) AS months_since_signup
    FROM fact_listens f
    JOIN user_cohorts uc ON f.user_key = uc.user_key
    WHERE f.event_date >= uc.cohort_month
)
SELECT
    ua.cohort_month,
    ua.months_since_signup,
    cs.cohort_size,
    COUNT(DISTINCT ua.user_key) AS active_users,
    ROUND(COUNT(DISTINCT ua.user_key)::DOUBLE / cs.cohort_size, 4) AS retention_rate
FROM user_activity ua
JOIN cohort_sizes cs ON ua.cohort_month = cs.cohort_month
WHERE ua.months_since_signup <= 12   -- Show up to 12 months of retention
GROUP BY ua.cohort_month, ua.months_since_signup, cs.cohort_size
ORDER BY ua.cohort_month, ua.months_since_signup;


-- =============================================================================
-- Exercise 5c: Ad Revenue by Show
-- =============================================================================
-- Approach:
--   1. Join fact_ad_events to fact_listens via event_id to find which episode
--      each ad was attached to.
--   2. Join through dim_episodes to dim_podcasts to get podcast names.
--   3. Calculate total revenue, impression count, click count, CTR, and RPM.
--
-- Note: RPM (Revenue Per Mille) = revenue per 1000 impressions.
-- CTR (Click-Through Rate) = clicks / impressions.
-- =============================================================================

WITH ad_attribution AS (
    -- Link each ad event to its podcast through the listening event chain:
    -- ad_event -> listening_event (event_id) -> episode -> podcast
    SELECT
        a.ad_event_id,
        a.action,
        a.revenue_sar,
        a.advertiser,
        p.name_en AS podcast_name,
        p.category
    FROM fact_ad_events a
    -- Join to fact_listens to find which episode the ad was played during
    JOIN fact_listens fl ON a.event_id = fl.event_id
    -- Walk up the dimension chain to get podcast info
    JOIN dim_episodes ep ON fl.episode_key = ep.episode_key
    JOIN dim_podcasts p  ON ep.podcast_id = p.podcast_id AND p.is_current = true
)
SELECT
    podcast_name,
    category,
    -- Total revenue in SAR
    ROUND(SUM(revenue_sar), 2)                                  AS total_revenue_sar,
    -- Count impressions (all ad views)
    COUNT(*) FILTER (WHERE action = 'impression')               AS impressions,
    -- Count clicks
    COUNT(*) FILTER (WHERE action = 'click')                    AS clicks,
    -- Click-through rate: what percentage of impressions led to a click
    ROUND(
        COUNT(*) FILTER (WHERE action = 'click')::DOUBLE /
        NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0) * 100,
        2
    )                                                           AS ctr_pct,
    -- Revenue per 1000 impressions
    ROUND(
        SUM(revenue_sar) /
        NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0) * 1000,
        2
    )                                                           AS rpm_sar
FROM ad_attribution
GROUP BY podcast_name, category
ORDER BY total_revenue_sar DESC;


-- =============================================================================
-- Exercise 5d: Peak Listening Hours
-- =============================================================================
-- Approach:
--   1. Extract day_of_week and hour_of_day from event_timestamp.
--   2. Aggregate total listens and average listened_seconds.
--   3. Result is "heatmap-ready": rows are day-hour combinations.
--
-- This tells the business when users are most active so they can schedule
-- new episode releases and ad campaigns.
-- =============================================================================

SELECT
    d.day_of_week,
    d.day_name,
    EXTRACT(HOUR FROM f.event_timestamp)::INTEGER AS hour_of_day,
    COUNT(*)                                       AS total_listens,
    ROUND(AVG(f.listened_seconds), 0)              AS avg_listened_seconds
FROM fact_listens f
JOIN dim_dates d ON f.date_key = d.date_key
GROUP BY d.day_of_week, d.day_name, hour_of_day
ORDER BY d.day_of_week, hour_of_day;


-- =============================================================================
-- Exercise 5e: Streaming Quality by ISP
-- =============================================================================
-- Approach:
--   1. Aggregate CDN quality metrics per ISP.
--   2. Calculate error rate as the percentage of requests with non-null error_type.
--
-- This helps the platform team identify which ISPs have the worst streaming
-- experience so they can work with CDN providers to improve routing.
-- =============================================================================

SELECT
    isp,
    COUNT(*)                                                    AS total_requests,
    ROUND(AVG(startup_time_ms), 0)                              AS avg_startup_ms,
    ROUND(AVG(rebuffer_ratio), 4)                               AS avg_rebuffer_ratio,
    -- Error rate: percentage of requests that had any error
    ROUND(
        COUNT(*) FILTER (WHERE error_type IS NOT NULL AND error_type != '')::DOUBLE
        / COUNT(*) * 100,
        2
    )                                                           AS error_rate_pct,
    ROUND(AVG(bytes_transferred) / (1024.0 * 1024.0), 1)       AS avg_mb_transferred
FROM fact_cdn_quality
GROUP BY isp
ORDER BY total_requests DESC;


-- =============================================================================
-- Exercise 8a: Power Listeners Analysis
-- =============================================================================
-- Approach:
--   1. Calculate total listening time per user.
--   2. Find the 95th percentile threshold (top 5% = power listeners).
--   3. Compare power listeners vs regular listeners on key metrics.
--
-- Uses CTEs to build the analysis step by step for readability.
-- =============================================================================

WITH user_totals AS (
    -- Total listening stats per user
    SELECT
        f.user_key,
        SUM(f.listened_seconds)         AS total_seconds,
        COUNT(*)                        AS total_listens,
        AVG(f.listened_seconds)         AS avg_session_seconds,
        COUNT(DISTINCT ep.podcast_id)   AS unique_podcasts
    FROM fact_listens f
    JOIN dim_episodes ep ON f.episode_key = ep.episode_key
    GROUP BY f.user_key
),
threshold AS (
    -- Find the 95th percentile of total listening time
    SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_seconds) AS p95
    FROM user_totals
),
classified AS (
    -- Label each user as power listener or regular
    SELECT
        ut.*,
        u.subscription_type,
        u.platform,
        CASE WHEN ut.total_seconds >= t.p95 THEN 'Power Listener'
             ELSE 'Regular'
        END AS listener_type
    FROM user_totals ut
    CROSS JOIN threshold t
    JOIN dim_users u ON ut.user_key = u.user_key
)
SELECT
    listener_type,
    COUNT(*)                                    AS user_count,
    ROUND(AVG(total_seconds) / 3600.0, 1)       AS avg_total_hours,
    ROUND(AVG(avg_session_seconds), 0)           AS avg_session_seconds,
    ROUND(AVG(unique_podcasts), 1)               AS avg_unique_podcasts,
    ROUND(AVG(total_listens), 0)                 AS avg_total_listens
FROM classified
GROUP BY listener_type
ORDER BY listener_type;


-- =============================================================================
-- Exercise 8b: Podcast Similarity (Shared Listeners)
-- =============================================================================
-- Approach:
--   1. For each user, find all podcasts they listened to.
--   2. Self-join to find pairs of podcasts with shared listeners.
--   3. Calculate Jaccard similarity: |A intersect B| / |A union B|
--
-- This is useful for recommendation systems: "listeners of X also enjoy Y."
-- =============================================================================

WITH user_podcasts AS (
    -- Distinct user-podcast combinations
    SELECT DISTINCT
        f.user_key,
        ep.podcast_id,
        p.name_en AS podcast_name
    FROM fact_listens f
    JOIN dim_episodes ep ON f.episode_key = ep.episode_key
    JOIN dim_podcasts p  ON ep.podcast_id = p.podcast_id AND p.is_current = true
),
podcast_listeners AS (
    -- Count total unique listeners per podcast (for union calculation)
    SELECT podcast_id, podcast_name, COUNT(DISTINCT user_key) AS listener_count
    FROM user_podcasts
    GROUP BY podcast_id, podcast_name
),
pairs AS (
    -- Self-join to find shared listeners between podcast pairs
    SELECT
        a.podcast_id   AS podcast_a_id,
        a.podcast_name AS podcast_a,
        b.podcast_id   AS podcast_b_id,
        b.podcast_name AS podcast_b,
        COUNT(DISTINCT a.user_key) AS shared_listeners
    FROM user_podcasts a
    JOIN user_podcasts b ON a.user_key = b.user_key
                       AND a.podcast_id < b.podcast_id   -- Avoid duplicates and self-pairs
    GROUP BY a.podcast_id, a.podcast_name, b.podcast_id, b.podcast_name
)
SELECT
    p.podcast_a,
    p.podcast_b,
    p.shared_listeners,
    la.listener_count AS listeners_a,
    lb.listener_count AS listeners_b,
    -- Jaccard similarity = intersection / union
    -- |A union B| = |A| + |B| - |A intersect B|
    ROUND(
        p.shared_listeners::DOUBLE /
        (la.listener_count + lb.listener_count - p.shared_listeners),
        4
    ) AS jaccard_similarity
FROM pairs p
JOIN podcast_listeners la ON p.podcast_a_id = la.podcast_id
JOIN podcast_listeners lb ON p.podcast_b_id = lb.podcast_id
ORDER BY shared_listeners DESC
LIMIT 20;


-- =============================================================================
-- Exercise 8c: Funnel Analysis
-- =============================================================================
-- Approach:
--   Build a listening funnel per podcast category:
--     Stage 1: Users who started an episode (event_type in start, play, resume)
--     Stage 2: Users who listened past 50% completion
--     Stage 3: Users who completed the episode (event_type = 'complete')
--   Calculate conversion rates between each stage.
--
-- Funnel analysis reveals where users drop off, helping content creators
-- understand engagement quality by category.
-- =============================================================================

WITH listen_data AS (
    -- Base data: every listen with category and completion
    SELECT
        f.user_key,
        f.event_type,
        f.completion_pct,
        p.category
    FROM fact_listens f
    JOIN dim_episodes ep ON f.episode_key = ep.episode_key
    JOIN dim_podcasts p  ON ep.podcast_id = p.podcast_id AND p.is_current = true
),
funnel AS (
    SELECT
        category,
        -- Stage 1: all users who had any listening event
        COUNT(DISTINCT user_key) AS stage1_started,
        -- Stage 2: users who listened past 50%
        COUNT(DISTINCT user_key) FILTER (
            WHERE completion_pct > 0.5
        ) AS stage2_past_50pct,
        -- Stage 3: users who completed the episode
        COUNT(DISTINCT user_key) FILTER (
            WHERE event_type = 'complete'
        ) AS stage3_completed
    FROM listen_data
    GROUP BY category
)
SELECT
    category,
    stage1_started,
    stage2_past_50pct,
    stage3_completed,
    -- Conversion from start to 50%
    ROUND(stage2_past_50pct::DOUBLE / NULLIF(stage1_started, 0) * 100, 1)
        AS pct_start_to_50,
    -- Conversion from 50% to complete
    ROUND(stage3_completed::DOUBLE / NULLIF(stage2_past_50pct, 0) * 100, 1)
        AS pct_50_to_complete,
    -- Overall conversion: start to complete
    ROUND(stage3_completed::DOUBLE / NULLIF(stage1_started, 0) * 100, 1)
        AS pct_start_to_complete
FROM funnel
ORDER BY stage1_started DESC;


-- =============================================================================
-- Exercise 8d: Revenue Attribution
-- =============================================================================
-- Approach:
--   Trace ad revenue back to podcasts using the attribution chain:
--     ad_event -> listening_event (via event_id) -> episode -> podcast
--   Calculate revenue per podcast, per listen, and per listening hour.
--
-- Uses CTEs to build the chain step by step for clarity.
-- =============================================================================

WITH attributed_revenue AS (
    -- Step 1: Link each ad event to its podcast
    SELECT
        a.ad_event_id,
        a.revenue_sar,
        p.podcast_id,
        p.name_en AS podcast_name
    FROM fact_ad_events a
    JOIN fact_listens fl  ON a.event_id = fl.event_id
    JOIN dim_episodes ep  ON fl.episode_key = ep.episode_key
    JOIN dim_podcasts p   ON ep.podcast_id = p.podcast_id AND p.is_current = true
),
podcast_listening AS (
    -- Step 2: Total listening stats per podcast (for per-listen and per-hour metrics)
    SELECT
        ep.podcast_id,
        COUNT(*) AS total_listens,
        SUM(f.listened_seconds) / 3600.0 AS total_hours
    FROM fact_listens f
    JOIN dim_episodes ep ON f.episode_key = ep.episode_key
    GROUP BY ep.podcast_id
),
podcast_revenue AS (
    -- Step 3: Aggregate revenue per podcast
    SELECT
        podcast_id,
        podcast_name,
        ROUND(SUM(revenue_sar), 2)  AS total_revenue_sar,
        COUNT(*)                     AS ad_events
    FROM attributed_revenue
    GROUP BY podcast_id, podcast_name
)
SELECT
    pr.podcast_name,
    pr.total_revenue_sar,
    pr.ad_events,
    pl.total_listens,
    ROUND(pl.total_hours, 1)                                   AS total_listening_hours,
    -- Revenue per listen: how much revenue each listen generates
    ROUND(pr.total_revenue_sar / NULLIF(pl.total_listens, 0), 4)  AS revenue_per_listen,
    -- Revenue per hour: monetisation efficiency
    ROUND(pr.total_revenue_sar / NULLIF(pl.total_hours, 0), 2)    AS revenue_per_hour
FROM podcast_revenue pr
JOIN podcast_listening pl ON pr.podcast_id = pl.podcast_id
ORDER BY total_revenue_sar DESC;
