# Module 04: Exercises -- Data Warehouse

Work through these exercises in order. Each builds on the previous one. Use DuckDB as your warehouse engine.

---

## Exercise 1: Design a Star Schema

**Goal:** Draw (on paper or in a diagram tool) the star schema for the podcast analytics warehouse.

**Tasks:**

1. Identify the **business processes** we want to analyse:
   - Listening behaviour (who listened to what, when, for how long)
   - Ad revenue (which ads were shown, clicks, revenue)
   - CDN quality (streaming quality, buffer events, errors)

2. For each business process, identify:
   - The **grain** (one row = one ___?)
   - The **dimensions** (who, what, where, when)
   - The **measures** (numeric facts to aggregate)

3. Design these tables:

   **Dimension Tables:**
   - `dim_users` -- user attributes
   - `dim_podcasts` -- podcast attributes
   - `dim_episodes` -- episode attributes
   - `dim_dates` -- calendar dimension (a row per day)

   **Fact Tables:**
   - `fact_listens` -- one row per listening event
   - `fact_ad_events` -- one row per ad impression or click
   - `fact_cdn_quality` -- one row per CDN log entry

4. For each table, list the columns, data types, and key relationships.

**Hints:**
- Look at the raw data files to understand available columns.
- Fact tables should contain only foreign keys (to dimensions) and numeric measures.
- Push descriptive attributes into dimension tables.

---

## Exercise 2: Create Dimension Tables

**Goal:** Write SQL (using DuckDB) to create the four dimension tables from raw data.

### 2a: dim_dates

Create a date dimension covering 2018-01-01 to 2025-12-31. Include:

| Column | Type | Description |
|---|---|---|
| `date_key` | INTEGER | YYYYMMDD format (e.g., 20240115) |
| `full_date` | DATE | The actual date |
| `year` | INTEGER | 2024 |
| `quarter` | INTEGER | 1-4 |
| `month` | INTEGER | 1-12 |
| `month_name` | VARCHAR | 'January', 'February', ... |
| `day_of_month` | INTEGER | 1-31 |
| `day_of_week` | INTEGER | 1 (Monday) to 7 (Sunday) |
| `day_name` | VARCHAR | 'Monday', 'Tuesday', ... |
| `week_of_year` | INTEGER | 1-53 |
| `is_weekend` | BOOLEAN | true for Saturday/Sunday |

**Hint:** Use DuckDB's `generate_series` to create a range of dates, then extract parts.

```sql
SELECT UNNEST(generate_series(DATE '2018-01-01', DATE '2025-12-31', INTERVAL 1 DAY)) AS full_date
```

### 2b: dim_users

Load from `users.csv`. Handle:
- Generate a surrogate `user_key` (integer)
- Standardise `gender` values (the raw data has 'f', 'F', 'female', 'male', 'm', 'M')
- Parse the messy `signup_date` (multiple formats: `YYYY-MM-DD`, `DD/MM/YYYY`, `YYYY-MM-DDT00:00:00`, `DD-MM-YYYY`)
- Handle NULL cities

### 2c: dim_podcasts

Load from `podcasts.json`. Include:
- Surrogate `podcast_key`
- All attributes: `podcast_id`, `name`, `name_en`, `category`, `language`, `host`, `created_at`

### 2d: dim_episodes

Load from `episodes.json`. Include:
- Surrogate `episode_key`
- All attributes plus a derived `duration_minutes` column
- Join-ready `podcast_id` for linking to `dim_podcasts`

---

## Exercise 3: Create Fact Tables

**Goal:** Create the three fact tables, joining to dimension keys.

### 3a: fact_listens

Load from `listening_events/*.jsonl`. For each listening event:
- Look up `user_key` from `dim_users`
- Look up `episode_key` from `dim_episodes`
- Look up `date_key` from `dim_dates`
- Calculate `completion_pct` = `listened_seconds / episode_duration_seconds`
- Keep: `event_id`, `user_key`, `episode_key`, `date_key`, `event_type`, `listened_seconds`, `completion_pct`, `platform`, `country`

**Hint:** DuckDB can read all JSONL files at once:
```sql
SELECT * FROM read_json_auto('data/raw/listening_events/*.jsonl')
```

### 3b: fact_ad_events

Load from `ad_events.json`. For each ad event:
- Look up `user_key`, `date_key`
- Keep: `ad_event_id`, `user_key`, `date_key`, `event_id`, `ad_type`, `action`, `advertiser`, `campaign_id`, `revenue_sar`, `duration_seconds`

### 3c: fact_cdn_quality

Load from `cdn_logs.csv`. For each CDN log entry:
- Look up `user_key`, `date_key`
- Keep: `log_id`, `user_key`, `date_key`, `event_id`, `isp`, `bitrate`, `buffer_events`, `rebuffer_ratio`, `startup_time_ms`, `error_type`, `cdn_node`, `bytes_transferred`

---

## Exercise 4: Implement SCD Type 2 for Podcasts Dimension

**Goal:** Implement Slowly Changing Dimension Type 2 to track podcast attribute changes over time.

**Scenario:** The following changes happened to our podcasts:

1. **2023-07-01**: "سوالف بزنس" (pod_001) rebrands to "سوالف بزنس وتقنية" (Swalif Business & Tech)
2. **2024-01-15**: pod_001 changes category from "Business" to "Business & Technology"
3. **2024-03-01**: "بودكاست أريكة" (pod_003) changes host from "سارة" to "سارة ونورة" (Sara & Noura)

**Tasks:**

1. Add SCD Type 2 columns to `dim_podcasts`:
   - `valid_from` (DATE)
   - `valid_to` (DATE, use '9999-12-31' for current records)
   - `is_current` (BOOLEAN)

2. Write a Python function that processes a "change event" and:
   - Closes the current record (set `valid_to` and `is_current = false`)
   - Inserts a new record with the updated values
   - Assigns a new surrogate key

3. After processing all three changes, verify that:
   - `dim_podcasts` has 13 rows (10 original + 3 new versions)
   - Querying with `is_current = true` returns 10 rows
   - Querying pod_001 returns 3 rows (original + 2 changes)

---

## Exercise 5: Analytical Queries

Write SQL queries to answer these business questions. Use the warehouse tables you built.

### 5a: Top 10 Podcasts by Total Listening Hours (Per Month)

For each month, rank podcasts by total listening hours. Show:
- Month, podcast name, total hours, rank within month

### 5b: User Cohort Retention

Group users by their signup month (cohort). For each cohort, calculate:
- How many users were active (had at least one listen) in each subsequent month
- Retention rate = active users / cohort size

**Output columns:** `cohort_month`, `months_since_signup`, `cohort_size`, `active_users`, `retention_rate`

### 5c: Ad Revenue by Show

For each podcast, calculate:
- Total ad revenue (SAR)
- Number of impressions
- Number of clicks
- Click-through rate (CTR)
- Revenue per 1000 impressions (RPM)

Join through: `fact_ad_events` -> `fact_listens` (via `event_id`) -> `dim_episodes` -> `dim_podcasts`

### 5d: Peak Listening Hours

Find the hour of day and day of week when listening is most popular. Show a heatmap-ready result:
- `day_of_week`, `hour_of_day`, `total_listens`, `avg_listened_seconds`

### 5e: Streaming Quality by ISP

For each ISP, calculate:
- Average startup time
- Average rebuffer ratio
- Error rate (percentage of requests with a non-null error)
- Average bytes transferred

---

## Exercise 6: Implement Partitioning by Date

**Goal:** Demonstrate date-based partitioning using DuckDB's Hive-partitioned Parquet export.

**Tasks:**

1. Export `fact_listens` to Hive-partitioned Parquet files:
```sql
COPY (
    SELECT *, year(event_date) AS year, month(event_date) AS month
    FROM fact_listens
)
TO 'output/fact_listens_partitioned'
(FORMAT PARQUET, PARTITION_BY (year, month));
```

2. Query the partitioned data and observe that DuckDB only reads relevant partitions:
```sql
SELECT count(*) FROM parquet_scan('output/fact_listens_partitioned/**/*.parquet', hive_partitioning=true)
WHERE year = 2023 AND month = 6;
```

3. Compare query performance (timing) between the partitioned Parquet and the DuckDB table.

---

## Exercise 7: Window Functions

Write queries using window functions. Each query should be a single SQL statement.

### 7a: Running Total of Listens Per Podcast

For each podcast, calculate the cumulative number of listens per day:
- `event_date`, `podcast_name`, `daily_listens`, `running_total_listens`

### 7b: Rank Users by Listening Time

Rank users by their total listening time. Use:
- `RANK()` -- allows gaps (1, 2, 2, 4)
- `DENSE_RANK()` -- no gaps (1, 2, 2, 3)
- `ROW_NUMBER()` -- unique (1, 2, 3, 4)

Show all three rankings side by side.

### 7c: 7-Day Moving Average of Daily Listens

Calculate a 7-day rolling average of total listens per day across the platform.

### 7d: Month-over-Month Growth Rate

For each podcast, calculate the month-over-month growth rate in listening hours.
Use `LAG()` to reference the previous month.

### 7e: Percentile Distribution of Listen Duration

For each podcast category, calculate the 25th, 50th (median), 75th, and 95th percentiles of listen duration.

---

## Exercise 8: CTEs and Complex Analytics

Write each query using Common Table Expressions (CTEs) for readability.

### 8a: Power Listeners Analysis

Find "power listeners" (users in the top 5% by total listening time) and compare their behaviour:
- Average session length
- Number of unique podcasts
- Most common platform
- Subscription type distribution

### 8b: Podcast Similarity (Content-Based)

Find pairs of podcasts that share the most listeners. For each pair:
- Number of shared listeners
- Jaccard similarity (shared / union)

### 8c: Funnel Analysis

Build a listening funnel:
1. Users who started an episode (event_type = 'start' or 'play')
2. Users who listened past 50% (completion_pct > 0.5)
3. Users who completed an episode (event_type = 'complete')

Show conversion rates between each stage, broken down by podcast category.

### 8d: Revenue Attribution

Attribute ad revenue to podcasts by tracing:
`ad_event` -> `listening_event` (via event_id) -> `episode` -> `podcast`

Calculate:
- Revenue per podcast
- Revenue per listen
- Revenue per listening hour

Use CTEs to build the attribution chain step by step.
