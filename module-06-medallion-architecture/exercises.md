# Module 06 - Medallion Architecture: Exercises

These exercises walk you through building a complete medallion architecture for a
podcast platform, progressing from raw data ingestion (Bronze) through cleaning
(Silver) to business-ready aggregates (Gold).

**Prerequisites:**
```bash
pip install pandas pyarrow duckdb
```

**Data Location:** All raw data is at `data/raw/` relative to the project root.

**Output Locations:**
- Bronze: `data/bronze/`
- Silver: `data/silver/`
- Gold: `data/gold/`

**Important:** Run exercises in order. Gold depends on Silver, which depends on Bronze.

---

## Exercise 1: Build the Bronze Layer

**Goal:** Land all raw data sources into a structured Bronze layer as Parquet files,
preserving the original data exactly as-is, with added ingestion metadata.

**Input files:**
- `data/raw/podcasts.json`
- `data/raw/episodes.json`
- `data/raw/users.csv`
- `data/raw/listening_events/events_YYYY-MM-DD.jsonl` (daily files)
- `data/raw/cdn_logs.csv`
- `data/raw/ad_events.json`

**Requirements:**

1. Read each raw source file into a pandas DataFrame without applying any data
   cleaning or type coercion. Use `dtype=str` for CSV files to avoid premature
   type inference.

2. Add ingestion metadata columns to every record:
   - `_ingested_at`: Current ISO timestamp (when the pipeline ran)
   - `_source_file`: The name of the original source file
   - `_batch_id`: A UUID identifying this ingestion batch

3. For listening events (daily JSONL files):
   - Read all JSONL files from the `listening_events/` directory
   - Add an `_ingested_date` column extracted from the filename (e.g., `events_2024-01-01.jsonl` yields `2024-01-01`)
   - Write as Parquet partitioned by `_ingested_date`

4. Write each dataset to `data/bronze/` as Parquet files:
   - `data/bronze/podcasts/`
   - `data/bronze/episodes/`
   - `data/bronze/users/`
   - `data/bronze/listening_events/` (partitioned by `_ingested_date`)
   - `data/bronze/cdn_logs/`
   - `data/bronze/ad_events/`

5. Print summary statistics: row counts, column names, and null counts for each
   dataset.

**Key Principle:** Do NOT clean anything. Mixed date formats, duplicate records,
null values, inconsistent gender values -- all of these stay exactly as they are
in the source. Bronze is a faithful archive.

**Hints:**
- `pd.read_json("file.json")` for JSON arrays
- `pd.read_csv("file.csv", dtype=str)` to prevent type inference
- For JSONL: read line by line with `json.loads()` or use `pd.read_json(path, lines=True)`
- `df.to_parquet(path, engine="pyarrow", index=False)`
- For partitioned writes: `df.to_parquet(path, partition_cols=["_ingested_date"], engine="pyarrow")`

---

## Exercise 2: Build the Silver Layer - Users

**Goal:** Transform Bronze users into a clean, deduplicated, properly-typed Silver
table.

**Input:** `data/bronze/users/` (Parquet)

**Output:** `data/silver/users.parquet`

**Requirements:**

1. **Parse dates:** The `signup_date` column has mixed formats:
   - `2024-01-15` (ISO format)
   - `15/01/2024` (DD/MM/YYYY)
   - `01-15-2024` (MM-DD-YYYY)
   - `2024-01-15T00:00:00` (ISO with time)

   Parse all into consistent `datetime64` type. Use `pd.to_datetime()` with
   `format="mixed"` and `dayfirst=False`.

2. **Normalize gender values:** Map the inconsistent values to a standard set:
   - `m`, `M`, `male`, `Male` -> `male`
   - `f`, `F`, `female`, `Female` -> `female`
   - Empty string, NaN, null -> `unknown`

3. **Deduplicate:** Some user IDs appear multiple times (the source system sent
   corrections). Keep the record with the latest `signup_date` for each `user_id`.

4. **Handle nulls:**
   - `age`: Fill nulls with median age, cast to integer
   - `city`: Fill nulls with `"Unknown"`
   - `email`: Leave nulls as-is (some users do not have email)

5. **Add derived columns:**
   - `signup_year`: Extracted from `signup_date`
   - `age_group`: Categorize age into `"13-17"`, `"18-24"`, `"25-34"`, `"35-44"`,
     `"45-54"`, `"55-64"`, `"65+"`

6. **Validate:**
   - Reject records where `user_id` is null
   - Reject records where `age` < 13 or `age` > 120
   - Write rejected records to `data/silver/users_quarantine.parquet`

7. Print: total records in Bronze, duplicates removed, records quarantined,
   final Silver count, gender distribution, age group distribution.

**Hints:**
- `pd.cut()` for age group bucketing
- `df.sort_values("signup_date").drop_duplicates(subset=["user_id"], keep="last")`
- Use a gender mapping dictionary with `.str.strip().str.lower().map()`

---

## Exercise 3: Build the Silver Layer - Listening Events

**Goal:** Clean and enrich the Bronze listening events for analytics use.

**Input:** `data/bronze/listening_events/` (partitioned Parquet)

**Output:** `data/silver/listening_events.parquet`

**Requirements:**

1. **Deduplicate:** Remove records with duplicate `event_id`. Keep the first
   occurrence.

2. **Filter bot/test events:** Remove any events where `user_id` starts with
   `usr_000000` or `app_version` is `"0.0.0"` (these are test/bot events).

3. **Validate:**
   - `listened_seconds` must be >= 0
   - `event_type` must be one of: `play`, `pause`, `resume`, `complete`, `skip`, `seek`
   - `timestamp` must be a valid datetime
   - Quarantine invalid records to `data/silver/listening_events_quarantine.parquet`

4. **Type casting:**
   - Parse `timestamp` to datetime
   - Cast `listened_seconds` to integer

5. **Add derived columns:**
   - `event_date`: Date portion of timestamp
   - `event_hour`: Hour of the event (0-23)
   - `listened_minutes`: `listened_seconds / 60`, rounded to 2 decimals

6. **Join with episodes** (from Bronze) to add `podcast_id` and `duration_seconds`
   to each event. Compute `completion_rate = listened_seconds / duration_seconds`,
   capped at 1.0.

7. Print: total Bronze records, duplicates removed, bot/test events filtered,
   records quarantined, final Silver count, event type distribution, sample
   completion rates.

**Hints:**
- Read partitioned parquet with `pd.read_parquet("data/bronze/listening_events/")`
  which reads all partitions
- Use `pd.merge()` with `how="left"` to join with episodes
- Clip completion_rate: `df["completion_rate"].clip(upper=1.0)`

---

## Exercise 4: Build the Silver Layer - CDN Logs

**Goal:** Clean CDN streaming logs, fix data quality issues, and enrich with
derived fields.

**Input:** `data/bronze/cdn_logs/` (Parquet)

**Output:** `data/silver/cdn_logs.parquet`

**Requirements:**

1. **Fix negative values:** The `startup_time_ms` column contains some negative
   values (sensor errors). Replace negative values with the column median.

2. **Validate:**
   - `bytes_transferred` must be > 0
   - `rebuffer_ratio` must be between 0 and 1
   - `timestamp` must be a valid datetime
   - Quarantine invalid records

3. **Type casting:**
   - Parse `timestamp` to datetime
   - Cast numeric columns to appropriate types

4. **Enrich with ISP mapping:** Create a tier classification for ISPs:
   - Tier 1 (major): `STC`, `Mobily`, `Zain`
   - Tier 2 (regional): `Etisalat`, `Ooredoo`, `du`, `Batelco`
   - Tier 3: Everything else (including `Unknown`)

   Add an `isp_tier` column.

5. **Add derived columns:**
   - `event_date`: Date from timestamp
   - `cdn_region`: Extract from `cdn_node` (e.g., `cdn-ruh-09` -> `ruh`)
   - `has_error`: Boolean, True if `error_type` is not null/empty
   - `quality_score`: Composite score = `1 - rebuffer_ratio` scaled to 0-100

6. Print: total records, negative values fixed, records quarantined, ISP tier
   distribution, CDN region distribution, average quality score.

**Hints:**
- `df["cdn_node"].str.split("-").str[1]` to extract region
- `df["startup_time_ms"].clip(lower=0)` is an alternative to median replacement
- Use `.fillna()` carefully with the `error_type` column

---

## Exercise 5: Build the Gold Layer - Daily Listening Metrics

**Goal:** Create a daily aggregate table showing platform-wide listening metrics,
ready for a dashboard.

**Input:** `data/silver/listening_events.parquet`

**Output:** `data/gold/daily_listening_metrics.parquet`

**Requirements:**

1. Group by `event_date` and compute:
   - `dau` (Daily Active Users): Count of unique `user_id`
   - `total_events`: Count of all events
   - `total_listens`: Count of events where `event_type = 'play'`
   - `total_listened_hours`: Sum of `listened_minutes / 60`, rounded to 2 decimals
   - `avg_listened_minutes`: Average `listened_minutes` per play event
   - `avg_completion_rate`: Average `completion_rate` across play events
   - `unique_episodes`: Count of distinct `episode_id`
   - `unique_podcasts`: Count of distinct `podcast_id`

2. Sort by `event_date` ascending.

3. Add rolling metrics:
   - `dau_7d_avg`: 7-day rolling average of DAU
   - `listens_7d_avg`: 7-day rolling average of total_listens

4. Print: date range covered, total days, average DAU, peak DAU (and date),
   sample of the first and last 5 rows.

**Hints:**
- `df.groupby("event_date").agg(...)` with named aggregation
- `df["dau_7d_avg"] = df["dau"].rolling(7, min_periods=1).mean()`
- Use `.round(2)` for clean output

---

## Exercise 6: Build the Gold Layer - Podcast Performance

**Goal:** Create a podcast-level performance summary table for content analytics.

**Input:**
- `data/silver/listening_events.parquet`
- `data/bronze/podcasts/` (for podcast metadata)
- `data/bronze/episodes/` (for episode metadata)

**Output:** `data/gold/podcast_performance.parquet`

**Requirements:**

1. For each podcast, compute:
   - `total_listens`: Count of play events
   - `unique_listeners`: Unique user count
   - `total_listened_hours`: Sum of listened time in hours
   - `avg_completion_rate`: Average completion rate
   - `total_episodes`: Count of distinct episodes with at least one listen
   - `avg_listens_per_episode`: `total_listens / total_episodes`
   - `listener_retention_rate`: Percentage of users who listened to more than
     one episode of the same podcast

2. Join with podcast metadata to include: `name_en`, `category`, `language`.

3. Rank podcasts by `total_listens` descending (add `rank` column).

4. Print: top 10 podcasts by listens, category breakdown, total unique listeners
   across platform.

---

## Exercise 7: Build the Gold Layer - User Retention Cohorts

**Goal:** Build a cohort retention analysis table showing how well the platform
retains users over time.

**Input:**
- `data/silver/listening_events.parquet`
- `data/silver/users.parquet`

**Output:** `data/gold/user_retention_cohorts.parquet`

**Requirements:**

1. Define cohorts by `signup_year` and `signup_month` (from Silver users).

2. For each cohort, compute monthly activity: did the user have at least one
   listening event in each subsequent month?

3. Build a cohort retention table with columns:
   - `cohort` (e.g., `2022-01`)
   - `month_offset` (0 = signup month, 1 = next month, etc.)
   - `active_users`: Count of users in the cohort who were active in that month
   - `cohort_size`: Total users in the cohort
   - `retention_rate`: `active_users / cohort_size`

4. Limit to month offsets 0 through 12 (one year of retention).

5. Print: cohort sizes, month-0 activation rates, month-6 retention rates,
   best and worst retaining cohorts.

**Hints:**
- Create a `cohort` column: `users["signup_date"].dt.to_period("M")`
- For each listening event, compute `activity_month = event_date.to_period("M")`
- `month_offset = activity_month - cohort_month` (using Period arithmetic)
- Use `pd.crosstab()` or groupby-pivot for the retention matrix

---

## Exercise 8: Build the Gold Layer - Ad Revenue Analytics

**Goal:** Create an ad revenue analytics table for the monetization team.

**Input:**
- `data/bronze/ad_events/` (Parquet from Bronze)
- `data/silver/listening_events.parquet` (for context enrichment)

**Output:** `data/gold/ad_revenue.parquet`

**Requirements:**

1. Clean ad events:
   - Parse `timestamp` to datetime
   - Extract `event_date` from timestamp
   - Validate `revenue_sar` >= 0
   - Deduplicate by `ad_event_id`

2. Compute daily revenue metrics grouped by `event_date`, `ad_type`, and
   `advertiser`:
   - `total_impressions`: Count where `action = 'impression'`
   - `total_clicks`: Count where `action = 'click'`
   - `total_completes`: Count where `action = 'complete'`
   - `total_skips`: Count where `action = 'skip'`
   - `total_revenue_sar`: Sum of `revenue_sar`
   - `avg_revenue_per_event`: Mean `revenue_sar`
   - `ctr` (click-through rate): `total_clicks / total_impressions`
   - `completion_rate`: `total_completes / (total_impressions + total_clicks + total_completes + total_skips)`

3. Also create a monthly summary rolled up by `advertiser`:
   - Write to `data/gold/ad_revenue_monthly.parquet`

4. Print: total ad revenue, top advertisers by revenue, revenue by ad type,
   average CTR, monthly revenue trend (first and last 3 months).

---

## Exercise 9: Implement Schema Evolution

**Goal:** Demonstrate how the medallion architecture handles a new column appearing
in source data.

**Scenario:** The podcast platform adds a `preferred_language` column to user data.
Show how each layer adapts.

**Requirements:**

1. Simulate schema evolution:
   - Read the existing Bronze users Parquet
   - Create a "new batch" of user data that includes a `preferred_language` column
   - Append the new batch to Bronze using schema union (all columns from both old
     and new schemas)

2. Update the Silver layer:
   - Read the evolved Bronze data (which now has `preferred_language` for some rows
     and null for older rows)
   - Fill `preferred_language` nulls with `"unknown"`
   - Process through the same Silver cleaning logic

3. Show the schema before and after evolution.

4. Demonstrate that existing Gold queries still work (backward compatibility)
   and that the new column can be used in new Gold queries.

5. Print: schema diff (before vs after), null counts for new column, sample of
   evolved records.

**Hints:**
- `pd.concat([old_df, new_df])` automatically unions schemas if columns differ
- `pyarrow.parquet.read_schema()` to inspect Parquet schema without reading data
- Use `join="outer"` in concat for schema merging

---

## Exercise 10: Implement MERGE / Upsert Pattern

**Goal:** Implement idempotent writes using the MERGE/upsert pattern with DuckDB,
simulating what Delta Lake's MERGE command does.

**Scenario:** User data arrives in batches. Some records are new users, others are
updates to existing users (e.g., changed subscription, updated email). Running the
pipeline twice with the same data should produce the same result (idempotency).

**Requirements:**

1. Create an initial Silver users table using the first batch of data.

2. Generate an "update batch" that contains:
   - Some existing users with updated fields (e.g., subscription changed from
     `free` to `premium`)
   - Some new users not in the original batch

3. Implement a MERGE/upsert using DuckDB SQL:
   ```sql
   -- Pseudocode for the MERGE pattern
   INSERT OR REPLACE INTO silver_users
   SELECT * FROM incoming_batch
   WHERE incoming_batch.user_id = silver_users.user_id
   -- When matched: update the existing record
   -- When not matched: insert as new record
   ```

4. Verify idempotency: run the upsert twice with the same data and confirm the
   row count does not change.

5. Show the audit trail: which records were inserted vs. updated.

6. Print: initial count, incoming batch size, new inserts, updates, final count,
   idempotency check result.

**Hints:**
- DuckDB can read Parquet directly: `duckdb.sql("SELECT * FROM 'file.parquet'")`
- Use `CREATE TABLE ... AS SELECT` for the initial load
- For MERGE simulation:
  ```python
  conn.execute("""
      INSERT OR REPLACE INTO target
      SELECT * FROM incoming
  """)
  ```
- Or use the full MERGE syntax in DuckDB (supported since v0.8):
  ```sql
  MERGE INTO target USING source ON target.id = source.id
  WHEN MATCHED THEN UPDATE SET ...
  WHEN NOT MATCHED THEN INSERT ...
  ```

---

## Verification Checklist

After completing all exercises, verify your pipeline:

```bash
# Check that all output directories exist
ls data/bronze/
ls data/silver/
ls data/gold/

# Check file sizes (should all be > 0)
du -sh data/bronze/*
du -sh data/silver/*
du -sh data/gold/*
```

Expected outputs:
- Bronze: 6 datasets (podcasts, episodes, users, listening_events, cdn_logs, ad_events)
- Silver: 3 cleaned datasets + quarantine files (users, listening_events, cdn_logs)
- Gold: 4+ aggregate tables (daily_metrics, podcast_performance, user_retention, ad_revenue)

Each Gold table should be directly usable for visualization in Module 10 (BI Dashboards).
