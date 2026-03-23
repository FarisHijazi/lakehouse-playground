# Module 01 Exercises: Docker & Postgres

Work through these exercises in order. Each builds on the previous one. Resist the
urge to look at `solutions/solutions.sql` until you have tried each exercise yourself.

---

## Exercise 1: Start the Environment

**Goal:** Stand up Postgres and pgAdmin using Docker Compose.

### Tasks

1. From `module-01-docker-postgres/`, run:
   ```bash
   docker compose up -d
   ```
2. Verify both containers are running:
   ```bash
   docker compose ps
   ```
3. Check Postgres is healthy:
   ```bash
   docker compose logs postgres
   ```
   You should see `database system is ready to accept connections`.

4. Connect using `psql`:
   ```bash
   docker compose exec postgres psql -U lakehouse -d podcast_platform
   ```
5. Run `\dt` to list tables. You should see: `podcasts`, `episodes`, `users`,
   `listening_events`, `cdn_logs`, `ad_events`.

6. Open pgAdmin at `http://localhost:8080`. Add a server connection:
   - Host: `postgres` (the Docker service name)
   - Port: `5432`
   - Username: `lakehouse`
   - Password: `lakehouse123`

### Checkpoint
- [ ] Both containers are running (`docker compose ps` shows 2 services)
- [ ] You can connect to Postgres via psql
- [ ] You can see the empty tables with `\dt`
- [ ] pgAdmin is accessible at localhost:8080

---

## Exercise 2: Load Raw Data into Postgres

**Goal:** Get the raw data from `/data/raw/` into Postgres tables.

### 2a: Load Podcasts (Warm-up)

Write a Python script (or use the provided `load_data.py`) to load `podcasts.json`
into the `podcasts` table.

**Hints:**
- Use `psycopg2` to connect: `psycopg2.connect(host='localhost', port=5432, user='lakehouse', password='lakehouse123', dbname='podcast_platform')`
- Read the JSON file, iterate rows, INSERT each one
- Or use `psycopg2.extras.execute_values()` for batch inserts

**Verify:**
```sql
SELECT COUNT(*) FROM podcasts;
-- Expected: 10

SELECT podcast_id, name_en, category FROM podcasts LIMIT 5;
```

### 2b: Load Episodes

Load `episodes.json` into the `episodes` table.

**Watch out for:** The `published_at` field has format `'2019-03-15 00:00:00'` --
you may need to parse it as a timestamp.

**Verify:**
```sql
SELECT COUNT(*) FROM episodes;
-- Expected: 784

SELECT podcast_id, COUNT(*) FROM episodes GROUP BY podcast_id ORDER BY COUNT(*) DESC;
```

### 2c: Load Users (This is where it gets real)

Load `users.csv` into the `users` table. The raw data is intentionally messy:

- **Date formats are inconsistent:** `2024-09-10`, `23/09/2022`, `2022-09-21T00:00:00`, `03-09-2019`
- **Gender values are inconsistent:** `m`, `male`, `M`, `f`, `female`, `F`
- **Some ages are missing** (empty string)
- **Some cities are missing** (empty string)
- **Subscription types need normalization:** `premium_annual` should map to `premium`

Your script must handle all of this. This is what real data engineering looks like.

**Verify:**
```sql
SELECT COUNT(*) FROM users;
-- Expected: 5000

-- Check gender was normalized
SELECT gender, COUNT(*) FROM users GROUP BY gender ORDER BY COUNT(*) DESC;
-- Should only have 'm' and 'f'

-- Check subscription was normalized
SELECT subscription_type, COUNT(*) FROM users GROUP BY subscription_type;
-- Should only have 'free', 'premium', 'trial'
```

### 2d: Load Listening Events (Bulk loading)

Load all JSONL files from `listening_events/` directory. There are ~2500 files
with a total of ~200k events.

**Performance challenge:** Loading 200k rows one-by-one with INSERT is slow.
Try these approaches and compare:

1. **Naive:** One INSERT per row. Time it.
2. **Batch:** Use `execute_values()` with batches of 1000. Time it.
3. **COPY:** Use `copy_expert()` with StringIO. Time it.

You should see a 10-50x speedup from naive to COPY.

**Verify:**
```sql
SELECT COUNT(*) FROM listening_events;
-- Expected: ~204,144

SELECT event_type, COUNT(*) FROM listening_events GROUP BY event_type ORDER BY COUNT(*) DESC;
```

### 2e: Load CDN Logs and Ad Events

Load the remaining tables:
- `cdn_logs.csv` -> `cdn_logs` (50k rows)
- `ad_events.json` -> `ad_events` (~18k rows)

**Verify:**
```sql
SELECT COUNT(*) FROM cdn_logs;
SELECT COUNT(*) FROM ad_events;
```

### Checkpoint
- [ ] All 6 tables have data
- [ ] Users were cleaned (consistent genders, normalized subscriptions, parsed dates)
- [ ] You understand the difference between INSERT, batch INSERT, and COPY performance

---

## Exercise 3: Analytical SQL Queries

**Goal:** Write SQL queries that answer real business questions. These are the kinds
of queries a data engineering manager would ask you to support.

### 3a: Top 10 Episodes by Total Listen Time

Find the 10 episodes with the most total listened seconds. Include the podcast name
and episode title.

Expected output columns: `podcast_name`, `episode_title`, `total_listened_seconds`, `listener_count`

### 3b: Daily Active Listeners (DAL)

Calculate the number of unique listeners per day. This is the most common engagement
metric for any content platform.

Expected output columns: `day`, `unique_listeners`

Order by day. What trends do you see?

### 3c: User Retention - Week 1 vs Week 5

For users who signed up in 2022, calculate:
- How many had at least one listening event in their first 7 days?
- How many had at least one listening event in days 29-35 (week 5)?
- What is the retention rate (week 5 listeners / week 1 listeners)?

This is a simplified cohort retention analysis.

### 3d: Podcast Completion Rate

For each podcast, calculate the percentage of listening events that were "complete"
events. Which podcasts have the highest completion rate? Does episode duration
correlate with completion rate?

Expected output columns: `podcast_name`, `total_events`, `complete_events`, `completion_rate`

### 3e: Revenue by Advertiser

Calculate total ad revenue (in SAR) by advertiser. Also calculate:
- Number of impressions
- Number of clicks
- Click-through rate (clicks / impressions)
- Average revenue per impression

Order by total revenue descending.

### 3f: Platform Distribution Over Time

For each quarter (YYYY-Q format), calculate the percentage of listening events from
each platform (ios, android, web, car_play, smart_speaker). How has the platform
mix shifted over time?

### Checkpoint
- [ ] You can write JOINs across the dimension and fact tables
- [ ] You understand GROUP BY, aggregate functions, and window functions
- [ ] You can calculate retention and conversion metrics

---

## Exercise 4: Indexes and Query Plans

**Goal:** Understand how Postgres executes queries and how indexes affect performance.

### 4a: Read a Query Plan

Run this query with `EXPLAIN ANALYZE`:
```sql
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM listening_events
WHERE event_timestamp >= '2023-01-01'
  AND event_timestamp < '2024-01-01';
```

Answer these questions:
1. Is it doing a Seq Scan or Index Scan?
2. What is the estimated cost vs actual time?
3. How many rows did it estimate vs how many it actually found?

### 4b: Compare With and Without Indexes

Drop the timestamp index and re-run the query:
```sql
DROP INDEX idx_events_timestamp;

EXPLAIN ANALYZE
SELECT COUNT(*)
FROM listening_events
WHERE event_timestamp >= '2023-01-01'
  AND event_timestamp < '2024-01-01';
```

Now recreate it:
```sql
CREATE INDEX idx_events_timestamp ON listening_events(event_timestamp);
```

Compare the two plans. How much faster is the indexed version?

### 4c: Composite Index Design

Consider this query that runs frequently in your analytics dashboard:
```sql
SELECT episode_id, COUNT(*) as plays, SUM(listened_seconds) as total_seconds
FROM listening_events
WHERE event_type = 'play'
  AND event_timestamp >= '2023-06-01'
  AND event_timestamp < '2023-07-01'
GROUP BY episode_id
ORDER BY total_seconds DESC
LIMIT 20;
```

1. Run `EXPLAIN ANALYZE` on it as-is.
2. Create a composite index that would help this query. Think about column order.
3. Run `EXPLAIN ANALYZE` again and compare.

**Hint:** The most selective column should generally come first in a composite index.

### 4d: Partial Index

CDN error analysis is a common SRE query, but most CDN logs have no errors. A partial
index is perfect here:

```sql
-- This index already exists in init.sql:
-- CREATE INDEX idx_cdn_error_type ON cdn_logs(error_type) WHERE error_type IS NOT NULL;
```

Compare the query plan for:
```sql
EXPLAIN ANALYZE
SELECT error_type, COUNT(*), AVG(rebuffer_ratio)
FROM cdn_logs
WHERE error_type IS NOT NULL
GROUP BY error_type;
```

Drop the partial index, replace it with a full index, and compare the size:
```sql
SELECT pg_size_pretty(pg_relation_size('idx_cdn_error_type'));
```

### Checkpoint
- [ ] You can read EXPLAIN ANALYZE output (node types, costs, actual times, rows)
- [ ] You understand when Postgres chooses Seq Scan vs Index Scan
- [ ] You can design composite indexes for multi-column filter queries
- [ ] You know when partial indexes save space

---

## Exercise 5: Views for Analytics

**Goal:** Create views that encapsulate business logic. In a real platform, downstream
teams (product, growth, finance) query views -- not raw tables.

### 5a: Podcast Performance Dashboard View

Create a view `v_podcast_performance` that shows, for each podcast:
- Podcast name (Arabic and English)
- Category
- Number of episodes
- Total listening events
- Total listened hours
- Unique listeners
- Average completion rate
- Most recent episode date

This is the view a product manager would query every morning.

### 5b: Daily Metrics View

Create a view `v_daily_metrics` that shows, for each day:
- Unique listeners (DAL)
- Total listening events
- Total listened hours
- New users (signed up that day)
- Revenue from ads

This is the view that feeds the executive dashboard.

### 5c: User Segments View

Create a view `v_user_segments` that classifies each user into engagement segments:
- **Power User:** 50+ listening events in the last 90 days
- **Regular:** 10-49 events in the last 90 days
- **Casual:** 1-9 events in the last 90 days
- **Dormant:** 0 events in the last 90 days

Include: `user_id`, `name`, `segment`, `total_events_90d`, `last_listen_date`,
`favorite_podcast` (the one they listened to most)

### 5d: CDN Health View

Create a view `v_cdn_health` that shows daily CDN performance:
- Error rate (% of requests with errors)
- Average startup time
- Average rebuffer ratio
- P95 startup time (use `PERCENTILE_CONT`)
- Worst performing CDN node

### Checkpoint
- [ ] Your views produce correct results (spot-check against raw queries)
- [ ] You understand the difference between views and materialized views
- [ ] You could explain to a product manager what each view shows

---

## Bonus: Think Like a Data Engineer

After completing all exercises, reflect on these questions:

1. **Schema evolution:** If the platform adds a "bookmarks" feature, what tables
   and indexes would you add? How do you alter the schema without downtime?

2. **Data quality:** The users table has messy data. In production, where should
   data cleaning happen -- in the ingestion script, in the database (CHECK constraints),
   or in a transformation layer (dbt)? What are the tradeoffs?

3. **Scale:** The listening_events table has 200k rows. At 1M events/day, what
   changes would you make? (Hint: partitioning, archival, pre-aggregation.)

4. **Observability:** How would you monitor this database in production? What
   metrics would you alert on?
