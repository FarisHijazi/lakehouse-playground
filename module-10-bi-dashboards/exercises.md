# Module 10: Exercises -- BI & Dashboards

## Exercise 1: Define KPIs and Metrics for the Podcast Platform

**Objective**: Before building dashboards, clearly define what you are measuring and why.

**Tasks**:

1. For each business domain below, define at least three KPIs. For each KPI, specify:
   - **Name** (e.g., `monthly_active_users`)
   - **Business question it answers** (e.g., "How many users are engaging with our content?")
   - **SQL expression** (using the raw data schema)
   - **Dimensions** it can be sliced by (e.g., country, platform, subscription_type)
   - **Target / benchmark** (e.g., DAU/MAU ratio > 0.2)

   Business domains:
   - **Growth**: DAU, WAU, MAU, signup trends
   - **Engagement**: listen duration, completion rate, sessions per user
   - **Monetization**: ad revenue, fill rate, ARPU
   - **Quality**: rebuffer rate, startup time, error rate
   - **Content**: top podcasts, category mix, trending episodes

2. Identify which KPIs are **leading indicators** (predict future outcomes) vs.
   **lagging indicators** (measure past results).

3. Create a KPI hierarchy: which executive-level metric rolls up from which
   operational metrics?

**Deliverable**: A markdown document or YAML file with your KPI definitions.

---

## Exercise 2: Build an Executive Summary Dashboard

**Objective**: Build a single-page dashboard that gives leadership a quick overview
of platform health.

**Requirements**:

1. **KPI cards** at the top showing:
   - Total unique listeners (all time)
   - Average listen duration (minutes)
   - Episode completion rate
   - Total ad revenue (SAR)

2. **Trend chart**: Daily active users over time (line chart).

3. **Breakdown chart**: Listens by platform (bar chart).

4. **Category chart**: Listens by podcast category (pie/donut chart).

5. Use the Plotly `make_subplots` layout or individual figures combined in a single
   HTML file.

**Data sources**: `listening_events/*.jsonl`, `episodes.json`, `podcasts.json`,
`ad_events.json`

**Starter hint**:
```python
import duckdb
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
con = duckdb.connect()

# Load listening events
events = con.execute(f"""
    SELECT * FROM read_json_auto('{DATA_DIR}/listening_events/*.jsonl')
""").df()
```

**Solution**: `solutions/executive_dashboard.py`

---

## Exercise 3: Build a Podcast Performance Deep-Dive

**Objective**: Create a dashboard that lets content teams analyze individual podcast
and episode performance.

**Requirements**:

1. **Top 10 podcasts** by total plays (horizontal bar chart).
2. **Top 10 episodes** by unique listeners (horizontal bar chart).
3. **Listens over time by podcast** (stacked area chart, top 5 podcasts).
4. **Episode completion rate by podcast** (bar chart comparing completion rates).
5. **Listen duration distribution** (histogram of listened_seconds).
6. **Summary table** of all podcasts with columns: name, total_plays,
   unique_listeners, avg_duration_min, completion_rate.

**Data sources**: `listening_events/*.jsonl`, `episodes.json`, `podcasts.json`

**Solution**: `solutions/podcast_performance.py`

---

## Exercise 4: Build a User Engagement Analysis

**Objective**: Understand how users engage with the platform over time using cohort
analysis and retention metrics.

**Requirements**:

1. **User signup cohort chart**: Number of users who signed up each month (bar chart).
2. **Listening frequency distribution**: Histogram of events per user.
3. **User activity heatmap**: Events by day-of-week and hour-of-day.
4. **Platform usage over time**: Stacked area of events by platform (ios, android,
   web, etc.).
5. **Subscription type breakdown**: Pie chart of free vs. premium vs. trial users.
6. **Power users table**: Top 20 users by total listened minutes.

**Data sources**: `listening_events/*.jsonl`, `users.csv`

**Solution**: `solutions/user_engagement.py`

---

## Exercise 5: Build an Ad Revenue Dashboard

**Objective**: Track advertising performance and revenue for the platform.

**Requirements**:

1. **KPI cards**: Total revenue, total impressions, average CPM, click-through rate.
2. **Revenue over time**: Monthly revenue trend (bar chart).
3. **Revenue by ad type**: pre_roll, mid_roll, post_roll breakdown (stacked bar).
4. **Top advertisers**: Bar chart of revenue by advertiser.
5. **Ad funnel**: impression -> click -> complete conversion funnel (funnel chart).
6. **Campaign performance table**: Revenue, impressions, CTR, and completion rate
   per campaign.

**Data sources**: `ad_events.json`

**Solution**: `solutions/ad_revenue.py`

---

## Exercise 6: Build a Streaming Quality Monitoring Dashboard

**Objective**: Monitor CDN and streaming quality to ensure a smooth listener
experience.

**Requirements**:

1. **KPI cards**: Median startup time, average rebuffer ratio, error rate, total
   sessions.
2. **Startup time distribution**: Histogram of startup_time_ms.
3. **Rebuffer ratio over time**: Line chart of daily average rebuffer_ratio.
4. **Error breakdown**: Bar chart of error types and their frequency.
5. **Quality by CDN node**: Table showing avg startup time and error rate per CDN
   node.
6. **Bitrate distribution**: Pie chart of sessions by bitrate.
7. **Quality by ISP**: Comparison of rebuffer ratio across ISPs.

**Data sources**: `cdn_logs.csv`

**Solution**: `solutions/streaming_quality.py`

---

## Exercise 7: Build a Geographic Heatmap of Listeners

**Objective**: Visualize the geographic distribution of listeners and identify
growth markets.

**Requirements**:

1. **World choropleth map**: Listeners by country (color intensity).
2. **Top 10 countries** by listener count (horizontal bar chart).
3. **Platform preference by country**: Stacked bar showing platform mix per top
   country.
4. **Subscription type by country**: What % of users in each country are premium?
5. **Listening volume by country**: Total listened hours per country.
6. **Summary table**: Country, listeners, total_hours, avg_session_min,
   pct_premium.

**Data sources**: `listening_events/*.jsonl`, `users.csv`

**Solution**: `solutions/geographic_analysis.py`

---

## Exercise 8: Create a Metrics Definitions Document (the "Metrics Layer")

**Objective**: Programmatically define, compute, and validate all platform metrics
in one place.

**Requirements**:

1. Read metric definitions from `metrics/metrics.yml`.
2. For each metric, compute its current value from the raw data.
3. Generate a summary report showing:
   - Metric name and description
   - Current value
   - Trend (up/down compared to previous period)
   - Status (green/yellow/red based on thresholds)
4. Output the report as both:
   - A printed console table
   - An HTML page with styled metric cards

**Data sources**: All raw data files, `metrics/metrics.yml`

**Solution**: `solutions/metrics_definitions.py`

---

## Bonus Challenges

### Bonus 1: Real-Time Dashboard Simulation

Add a "live refresh" simulation to the executive dashboard by regenerating data
every few seconds using Plotly's animation frames.

### Bonus 2: Dashboard Embedding

Create a simple Flask or FastAPI app that serves your Plotly dashboards as embedded
pages with a navigation sidebar.

### Bonus 3: Alerting Layer

Write a script that reads `metrics/metrics.yml`, computes current values, and
sends a Slack-style alert (print to console) whenever a metric crosses its
warning or critical threshold.
