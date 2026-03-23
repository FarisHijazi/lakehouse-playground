# Module 10: Exercises -- BI & Dashboards

## Exercise 1: Define KPIs and Metrics for the Taxi Analytics Platform

**Objective**: Before building dashboards, clearly define what you are measuring and why.

**Tasks**:

1. For each business domain below, define at least three KPIs. For each KPI, specify:
   - **Name** (e.g., `avg_fare_amount`)
   - **Business question it answers** (e.g., "What is the average fare passengers pay?")
   - **SQL expression** (using the raw data schema)
   - **Dimensions** it can be sliced by (e.g., borough, payment_type, vendor)
   - **Target / benchmark** (e.g., avg fare > $10)

   Business domains:
   - **Trip Volume**: daily trips, trips per hour, peak hour demand
   - **Revenue**: total revenue, average fare, tip percentage, revenue per mile
   - **Trip Characteristics**: average distance, average duration, speed
   - **Geographic**: trips by borough, top pickup zones, popular routes
   - **Payment**: payment type mix, credit card rate, tip by payment type

2. Identify which KPIs are **leading indicators** (predict future outcomes) vs.
   **lagging indicators** (measure past results).

3. Create a KPI hierarchy: which executive-level metric rolls up from which
   operational metrics?

**Deliverable**: A markdown document or YAML file with your KPI definitions.

---

## Exercise 2: Build an Executive Summary Dashboard

**Objective**: Build a single-page dashboard that gives leadership a quick overview
of taxi fleet performance.

**Requirements**:

1. **KPI cards** at the top showing:
   - Total trips
   - Average fare amount ($)
   - Average trip distance (miles)
   - Total revenue ($)

2. **Trend chart**: Daily trip count over time (line chart).

3. **Breakdown chart**: Trips by borough (bar chart).

4. **Payment chart**: Trips by payment type (pie/donut chart).

5. Use the Plotly `make_subplots` layout or individual figures combined in a single
   HTML file.

**Data sources**: `yellow_tripdata_*.parquet`, `taxi_zone_lookup.csv`,
`payment_types.csv`

**Starter hint**:
```python
import duckdb
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
con = duckdb.connect()

# Load yellow taxi trips
trips = con.execute(f"""
    SELECT * FROM read_parquet('{DATA_DIR}/yellow_tripdata_*.parquet')
""").df()
```

**Solution**: `solutions/executive_dashboard.py`

---

## Exercise 3: Build a Geographic Analysis Dashboard

**Objective**: Create a dashboard that visualizes taxi trip patterns across NYC
boroughs and zones, identifying popular pickup/dropoff locations and routes.

**Requirements**:

1. **Top 15 pickup zones** by trip count (horizontal bar chart).
2. **Top 15 dropoff zones** by trip count (horizontal bar chart).
3. **Borough-to-borough flow**: Heatmap of trip counts between pickup and dropoff boroughs.
4. **Top 10 most popular routes** (zone pair with highest trip count, table).
5. **Trips by borough over time**: Daily trip count per borough (stacked area chart).
6. **Summary table** of all boroughs with columns: borough, total_trips,
   avg_fare, avg_distance, avg_tip_pct.

**Data sources**: `yellow_tripdata_*.parquet`, `taxi_zone_lookup.csv`

**Solution**: `solutions/geographic_analysis.py`

---

## Exercise 4: Create a Metrics Definitions Document (the "Metrics Layer")

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

### Bonus 4: Weather Impact Analysis

Join taxi trip data with `nyc_weather_2023.csv` to analyze how weather conditions
(rain, snow, temperature) affect trip volume, fares, and tip percentages. Build
a dashboard comparing rainy vs. dry day metrics.

### Bonus 5: Uber/Lyft Comparison

Use the `fhvhv_tripdata` files to compare ride-hail (Uber/Lyft) trip patterns
against yellow and green taxi trips. Analyze differences in trip distance,
driver pay, and geographic coverage.
