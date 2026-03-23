# Module 01 Exercises: Docker & Postgres

Work through these exercises in order. Each builds on the previous one.

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
   docker compose exec postgres psql -U lakehouse -d nyc_taxi
   ```
5. Run `\dt` to list tables. You should see 8 tables: `taxi_zones`, `vendors`,
   `rate_codes`, `payment_types`, `fhv_bases`, `yellow_taxi_trips`,
   `green_taxi_trips`, `fhv_trips`, `daily_weather`.

6. Open pgAdmin at `http://localhost:8080`. Add a server connection:
   - Host: `postgres` (the Docker service name)
   - Port: `5432`
   - Username: `lakehouse`
   - Password: `lakehouse123`

### Checkpoint
- [ ] Both containers are running (`docker compose ps` shows 2 services)
- [ ] You can connect to Postgres via psql
- [ ] You can see all 8 empty tables with `\dt`
- [ ] pgAdmin is accessible at localhost:8080

---

## Exercise 2: Load Raw Data into Postgres

**Goal:** Get the real NYC TLC data into Postgres tables.

### 2a: Load Dimension Tables (Warm-up)

Write a Python script (or use the provided `load_data.py`) to load the small
CSV dimension tables: `taxi_zone_lookup.csv`, `vendors.csv`, `rate_codes.csv`,
`payment_types.csv`, `fhv_bases.csv`, and `nyc_weather_2023.csv`.

**Hints:**
- Use `psycopg2` to connect: `psycopg2.connect(host='localhost', port=5432, user='lakehouse', password='lakehouse123', dbname='nyc_taxi')`
- Read each CSV, iterate rows, use `COPY` or `execute_values()` for batch loading

**Verify:**
```sql
SELECT COUNT(*) FROM taxi_zones;
-- Expected: 265

SELECT borough, COUNT(*) FROM taxi_zones GROUP BY borough ORDER BY COUNT(*) DESC;
-- Manhattan should have the most zones

SELECT * FROM vendors;
SELECT * FROM rate_codes;
SELECT * FROM payment_types;
```

### 2b: Load Yellow Taxi Trips (The Big One)

Load the yellow taxi Parquet files into `yellow_taxi_trips`. This is millions of rows.

**Performance challenge:** Try these approaches and compare:
1. **Naive:** Read parquet with pandas, iterate rows, INSERT each one. Time it.
2. **Batch:** Use `execute_values()` with batches of 5000. Time it.
3. **COPY:** Use `copy_expert()` with StringIO buffer. Time it.

You should see a 10-50x speedup from naive to COPY.

**Key learning:** The Parquet column names don't match the Postgres columns exactly
(e.g., `VendorID` vs `vendor_id`, `PULocationID` vs `pu_location_id`). Your loader
needs a column mapping.

**Verify:**
```sql
SELECT COUNT(*) FROM yellow_taxi_trips;
-- Expected: 3M+ (depends on how many months you downloaded)

-- Check for data quality issues (these exist in real data!)
SELECT COUNT(*) FROM yellow_taxi_trips WHERE fare_amount < 0;
SELECT COUNT(*) FROM yellow_taxi_trips WHERE passenger_count IS NULL;
SELECT COUNT(*) FROM yellow_taxi_trips WHERE trip_distance = 0 AND fare_amount > 0;
```

### 2c: Load Green Taxi and FHV Trips

Load the remaining trip data:
- `green_tripdata_*.parquet` → `green_taxi_trips`
- `fhvhv_tripdata_*.parquet` → `fhv_trips`

**Watch out for:** Different schemas across these three datasets. Green taxis have
`lpep_pickup_datetime` (not `tpep_`), `ehail_fee`, and `trip_type`. FHV has a
completely different structure (no fare breakdown, but has `driver_pay`, `tips`, etc.).

**Verify:**
```sql
SELECT COUNT(*) FROM green_taxi_trips;
SELECT COUNT(*) FROM fhv_trips;

-- Compare schemas
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'yellow_taxi_trips' ORDER BY ordinal_position;

SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'fhv_trips' ORDER BY ordinal_position;
```

### 2d: Load Weather Data

Load `nyc_weather_2023.csv` into `daily_weather`.

**Verify:**
```sql
SELECT COUNT(*) FROM daily_weather;
-- Expected: 365

SELECT * FROM daily_weather ORDER BY precipitation_in DESC LIMIT 5;
```

### Checkpoint
- [ ] All 8 tables have data
- [ ] You understand the COPY performance advantage over INSERT
- [ ] You handled the column name mapping between Parquet and Postgres
- [ ] You noticed the real data quality issues (nulls, negatives, zeros)

---

## Exercise 3: Analytical SQL Queries

**Goal:** Write SQL queries that answer real business questions about NYC taxi operations.

### 3a: Revenue by Borough

Calculate total fare revenue by pickup borough. Join `yellow_taxi_trips` with
`taxi_zones` to get borough names.

Expected columns: `borough`, `total_trips`, `total_revenue`, `avg_fare`

Which borough generates the most taxi revenue?

### 3b: Hourly Trip Patterns

Calculate the average number of yellow taxi trips by hour of day. What are the peak
hours? How does this compare to what you'd expect?

Expected columns: `hour_of_day`, `avg_daily_trips`, `avg_fare`, `avg_tip`

### 3c: Weather Impact on Taxi Demand

Join `yellow_taxi_trips` with `daily_weather` (by pickup date). Compare trip volume
and average fares on:
- Rainy days (precipitation > 0.1 inches) vs dry days
- Snow days (snowfall > 0) vs no-snow days
- Cold days (temp_avg < 32F) vs warm days (temp_avg > 70F)

Do New Yorkers take more taxis when it rains?

### 3d: Tipping Analysis by Payment Type

Calculate average tip percentage (`tip_amount / fare_amount`) grouped by payment type.
Join with `payment_types` for readable names.

**Important insight:** Credit card tips are recorded, but cash tips are NOT (they show
as $0). This is a classic data engineering gotcha — the data looks like cash riders
don't tip, but it's a measurement issue, not a behavioral one.

### 3e: Airport Trip Analysis

Analyze trips to/from the three NYC airports. The zone IDs are:
- JFK Airport: location_id = 132
- LaGuardia Airport: location_id = 138
- Newark Airport: location_id = 1

Calculate for each airport:
- Number of pickups and dropoffs
- Average fare, tip, and total
- Average trip distance
- Most common pickup/dropoff zone for the return trip

### 3f: Uber vs Lyft Comparison

Using the `fhv_trips` table, compare Uber (HV0003) vs Lyft (HV0005):
- Total trips
- Average trip miles and time
- Average base passenger fare
- Average tips and driver pay
- Percentage of shared rides (`shared_request_flag = 'Y'`)

### Checkpoint
- [ ] You can write JOINs across fact and dimension tables
- [ ] You understand GROUP BY, aggregate functions, and date/time extraction
- [ ] You noticed the cash tip measurement issue (real-world data literacy!)

---

## Exercise 4: Indexes and Query Plans

**Goal:** Understand how Postgres executes queries and how indexes affect performance.

### 4a: Read a Query Plan

Run this query with `EXPLAIN ANALYZE`:
```sql
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM yellow_taxi_trips
WHERE tpep_pickup_datetime >= '2023-01-01'
  AND tpep_pickup_datetime < '2023-02-01';
```

Answer these questions:
1. Is it doing a Seq Scan or Index Scan?
2. What is the estimated cost vs actual time?
3. How many rows did it estimate vs how many it actually found?

### 4b: Compare With and Without Indexes

Drop the pickup datetime index and re-run the query:
```sql
DROP INDEX idx_yellow_pickup_dt;

EXPLAIN ANALYZE
SELECT COUNT(*)
FROM yellow_taxi_trips
WHERE tpep_pickup_datetime >= '2023-01-01'
  AND tpep_pickup_datetime < '2023-02-01';
```

Now recreate it:
```sql
CREATE INDEX idx_yellow_pickup_dt ON yellow_taxi_trips(tpep_pickup_datetime);
```

Compare the two plans. How much faster is the indexed version?

### 4c: Composite Index Design

Consider this query that a revenue dashboard runs frequently:
```sql
SELECT pu_location_id, COUNT(*) as trips, SUM(total_amount) as revenue
FROM yellow_taxi_trips
WHERE tpep_pickup_datetime >= '2023-01-01'
  AND tpep_pickup_datetime < '2023-02-01'
  AND payment_type = 1  -- credit card only
GROUP BY pu_location_id
ORDER BY revenue DESC
LIMIT 20;
```

1. Run `EXPLAIN ANALYZE` on it as-is.
2. Create a composite index that would help this query. Think about column order.
3. Run `EXPLAIN ANALYZE` again and compare.

### 4d: Zone Lookup Join Optimization

This common query joins trips with zones — but zones is small (265 rows):
```sql
EXPLAIN ANALYZE
SELECT z.borough, z.zone, COUNT(*) as trips
FROM yellow_taxi_trips t
JOIN taxi_zones z ON t.pu_location_id = z.location_id
WHERE t.tpep_pickup_datetime >= '2023-01-01'
  AND t.tpep_pickup_datetime < '2023-02-01'
GROUP BY z.borough, z.zone
ORDER BY trips DESC
LIMIT 20;
```

Is Postgres using a Hash Join or Nested Loop? For small dimension tables, which is better?

### Checkpoint
- [ ] You can read EXPLAIN ANALYZE output (node types, costs, actual times, rows)
- [ ] You understand when Postgres chooses Seq Scan vs Index Scan
- [ ] You can design composite indexes for multi-column filter queries
- [ ] You understand join strategies (Hash Join vs Nested Loop vs Merge Join)

---

## Exercise 5: Views for Analytics

**Goal:** Create views that encapsulate business logic. In a real platform, downstream
teams query views — not raw tables.

### 5a: Daily Trip Summary View

Create a view `v_daily_trip_summary` that shows, for each day:
- Total trips (yellow + green + FHV)
- Total revenue (yellow + green only — FHV doesn't have the same fare breakdown)
- Average fare amount
- Average tip percentage (credit card trips only)
- Average trip distance

This is the view that feeds the executive dashboard.

### 5b: Zone Performance View

Create a view `v_zone_performance` that shows, for each taxi zone:
- Borough
- Zone name
- Total pickups and dropoffs
- Average fare and tip
- Most common payment type
- Average trip distance from that zone

### 5c: Hourly Demand View

Create a view `v_hourly_demand` that shows, for each hour of each day:
- Trip count
- Average fare
- Whether it's a weekday or weekend
- Whether it's rush hour (7-9 AM or 4-7 PM on weekdays)

This view would feed a real-time demand forecasting model.

### 5d: Data Quality View

Create a view `v_data_quality_issues` that flags problematic records:
- Trips where `fare_amount < 0`
- Trips where `trip_distance = 0` and `fare_amount > 10`
- Trips where `passenger_count` is null or 0
- Trips where pickup datetime > dropoff datetime
- Trips with `total_amount > 500` (outliers)
- Trips with `rate_code_id = 99` (unknown)

This is the view a data quality team would monitor.

### Checkpoint
- [ ] Your views produce correct results (spot-check against raw queries)
- [ ] You understand the difference between views and materialized views
- [ ] You could explain to a stakeholder what each view shows

---

## Bonus: Think Like a Data Engineer

After completing all exercises, reflect on these questions:

1. **Schema evolution:** The TLC added `airport_fee` in 2019 and `congestion_surcharge`
   earlier. How do you handle Parquet files from different years with different schemas?

2. **Data quality:** The real data has negative fares, null passengers, and impossible
   speeds. In production, where should cleaning happen — in the ingestion script, in
   the database (CHECK constraints), or in a transformation layer (dbt)? What are the tradeoffs?

3. **Scale:** Yellow taxi alone is ~3M rows/month. At that volume, what changes would
   you make? (Hint: table partitioning, parallel COPY, connection pooling.)

4. **Observability:** How would you monitor this database in production? What metrics
   would you alert on? (Row count drops, load latency, query performance degradation.)
