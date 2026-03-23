# Module 06 - Medallion Architecture: Exercises

These exercises walk you through building a complete medallion architecture for NYC
taxi trip data, progressing from raw data ingestion (Bronze) through cleaning
(Silver) to business-ready aggregates (Gold).

Every exercise includes notes on how you would implement the same pipeline in
Databricks using Delta Live Tables, Unity Catalog, and Auto Loader.

**Prerequisites:**
```bash
pip install pyspark pyarrow duckdb
```

**Data Location:** All raw data is at `data/raw/` relative to the project root.

Raw data files:
- `yellow_tripdata_YYYY-MM.parquet` -- Yellow taxi trip records
- `green_tripdata_YYYY-MM.parquet` -- Green taxi trip records
- `fhvhv_tripdata_YYYY-MM.parquet` -- For-Hire Vehicle (Uber/Lyft) trip records
- `taxi_zone_lookup.csv` -- Mapping of LocationID to zone name and borough
- `vendors.csv` -- Vendor ID to name mapping
- `rate_codes.csv` -- Rate code descriptions
- `payment_types.csv` -- Payment type descriptions
- `fhv_bases.csv` -- FHV base license information
- `nyc_weather_2023.csv` -- Daily weather observations for NYC

**Output Locations:**
- Bronze: `data/bronze/`
- Silver: `data/silver/`
- Gold: `data/gold/`

**Important:** Run exercises in order. Gold depends on Silver, which depends on Bronze.

---

## Exercise 1: Build the Bronze Layer

**Goal:** Land all raw data sources into a structured Bronze layer as Parquet files,
preserving the original data exactly as-is, with added ingestion metadata.

**Databricks equivalent:** In production, you would use **Auto Loader**
(`cloudFiles` format) to incrementally ingest new parquet files as they arrive in
cloud storage. Auto Loader automatically tracks which files have been processed.

**Input files:**
- `data/raw/yellow_tripdata_*.parquet`
- `data/raw/green_tripdata_*.parquet`
- `data/raw/fhvhv_tripdata_*.parquet`
- `data/raw/taxi_zone_lookup.csv`
- `data/raw/vendors.csv`, `rate_codes.csv`, `payment_types.csv`, `fhv_bases.csv`
- `data/raw/nyc_weather_2023.csv`

**Requirements:**

1. Read each raw source using PySpark without applying any data cleaning or type
   coercion. For CSV files, read all columns as strings to avoid premature type
   inference.

2. Add ingestion metadata columns to every record:
   - `_ingested_at`: Current ISO timestamp (when the pipeline ran)
   - `_source_file`: The name of the original source file
   - `_batch_id`: A UUID identifying this ingestion batch

3. For trip data parquet files:
   - Read all matching files using glob patterns
   - Preserve the original schema exactly as-is

4. Write each dataset to `data/bronze/` as Parquet files:
   - `data/bronze/yellow_trips/`
   - `data/bronze/green_trips/`
   - `data/bronze/fhv_trips/`
   - `data/bronze/taxi_zones/`
   - `data/bronze/reference/` (vendors, rate_codes, payment_types, fhv_bases)
   - `data/bronze/weather/`

5. Print summary statistics: row counts, column names, and null counts.

**Key Principle:** Do NOT clean anything. Negative fares, null timestamps, duplicate
records -- all stay exactly as they are in the source. Bronze is a faithful archive.

**Databricks DLT equivalent:**
```python
@dlt.table(comment="Raw yellow taxi trip data")
def bronze_yellow_trips():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/mnt/raw/yellow_tripdata_*.parquet")
    )
```

---

## Exercise 2: Build the Silver Layer - Yellow Taxi Trips

**Goal:** Transform Bronze yellow taxi trips into a clean, validated, enriched
Silver table.

**Input:** `data/bronze/yellow_trips/` (Parquet)

**Output:** `data/silver/yellow_trips/`

**Requirements:**

1. **Filter nulls:** Remove trips where `tpep_pickup_datetime` or
   `tpep_dropoff_datetime` is null.

2. **Handle negative fares:** Flag or remove trips with negative `fare_amount`,
   `tip_amount`, or `total_amount`.

3. **Deduplicate:** Remove exact duplicate rows based on pickup time, dropoff time,
   pickup location, dropoff location, and fare amount.

4. **Add computed columns:**
   - `trip_duration_minutes`: Difference between dropoff and pickup times in minutes
   - `speed_mph`: `trip_distance / (trip_duration_minutes / 60)`, handling division
     by zero
   - `is_airport_trip`: True if pickup or dropoff is at JFK (LocationID 132) or
     LaGuardia (LocationID 138)
   - `is_rush_hour`: True if pickup hour is 7-9 or 17-19 on weekdays
   - `pickup_date`: Date portion of pickup datetime

5. **Validate:**
   - Trip duration must be between 0.5 and 720 minutes (12 hours)
   - Speed must be less than 100 mph
   - Quarantine invalid records to `data/silver/yellow_trips_quarantine/`

6. Print: total Bronze records, duplicates removed, records quarantined, final
   Silver count, fare statistics, airport trip percentage.

**Databricks DLT equivalent:**
```python
@dlt.table(comment="Cleaned yellow taxi trips")
@dlt.expect_or_drop("valid_pickup", "tpep_pickup_datetime IS NOT NULL")
@dlt.expect_or_drop("valid_fare", "fare_amount >= 0")
@dlt.expect_or_quarantine("reasonable_speed", "speed_mph < 100")
def silver_yellow_trips():
    return dlt.read("bronze_yellow_trips").withColumn(...)
```

---

## Exercise 3: Build the Silver Layer - Green Taxi Trips

**Goal:** Clean green taxi trips following a similar pattern to yellow, accounting
for schema differences.

**Input:** `data/bronze/green_trips/` (Parquet)

**Output:** `data/silver/green_trips/`

**Requirements:**

1. Green taxi data uses `lpep_pickup_datetime` / `lpep_dropoff_datetime` instead of
   `tpep_` prefix. Rename to common `pickup_datetime` / `dropoff_datetime`.

2. Apply the same cleaning, validation, and enrichment as yellow trips:
   - Filter null timestamps
   - Handle negative fares
   - Deduplicate
   - Add computed columns (duration, speed, airport flag, rush hour)
   - Quarantine invalid records

3. Green taxi data includes `trip_type` column (1=street-hail, 2=dispatch) that
   yellow does not have. Preserve it.

4. Print similar summary statistics as the yellow trip exercise.

---

## Exercise 4: Build the Silver Layer - FHV/Rideshare Trips

**Goal:** Clean FHV (For-Hire Vehicle) / rideshare trip data. This includes Uber
and Lyft trips, which have a different schema from taxi trips.

**Input:** `data/bronze/fhv_trips/` (Parquet)

**Output:** `data/silver/fhv_trips/`

**Requirements:**

1. FHV data has a different schema: `pickup_datetime`, `dropOff_datetime` (note
   the camelCase), `PULocationID`, `DOLocationID`, `dispatching_base_num`,
   etc. Standardize column names to snake_case.

2. FHV data does NOT have fare information. Focus on:
   - Filter null pickup/dropoff times
   - Add computed columns: `trip_duration_minutes`, `is_airport_trip`,
     `is_rush_hour`, `pickup_date`
   - Validate duration ranges
   - Deduplicate

3. Join with `fhv_bases.csv` to add base name information.

4. Print summary statistics including base distribution and duration stats.

---

## Exercise 5: Build the Gold Layer - Daily Trip Metrics

**Goal:** Create a daily aggregate table showing trip-level metrics across all
taxi types, ready for a dashboard.

**Input:**
- `data/silver/yellow_trips/`
- `data/silver/green_trips/`
- `data/silver/fhv_trips/`

**Output:** `data/gold/daily_metrics/`

**Requirements:**

1. Union all three trip types with a `taxi_type` column (`yellow`, `green`, `fhv`).

2. Group by `pickup_date` and `taxi_type`, computing:
   - `total_trips`: Count of trips
   - `total_revenue`: Sum of `total_amount` (yellow/green only; null for FHV)
   - `avg_distance`: Average `trip_distance`
   - `avg_tip`: Average `tip_amount` (yellow/green only)
   - `avg_duration_minutes`: Average `trip_duration_minutes`
   - `airport_trip_pct`: Percentage of trips to/from airports

3. Also create a rolled-up version without taxi_type breakdown.

4. Add 7-day rolling averages for total_trips and total_revenue.

5. Print: date range, peak day, revenue by taxi type, trend summary.

**Databricks equivalent:**
```sql
CREATE MATERIALIZED VIEW nyc_taxi.gold.daily_metrics AS
SELECT pickup_date, taxi_type, COUNT(*) AS total_trips, ...
FROM nyc_taxi.silver.yellow_trips
GROUP BY pickup_date, taxi_type;
```

---

## Exercise 6: Build the Gold Layer - Zone Analytics

**Goal:** Analyze trip patterns at the taxi zone level for urban planning and
business intelligence.

**Input:**
- `data/silver/yellow_trips/`
- `data/silver/green_trips/`
- `data/bronze/taxi_zones/`

**Output:** `data/gold/zone_analytics/`

**Requirements:**

1. Join trips with `taxi_zone_lookup.csv` to get zone names and boroughs.

2. Compute per-zone metrics:
   - Top pickup zones (by trip count)
   - Top dropoff zones (by trip count)
   - Revenue by borough
   - Average trip distance and duration per zone
   - Average fare per zone

3. Create an OD (origin-destination) matrix showing the most common zone pairs.

4. Print: top 10 pickup zones, top 10 dropoff zones, borough revenue breakdown.

---

## Exercise 7: Build the Gold Layer - Hourly Patterns

**Goal:** Analyze temporal patterns in taxi usage for operational optimization.

**Input:**
- `data/silver/yellow_trips/`
- `data/silver/green_trips/`

**Output:** `data/gold/hourly_patterns/`

**Requirements:**

1. Create hourly trip distribution: average trips per hour of day.

2. Compare weekday vs. weekend patterns:
   - Average trips by hour, split by weekday/weekend
   - Average fare by hour, split by weekday/weekend

3. Rush hour analysis:
   - Define morning rush (7-9 AM) and evening rush (5-7 PM)
   - Compare trip volume, average fare, average tip during rush vs. non-rush
   - Compare weekday rush vs. weekend same hours

4. Add day-of-week analysis: which day has the most trips, highest revenue,
   longest average trips.

5. Print: peak hours, weekday vs weekend comparison, rush hour premium analysis.

---

## Exercise 8: Build the Gold Layer - Weather Impact

**Goal:** Analyze how weather conditions affect taxi usage and revenue.

**Input:**
- `data/silver/yellow_trips/`
- `data/silver/green_trips/`
- `data/bronze/weather/`

**Output:** `data/gold/weather_impact/`

**Requirements:**

1. Aggregate Silver trips to daily level (total trips, total revenue, avg tip).

2. Join with daily weather data on date. Key weather fields:
   - Temperature (high/low)
   - Precipitation
   - Snow depth
   - Wind speed

3. Compute correlations and bucketed analysis:
   - Trip volume on rainy vs. dry days
   - Revenue on cold vs. warm days
   - Tip percentage on snowy vs. clear days
   - Create temperature buckets and precipitation buckets

4. Print: correlation matrix, rainy day premium, cold weather impact, snow day
   analysis.

---

## Exercise 9: Implement Schema Evolution

**Goal:** Demonstrate how the medallion architecture handles schema changes, using
the real-world example of NYC TLC adding the `airport_fee` column to yellow taxi
data starting in 2019.

**Requirements:**

1. Show the schema of an "old" yellow taxi file (without `airport_fee`).

2. Show the schema of a "new" yellow taxi file (with `airport_fee`).

3. Demonstrate how Bronze handles this with `unionByName(allowMissingColumns=True)`.

4. Show how Silver processes the evolved schema, filling nulls for the new column
   in old records.

5. Verify Gold queries remain backward compatible.

**Databricks equivalent:**
```python
# Delta Lake handles this automatically:
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("nyc_taxi.bronze.yellow_trips")
```

---

## Exercise 10: Implement MERGE / Upsert Pattern

**Goal:** Implement idempotent writes using the MERGE/upsert pattern, showing how
to handle late-arriving taxi trip records.

**Scenario:** TLC occasionally publishes corrections to trip data. Some records in
a new monthly file are updates to records from previous months (corrected fares,
amended tips). Running the pipeline twice with the same data should produce the
same result (idempotency).

**Requirements:**

1. Create an initial Silver yellow trips table using the first batch of data.

2. Generate a "correction batch" containing:
   - Some existing trips with updated fare amounts
   - Some new trips not in the original batch

3. Implement a MERGE/upsert using DuckDB SQL:
   ```sql
   MERGE INTO silver_yellow_trips AS target
   USING corrections AS source
   ON target.pickup_datetime = source.pickup_datetime
     AND target.dropoff_datetime = source.dropoff_datetime
     AND target.PULocationID = source.PULocationID
   WHEN MATCHED THEN UPDATE SET ...
   WHEN NOT MATCHED THEN INSERT ...
   ```

4. Verify idempotency: run the upsert twice and confirm row count is stable.

5. Show the audit trail: which records were inserted vs. updated.

**Databricks equivalent:**
```sql
-- In Databricks, this is native Delta Lake:
MERGE INTO nyc_taxi.silver.yellow_trips AS target
USING nyc_taxi.bronze.yellow_trips_corrections AS source
ON target.surrogate_key = source.surrogate_key
WHEN MATCHED AND source.total_amount != target.total_amount THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
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
- Bronze: 6 datasets (yellow_trips, green_trips, fhv_trips, taxi_zones, reference, weather)
- Silver: 3 cleaned trip datasets + quarantine files
- Gold: 4 aggregate tables (daily_metrics, zone_analytics, hourly_patterns, weather_impact)

Each Gold table should be directly usable for visualization in Module 10 (BI Dashboards).
