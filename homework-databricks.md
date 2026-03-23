# Databricks Homework: NYC Taxi Lakehouse

**Dataset:** NYC TLC Trip Record Data (same data from this repo)
**Platform:** Databricks Community Edition (free) or any Databricks workspace
**Time estimate:** 6-8 hours across all parts

---

## Prerequisites

1. Sign up at [Databricks Community Edition](https://community.cloud.databricks.com/login.html) (free)
2. Clone or download this repo so you have the files in `data/raw/`
3. Basic Python and SQL knowledge

---

## Part 1: Workspace Setup & Data Upload

### Step 1.1 — Create a cluster

1. Go to **Compute** in the left sidebar
2. Click **Create Cluster**
3. Name it `taxi-lakehouse`
4. Select the latest Databricks Runtime (13.x+ LTS recommended)
5. Leave defaults for Community Edition (single node)
6. Click **Create Cluster** and wait for it to start (~5 min)

### Step 1.2 — Upload raw data to DBFS

1. Go to **Data** in the left sidebar
2. Click **Create Table** → **Upload File**
3. Upload these files one at a time from `data/raw/`:
   - `yellow_tripdata_2023-01.parquet`
   - `yellow_tripdata_2023-02.parquet`
   - `yellow_tripdata_2023-03.parquet`
   - `green_tripdata_2023-01.parquet`
   - `taxi_zone_lookup.csv`
   - `vendors.csv`
   - `rate_codes.csv`
   - `payment_types.csv`
   - `nyc_weather_2023.csv`
4. Note the DBFS paths (typically `/FileStore/tables/<filename>`)

> **Alternative:** Use the Databricks CLI to upload all at once:
> ```bash
> pip install databricks-cli
> databricks configure --token
> # Enter your workspace URL and personal access token
> databricks fs mkdirs dbfs:/FileStore/taxi_raw/
> databricks fs cp data/raw/ dbfs:/FileStore/taxi_raw/ --recursive
> ```

### Step 1.3 — Create your first notebook

1. Go to **Workspace** → **Users** → your email
2. Right-click → **Create** → **Notebook**
3. Name it `00_setup_verification`
4. Set language to **Python**
5. Attach it to your `taxi-lakehouse` cluster

Paste and run this cell to verify your data is accessible:

```python
# Verify uploaded files
files = dbutils.fs.ls("/FileStore/taxi_raw/")
for f in files:
    print(f"{f.name:45s} {f.size:>12,} bytes")
```

**Checkpoint:** You should see all 9 files listed with their sizes.

---

## Part 2: Explore the Data (Notebook: `01_exploration`)

Create a new notebook called `01_exploration`.

### Step 2.1 — Load and inspect yellow taxi data

```python
yellow = spark.read.parquet("/FileStore/taxi_raw/yellow_tripdata_2023-01.parquet")
```

**Tasks:**
1. Print the schema with `yellow.printSchema()` — write down every column name and type
2. Count total rows: `yellow.count()`
3. Show the first 10 rows: `yellow.show(10, truncate=False)`
4. Run `yellow.describe().show()` — note the min/max values for `fare_amount`, `trip_distance`, `passenger_count`

### Step 2.2 — Find data quality issues

Run each of these and record the counts:

```python
from pyspark.sql.functions import col

# How many rows have null passenger count?
yellow.filter(col("passenger_count").isNull()).count()

# How many rows have fare_amount <= 0?
yellow.filter(col("fare_amount") <= 0).count()

# How many rows have trip_distance = 0?
yellow.filter(col("trip_distance") == 0).count()

# What are the distinct RatecodeID values?
yellow.select("RatecodeID").distinct().show()
```

### Step 2.3 — Explore dimension tables

```python
zones = spark.read.option("header", True).csv("/FileStore/taxi_raw/taxi_zone_lookup.csv")
zones.show(10)

vendors = spark.read.option("header", True).csv("/FileStore/taxi_raw/vendors.csv")
vendors.show()

weather = spark.read.option("header", True).csv("/FileStore/taxi_raw/nyc_weather_2023.csv")
weather.printSchema()
weather.show(5)
```

**Questions to answer (write answers in a markdown cell):**
1. How many taxi zones are there?
2. What boroughs are represented?
3. Which zone has LocationID = 132? (You'll see it a lot)
4. What weather columns are available?

---

## Part 3: Build Bronze Layer (Notebook: `02_bronze`)

Create notebook `02_bronze`. You'll ingest raw data into Delta tables with NO transformations — just add metadata.

### Step 3.1 — Create the database

```python
spark.sql("CREATE DATABASE IF NOT EXISTS taxi_lakehouse")
spark.sql("USE taxi_lakehouse")
```

### Step 3.2 — Ingest yellow trips to Bronze

```python
from pyspark.sql.functions import current_timestamp, lit, input_file_name

yellow_raw = (
    spark.read.parquet("/FileStore/taxi_raw/yellow_tripdata_2023-*.parquet")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_taxi_type", lit("yellow"))
)

(yellow_raw.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("taxi_lakehouse.bronze_trips")
)

print(f"Bronze yellow trips: {spark.table('taxi_lakehouse.bronze_trips').count():,}")
```

### Step 3.3 — Append green trips to Bronze

```python
green_raw = (
    spark.read.parquet("/FileStore/taxi_raw/green_tripdata_2023-01.parquet")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_taxi_type", lit("green"))
)

# Use mergeSchema because green has different columns (trip_type, ehail_fee)
(green_raw.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("taxi_lakehouse.bronze_trips")
)

print(f"Bronze total trips: {spark.table('taxi_lakehouse.bronze_trips').count():,}")
```

### Step 3.4 — Ingest dimension tables to Bronze

```python
# Zones
(spark.read.option("header", True).option("inferSchema", True)
    .csv("/FileStore/taxi_raw/taxi_zone_lookup.csv")
    .withColumn("_ingested_at", current_timestamp())
    .write.format("delta").mode("overwrite")
    .saveAsTable("taxi_lakehouse.bronze_zones")
)

# Weather
(spark.read.option("header", True).option("inferSchema", True)
    .csv("/FileStore/taxi_raw/nyc_weather_2023.csv")
    .withColumn("_ingested_at", current_timestamp())
    .write.format("delta").mode("overwrite")
    .saveAsTable("taxi_lakehouse.bronze_weather")
)

# Payment types
(spark.read.option("header", True).option("inferSchema", True)
    .csv("/FileStore/taxi_raw/payment_types.csv")
    .withColumn("_ingested_at", current_timestamp())
    .write.format("delta").mode("overwrite")
    .saveAsTable("taxi_lakehouse.bronze_payment_types")
)

# Rate codes
(spark.read.option("header", True).option("inferSchema", True)
    .csv("/FileStore/taxi_raw/rate_codes.csv")
    .withColumn("_ingested_at", current_timestamp())
    .write.format("delta").mode("overwrite")
    .saveAsTable("taxi_lakehouse.bronze_rate_codes")
)
```

### Step 3.5 — Verify

```sql
-- Run this in a SQL cell (change cell language to SQL with %sql)
%sql
SHOW TABLES IN taxi_lakehouse;
```

**Checkpoint:** You should see 6 bronze tables.

---

## Part 4: Build Silver Layer (Notebook: `03_silver`)

Silver = cleaned, deduplicated, standardized, with computed columns.

### Step 4.1 — Clean yellow trips

```python
from pyspark.sql.functions import (
    col, when, round, unix_timestamp, to_date, hour, dayofweek
)

spark.sql("USE taxi_lakehouse")

bronze = spark.table("bronze_trips").filter(col("_taxi_type") == "yellow")

silver_yellow = (bronze
    # ---- Filter out bad records ----
    .filter(col("fare_amount") > 0)
    .filter(col("trip_distance") > 0)
    .filter(col("tpep_pickup_datetime").isNotNull())
    .filter(col("tpep_dropoff_datetime").isNotNull())

    # ---- Add computed columns ----
    .withColumn("trip_duration_min",
        round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2)
    )
    .withColumn("speed_mph",
        round(col("trip_distance") / (
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 3600
        ), 2)
    )
    .withColumn("tip_percentage",
        when(col("fare_amount") > 0, round(col("tip_amount") / col("fare_amount") * 100, 2))
        .otherwise(0)
    )
    .withColumn("pickup_date", to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    .withColumn("pickup_dow", dayofweek("tpep_pickup_datetime"))

    # ---- Filter impossible trips ----
    .filter(col("trip_duration_min").between(1, 360))  # 1 min to 6 hours
    .filter(col("speed_mph").between(0.5, 100))         # reasonable speed

    # ---- Fill nulls ----
    .fillna({"passenger_count": 1, "RatecodeID": 1, "congestion_surcharge": 0})
)

(silver_yellow.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("pickup_date")
    .saveAsTable("taxi_lakehouse.silver_trips")
)

before = bronze.count()
after = spark.table("silver_trips").count()
print(f"Bronze: {before:,}  →  Silver: {after:,}  ({before - after:,} rows filtered, {(before-after)/before*100:.1f}%)")
```

### Step 4.2 — YOUR TURN: Clean and append green trips

Write the code yourself. Requirements:
1. Read from `bronze_trips` where `_taxi_type == "green"`
2. Green trips use `lpep_pickup_datetime` / `lpep_dropoff_datetime` instead of `tpep_*`
3. Apply the same quality filters (fare > 0, distance > 0, etc.)
4. Add the same computed columns (duration, speed, tip_percentage, pickup_date, etc.)
5. Append to `silver_trips` with `mode("append")` and `mergeSchema` enabled

**Hint:** You'll need to rename the datetime columns to match yellow's schema:
```python
.withColumnRenamed("lpep_pickup_datetime", "tpep_pickup_datetime")
.withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
```

### Step 4.3 — Create silver zone dimension

```python
zones = spark.table("bronze_zones")

silver_zones = (zones
    .withColumnRenamed("LocationID", "zone_id")
    .withColumnRenamed("Borough", "borough")
    .withColumnRenamed("Zone", "zone_name")
    .withColumnRenamed("service_zone", "service_zone")
    .filter(col("borough") != "Unknown")
)

(silver_zones.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("taxi_lakehouse.silver_zones")
)
```

### Step 4.4 — Verify silver quality

```sql
%sql
-- Check: no negative fares should exist
SELECT COUNT(*) as negative_fares FROM silver_trips WHERE fare_amount <= 0;

-- Check: no null durations
SELECT COUNT(*) as null_durations FROM silver_trips WHERE trip_duration_min IS NULL;

-- Check: row counts by taxi type
SELECT _taxi_type, COUNT(*) as trips FROM silver_trips GROUP BY _taxi_type;
```

**Checkpoint:** negative_fares = 0, null_durations = 0, both taxi types present.

---

## Part 5: Build Gold Layer (Notebook: `04_gold`)

Gold = business-ready aggregations and analytics tables.

### Step 5.1 — Daily metrics

```python
from pyspark.sql.functions import (
    col, count, sum, avg, round, min, max, countDistinct, percentile_approx
)

spark.sql("USE taxi_lakehouse")
trips = spark.table("silver_trips")

daily = (trips
    .groupBy("pickup_date", "_taxi_type")
    .agg(
        count("*").alias("total_trips"),
        round(sum("fare_amount"), 2).alias("total_fare_revenue"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("trip_distance"), 2).alias("avg_distance"),
        round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
        round(avg("tip_percentage"), 2).alias("avg_tip_pct"),
        round(avg("speed_mph"), 2).alias("avg_speed_mph"),
        countDistinct("PULocationID").alias("active_pickup_zones"),
    )
    .orderBy("pickup_date", "_taxi_type")
)

(daily.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("taxi_lakehouse.gold_daily_metrics")
)

daily.show(10)
```

### Step 5.2 — YOUR TURN: Zone performance

Create `gold_zone_performance` with these columns:
- `zone_id`, `zone_name`, `borough`
- `total_pickups` — count of trips picked up from this zone
- `total_dropoffs` — count of trips dropped off at this zone
- `avg_fare`, `avg_tip_pct`, `avg_distance`
- `total_revenue`

**Requirements:**
1. Join `silver_trips` with `silver_zones` on `PULocationID = zone_id` for pickup metrics
2. Join again on `DOLocationID = zone_id` for dropoff metrics
3. Combine into one table (hint: use two separate aggregations then join them)
4. Save as Delta table `gold_zone_performance`

### Step 5.3 — YOUR TURN: Hourly patterns

Create `gold_hourly_patterns` with:
- `pickup_hour` (0-23)
- `day_type` — "weekday" or "weekend" (hint: `dayofweek` returns 1=Sunday, 7=Saturday)
- `avg_trips` — average number of trips per day for that hour
- `avg_fare`, `avg_duration_min`, `avg_tip_pct`

Save as Delta table.

### Step 5.4 — Weather impact analysis

```python
from pyspark.sql.functions import to_date

trips = spark.table("silver_trips")
weather = (spark.table("bronze_weather")
    .withColumn("date", to_date("date", "yyyy-MM-dd"))
)

weather_impact = (trips
    .groupBy("pickup_date").agg(
        count("*").alias("total_trips"),
        round(avg("fare_amount"), 2).alias("avg_fare"),
        round(avg("trip_duration_min"), 2).alias("avg_duration"),
    )
    .join(weather, trips["pickup_date"] == weather["date"], "left")
    .drop("date")
    .withColumn("temp_bucket",
        when(col("temp_avg") < 30, "Freezing (<30F)")
        .when(col("temp_avg") < 50, "Cold (30-50F)")
        .when(col("temp_avg") < 70, "Mild (50-70F)")
        .otherwise("Warm (70F+)")
    )
    .withColumn("rain_flag",
        when(col("precipitation") > 0, "Rainy").otherwise("Dry")
    )
)

(weather_impact.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("taxi_lakehouse.gold_weather_impact")
)
```

**Checkpoint:** You should now have 4 gold tables.

```sql
%sql
SHOW TABLES IN taxi_lakehouse LIKE 'gold_*';
```

---

## Part 6: Delta Lake Features (Notebook: `05_delta_features`)

### Step 6.1 — Time travel

```sql
%sql
-- See the history of changes to a table
DESCRIBE HISTORY taxi_lakehouse.silver_trips;
```

```python
# Read a previous version
v0 = spark.read.format("delta").option("versionAsOf", 0).table("taxi_lakehouse.silver_trips")
print(f"Version 0 count: {v0.count():,}")

current = spark.table("silver_trips")
print(f"Current count:   {current.count():,}")
```

### Step 6.2 — Schema evolution

```python
# Add a new column to silver trips
from pyspark.sql.functions import lit

updated = spark.table("silver_trips").withColumn("is_airport",
    when(
        col("RatecodeID").isin(2, 3),  # JFK, Newark
        True
    ).otherwise(False)
)

(updated.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("taxi_lakehouse.silver_trips")
)

# Verify the new column
spark.table("silver_trips").filter(col("is_airport") == True).count()
```

### Step 6.3 — MERGE (Upsert) for late-arriving data

```python
from delta.tables import DeltaTable

# Simulate a late-arriving batch of corrected records
late_records = (spark.table("silver_trips")
    .limit(100)
    .withColumn("fare_amount", col("fare_amount") * 1.05)  # 5% fare correction
    .withColumn("_ingested_at", current_timestamp())
)

delta_table = DeltaTable.forName(spark, "taxi_lakehouse.silver_trips")

(delta_table.alias("target")
    .merge(
        late_records.alias("source"),
        """target.tpep_pickup_datetime = source.tpep_pickup_datetime
           AND target.tpep_dropoff_datetime = source.tpep_dropoff_datetime
           AND target.PULocationID = source.PULocationID
           AND target.DOLocationID = source.DOLocationID"""
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("MERGE complete — check DESCRIBE HISTORY for the new version")
```

### Step 6.4 — YOUR TURN: OPTIMIZE and VACUUM

1. Run `OPTIMIZE taxi_lakehouse.silver_trips ZORDER BY (pickup_date, PULocationID)` — this compacts small files and co-locates related data
2. Run `DESCRIBE HISTORY taxi_lakehouse.silver_trips` and note the new OPTIMIZE entry
3. Run `VACUUM taxi_lakehouse.silver_trips RETAIN 168 HOURS` to clean up old files
4. Try reading an old version that was before the VACUUM — what happens?

---

## Part 7: SQL Analytics (Notebook: `06_sql_analytics`)

Switch this entire notebook to **SQL** (set default language to SQL at the top).

### Step 7.1 — Top 10 busiest pickup zones

```sql
SELECT
    z.zone_name,
    z.borough,
    COUNT(*) as total_trips,
    ROUND(AVG(t.fare_amount), 2) as avg_fare,
    ROUND(SUM(t.total_amount), 2) as total_revenue
FROM taxi_lakehouse.silver_trips t
JOIN taxi_lakehouse.silver_zones z ON t.PULocationID = z.zone_id
GROUP BY z.zone_name, z.borough
ORDER BY total_trips DESC
LIMIT 10;
```

### Step 7.2 — YOUR TURN: Write these queries

**Query A:** Find the top 5 most profitable routes (pickup zone → dropoff zone pairs) by total revenue. Show pickup_zone, dropoff_zone, trip_count, total_revenue.

**Query B:** Calculate the average tip percentage by payment type. Join with `bronze_payment_types` to get human-readable names. Which payment type tips the most?

**Query C:** Find zones where the average speed is below 10 mph (congested areas). Show zone_name, borough, avg_speed, total_trips.

**Query D:** Write a query using a **window function** to rank each zone within its borough by total revenue. Show borough, zone_name, total_revenue, rank_in_borough.

**Query E:** Compare weekday vs weekend metrics: avg fare, avg distance, avg tip percentage, avg trip count per day.

---

## Part 8: Build a Simple Dashboard (Notebook: `07_dashboard`)

Databricks notebooks can display charts inline.

### Step 8.1 — Daily trip volume trend

```python
spark.sql("USE taxi_lakehouse")

daily = spark.sql("""
    SELECT pickup_date, _taxi_type, total_trips
    FROM gold_daily_metrics
    ORDER BY pickup_date
""")

display(daily)
```

After running, click the **chart icon** (bar chart) below the output table, then:
1. Chart type: **Line**
2. X-axis: `pickup_date`
3. Y-axis: `total_trips`
4. Group by: `_taxi_type`

### Step 8.2 — YOUR TURN: Create these visualizations

**Chart A:** Hourly trip distribution — bar chart of `pickup_hour` vs avg trips, colored by `day_type` (weekday/weekend)

**Chart B:** Borough revenue pie chart — total revenue by borough from `gold_zone_performance`

**Chart C:** Weather impact scatter plot — `temp_avg` on X-axis, `total_trips` on Y-axis, from `gold_weather_impact`

**Chart D:** Fare distribution histogram — create 20 buckets of fare_amount from silver_trips (hint: use `FLOOR(fare_amount / 5) * 5` to bucket fares into $5 increments)

---

## Part 9: Automate with Databricks Workflows (Optional)

### Step 9.1 — Create a multi-task job

1. Go to **Workflows** → **Create Job**
2. Name it `taxi_lakehouse_pipeline`
3. Add tasks in this order (each task runs a notebook):

| Task Name | Notebook | Depends On |
|-----------|----------|------------|
| `bronze_ingest` | `02_bronze` | — |
| `silver_clean` | `03_silver` | `bronze_ingest` |
| `gold_aggregate` | `04_gold` | `silver_clean` |

4. Set each task to use the `taxi-lakehouse` cluster
5. Click **Run now** and watch the DAG execute

### Step 9.2 — Add a schedule

1. Click the **Schedule** button (top right of the job)
2. Set it to run **daily at 6:00 AM**
3. Add an email notification for failures
4. Save (but pause it — no need to actually run daily on Community Edition)

---

## Part 10: Final Challenge — End-to-End Pipeline

Create one final notebook `08_final_challenge` that does everything from scratch in a single runnable pipeline.

### Requirements:

1. **Drop and recreate** the `taxi_lakehouse` database
2. **Bronze:** Ingest all raw data (trips + dimensions) as Delta tables with metadata columns
3. **Silver:** Clean trips (both yellow and green), add computed columns, partition by `pickup_date`
4. **Gold:** Create all 4 gold tables (daily_metrics, zone_performance, hourly_patterns, weather_impact)
5. **Quality checks:** After each layer, add assertions:
   ```python
   assert spark.table("silver_trips").filter(col("fare_amount") <= 0).count() == 0, "Silver has negative fares!"
   ```
6. **Print a final summary:**
   ```
   === Pipeline Complete ===
   Bronze trips: X,XXX,XXX
   Silver trips: X,XXX,XXX (Y.Y% pass rate)
   Gold tables:  4
   Total zones:  XXX
   Date range:   2023-01-01 to 2023-03-31
   ```

### Bonus challenges:

- [ ] Add a `gold_anomalies` table that flags days where trip volume is >2 standard deviations from the mean
- [ ] Implement SCD Type 2 on the zones dimension (simulate a zone name change using MERGE)
- [ ] Add data lineage columns (`_source_table`, `_transform_timestamp`) to every silver and gold table
- [ ] Create a `gold_route_matrix` table: for every (pickup_zone, dropoff_zone) pair, calculate trip count, avg fare, avg duration — only for pairs with > 100 trips

---

## Grading Rubric

| Section | Points | What to check |
|---------|--------|---------------|
| Part 1-2: Setup & Exploration | 10 | Data uploaded, quality issues documented |
| Part 3: Bronze | 15 | All tables created, metadata columns present, no transformations |
| Part 4: Silver | 20 | Quality filters applied, computed columns correct, green trips appended |
| Part 5: Gold | 20 | All 4 gold tables created, zone_performance and hourly_patterns done independently |
| Part 6: Delta Features | 10 | Time travel works, MERGE demonstrated, OPTIMIZE/VACUUM run |
| Part 7: SQL Analytics | 10 | All 5 queries written and produce correct results |
| Part 8: Dashboard | 10 | At least 3 charts created with correct axes/groupings |
| Part 10: Final Challenge | 5 | End-to-end notebook runs without errors |
| **Total** | **100** | |

---

## Tips

- **Use `display()` instead of `show()`** — it renders nice tables with sorting and charting built in
- **Use `%sql` magic** to switch a cell to SQL mode within a Python notebook
- **If your cluster dies**, Community Edition clusters auto-terminate after 2 hours of inactivity. Just restart it — your Delta tables persist in DBFS
- **If writes are slow**, use `.coalesce(1)` for small dimension tables to avoid tiny file overhead
- **Read the Delta Lake docs:** https://docs.delta.io/latest/index.html
- **Databricks keyboard shortcuts:** `Shift+Enter` = run cell, `Ctrl+Shift+P` = command palette
