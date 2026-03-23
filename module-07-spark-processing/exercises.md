# Module 07: Spark Processing - Exercises

These exercises use PySpark to process NYC Taxi & Limousine Commission (TLC) trip data. Each
exercise builds on the previous one, progressing from basic operations to a full medallion
pipeline with Delta Lake.

All solutions run locally with:

```python
spark = SparkSession.builder.master("local[*]").appName("TaxiAnalytics").getOrCreate()
```

Data lives at `../../data/raw/` relative to the solutions directory.

Each exercise includes Databricks equivalents in comments so you can translate directly to
notebook cells.

---

## Exercise 1: Spark Basics - Read and Explore NYC Taxi Data

**Goal**: Set up a SparkSession and load all NYC taxi data files into DataFrames.

**Tasks**:

1. Create a SparkSession with `local[*]` master and app name `TaxiAnalytics`.
2. Read `yellow_tripdata_*.parquet` files into a DataFrame.
3. Read `green_tripdata_*.parquet` files.
4. Read `fhvhv_tripdata_*.parquet` files.
5. Read dimension tables: `taxi_zone_lookup.csv`, `vendors.csv`, `rate_codes.csv`,
   `payment_types.csv`.
6. Read `nyc_weather_2023.csv`.
7. For each DataFrame: `printSchema()`, `show(5)`, `describe()`, `count()`.
8. Count nulls per column in the yellow trips DataFrame.

**Hints**:
- Use `pathlib.Path(__file__).parent.parent.parent / "data" / "raw"` for the data path.
- Parquet files self-describe their schema -- no `inferSchema` needed.
- CSV files need `header=True` and `inferSchema=True`.

**Expected output**: DataFrames for trips, zones, vendors, rate codes, payment types, weather.

---

## Exercise 2: Transformations - filter, select, withColumn, when/otherwise

**Goal**: Practice narrow transformations on taxi trip data.

**Tasks**:

1. **Filter**: Get only trips with `trip_distance > 0` and `fare_amount > 0`. Count how many
   rows are filtered out (data quality check).
2. **Select**: From the filtered trips, select `VendorID`, `tpep_pickup_datetime`,
   `tpep_dropoff_datetime`, `trip_distance`, `fare_amount`, `tip_amount`, `total_amount`,
   `PULocationID`, `DOLocationID`.
3. **withColumn**: Add `trip_duration_minutes` calculated from pickup and dropoff timestamps.
4. **withColumn**: Add `speed_mph` = `trip_distance / (trip_duration_minutes / 60)`.
5. **when/otherwise**: Classify trips by distance:
   - `< 1` mile: `"short"`
   - `1-5` miles: `"medium"`
   - `5-20` miles: `"long"`
   - `> 20` miles: `"extra_long"`
6. **Flag suspicious trips**: Trips with `speed_mph > 100` or (`trip_distance == 0` and
   `fare_amount > 0`) or `trip_duration_minutes < 0`.
7. **Time extraction**: Add `pickup_hour`, `pickup_day_of_week`, `pickup_month` columns.

**Key concepts**: These are all narrow transformations. Call `.explain()` and confirm there is
no `Exchange` (shuffle) in the plan.

---

## Exercise 3: Aggregations - groupBy, agg, pivot, rollup, cube

**Goal**: Perform aggregations that trigger shuffles and understand the cost.

**Tasks**:

1. **Revenue by borough**: Join trips with zones on `PULocationID`, then group by `Borough`
   to compute total revenue, average fare, and trip count.
2. **Revenue by zone and hour**: Group by `Zone` and `pickup_hour` for fine-grained analysis.
3. **Trip counts by day of week**: Group by `pickup_day_of_week` and count.
4. **Average tips by payment type**: Join with `payment_types` dimension, group by
   `payment_type_name`, and compute average `tip_amount` and average tip percentage.
5. **Pivot**: Create a pivot table of average fare by `Borough` (rows) and `pickup_hour`
   (columns).
6. **Multiple aggregations**: For each zone, compute total trips, total revenue, avg distance,
   avg duration, avg tip percentage, all in one `agg()` call.

**Hints**:
- `agg` accepts multiple expressions: `agg(sum("col1"), avg("col2"), countDistinct("col3"))`.
- Compare explain plans for `groupBy` vs `rollup` to see shuffle differences.

---

## Exercise 4: Joins - Trips with Zones, Weather, and Dimension Tables

**Goal**: Practice different join types with taxi trip data and dimension tables.

**Tasks**:

1. **Trips + pickup zones**: Join trips with `taxi_zone_lookup` on `PULocationID = LocationID`
   to get pickup borough and zone name. Rename as `pickup_borough`, `pickup_zone`.
2. **Trips + dropoff zones**: Join again for dropoff location. Rename as `dropoff_borough`,
   `dropoff_zone`.
3. **Trips + weather**: Join trips with weather data by date. Analyze how rain/snow affects
   trip volume and tips.
4. **Trips + rate codes + payment types**: Enrich trips with all dimension tables.
5. **Yellow + Green + FHV comparison**: Union the three trip types (align schemas first) and
   compare metrics.
6. **Broadcast join**: The zone lookup has 265 rows. Use `broadcast()` for the zone join and
   compare the explain plan with a regular join.
7. **Anti join**: Find zones that never appear as pickup locations.

**Key concept**: Small dimension tables (zones, vendors, rate codes) should always be broadcast.

---

## Exercise 5: Window Functions

**Goal**: Use window functions for ranking, running totals, and comparisons within groups.

**Tasks**:

1. **Running totals**: Daily revenue with a running total across the month.
2. **Rank zones**: Rank zones by total trip volume using `rank()` and `dense_rank()`.
3. **Moving average**: 7-day moving average of trip distances per zone.
4. **Lag/Lead**: Compare each day's revenue to the previous day and next day.
   Compute day-over-day revenue change.
5. **Cumulative distribution**: Use `percent_rank()` to find the fare amount at the
   50th, 75th, 90th, and 99th percentiles.
6. **Hourly patterns**: For each zone, rank hours by trip count to find peak hours.
7. **Row number for dedup**: Use `row_number()` to deduplicate trips that may have been
   recorded multiple times (same pickup time, location, and fare).

**Hints**:
```python
from pyspark.sql.window import Window

daily_window = Window.partitionBy("pickup_borough").orderBy("pickup_date")
zone_rank_window = Window.orderBy(desc("total_trips"))
moving_window = Window.partitionBy("PULocationID").orderBy("pickup_date").rowsBetween(-6, 0)
```

---

## Exercise 6: UDFs (User Defined Functions)

**Goal**: Create custom transformations when built-in functions are not enough.

**Tasks**:

1. **Fare category UDF**: Categorize `total_amount` into `"cheap"` (< $10), `"moderate"`
   ($10-30), `"expensive"` ($30-75), `"premium"` (> $75).
2. **Time of day classifier UDF**: Convert `pickup_hour` into `"early_morning"` (4-6),
   `"morning_rush"` (7-9), `"midday"` (10-15), `"evening_rush"` (16-19), `"evening"` (20-23),
   `"late_night"` (0-3).
3. **Tip percentage calculator UDF**: Compute `tip_amount / fare_amount * 100` with null and
   zero handling.
4. **Haversine distance UDF** (zone-centroid based): If zone centroids are available, compute
   approximate distance between pickup and dropoff zones.
5. **Performance comparison**: Time a built-in Spark expression vs an equivalent Python UDF
   for tip percentage. Observe the overhead.
6. Apply each UDF and show results with `.show()`.

**Key concept**: UDFs are a black box to the Catalyst optimizer. Prefer built-in functions.
On Databricks, consider Pandas UDFs for vectorized performance.

---

## Exercise 7: Spark SQL

**Goal**: Use SQL syntax to query taxi data via temporary views.

**Tasks**:

1. Register all DataFrames as temporary views (`trips`, `zones`, `weather`, `payment_types`,
   `rate_codes`).
2. **Basic query**: Top 10 busiest zones by trip count.
3. **Join query**: Revenue by borough with zone details.
4. **CTE query**: Find zones whose average fare exceeds the citywide average.
5. **Window function in SQL**: Rank boroughs by monthly revenue.
6. **Subquery**: Find zones that have trips but no corresponding weather correlation.
7. **CASE WHEN**: Categorize trips by time of day and compute metrics for each category.
8. **Create database and tables**: Use `CREATE DATABASE`, `CREATE TABLE` syntax like Databricks
   catalog. Show equivalent Databricks SQL with Unity Catalog.

**Databricks SQL equivalent**:
```sql
-- Local Spark SQL
CREATE DATABASE IF NOT EXISTS nyc_taxi;
CREATE TABLE nyc_taxi.trips AS SELECT * FROM trips;

-- Databricks Unity Catalog
CREATE CATALOG IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS analytics.nyc_taxi;
CREATE TABLE analytics.nyc_taxi.trips AS SELECT * FROM trips;
```

---

## Exercise 8: End-to-End Medallion Pipeline in Spark

**Goal**: Implement the full Bronze-Silver-Gold medallion architecture for taxi data.

**Tasks**:

### Bronze Layer
1. Read all raw parquet and CSV files as-is.
2. Add metadata columns: `_source_file`, `_ingested_at` (current timestamp), `_batch_id`.
3. Write to `data/spark_output/bronze/` without transformations.

### Silver Layer
4. Read from Bronze.
5. Clean trips:
   - Filter out trips with `trip_distance <= 0` or `fare_amount <= 0`.
   - Filter out trips with impossible speeds (> 200 mph) or negative durations.
   - Add `trip_duration_minutes`, `speed_mph` columns.
   - Deduplicate by pickup time + location + fare.
6. Enrich trips by joining with zone lookup (pickup and dropoff).
7. Write to `data/spark_output/silver/` partitioned by pickup date.

### Gold Layer
8. **Hourly revenue by borough**: Aggregate silver trips by borough and hour.
9. **Zone performance**: Top zones by revenue, trip count, and average tip.
10. **Daily summary**: DAU (unique medallions/drivers), total trips, total revenue, avg
    trip distance.
11. Write gold tables to `data/spark_output/gold/`.
12. Show Databricks DLT equivalents in comments.

**Validation**: Print row counts and sample data at each layer.

---

## Exercise 9: Performance Tuning

**Goal**: Understand how Spark executes queries and how to optimize them.

**Tasks**:

1. **Explain plans**: Run `.explain(True)` on a filter + join + aggregation query. Trace
   through Parsed, Analyzed, Optimized, and Physical plans.
2. **Predicate pushdown**: Write trips partitioned by date, read back with a date filter,
   and verify `PartitionFilters` in the plan.
3. **Repartition vs Coalesce**: Compare partition counts, explain plans, and use cases.
4. **Broadcast join optimization**: Compare sort-merge join vs broadcast join for the
   zones lookup. Measure wall-clock time difference.
5. **Caching**: Cache a filtered DataFrame used multiple times. Measure speedup.
6. **Partition strategies**: Compare partitioning by date vs borough vs both. Discuss
   tradeoffs.
7. Add Databricks-specific notes on Photon engine and Adaptive Query Execution.

---

## Exercise 10: Delta Lake Operations

**Goal**: Use Delta Lake for ACID transactions, upserts, and data versioning on taxi data.

**Prerequisites**: `pip install delta-spark`

**Tasks**:

1. **Setup**: Create a SparkSession with Delta Lake support.
2. **Create Delta table**: Write yellow trips as a Delta table.
3. **Time travel**: After updates, read previous versions and compare.
4. **MERGE (upsert)**: Simulate late-arriving trips that need to be merged into the table.
5. **Schema evolution**: Add a new `trip_category` column via schema merge.
6. **VACUUM and OPTIMIZE**: Clean up old files. Explain Databricks Z-ordering.
7. Add Unity Catalog context in comments -- how this maps to `catalog.schema.table`.
