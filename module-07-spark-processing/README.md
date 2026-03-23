# Module 07: Spark Processing

## Why This Module Exists

At some point, your data outgrows a single machine. A 200-row CSV opens in Excel. A
200,000-row CSV opens in pandas. A 200-million-row dataset does not open in either. Apache
Spark exists for this inflection point -- when the data is too large, too fast, or too complex
for single-node tools to handle within reasonable time and memory constraints.

But Spark is not just "pandas for big data." It is a distributed computing framework with its
own execution model, its own optimizer, and its own failure modes. Misunderstanding any of
these leads to jobs that are slower than pandas, not faster. This module teaches you how Spark
actually works, so you can use it effectively.

## What is Apache Spark

Spark is a unified analytics engine for large-scale data processing. It was created at UC
Berkeley's AMPLab in 2009 and donated to the Apache Software Foundation in 2013. It replaced
MapReduce as the dominant big data processing framework because it keeps intermediate data in
memory instead of writing it to disk between every step.

Key characteristics:

- **Distributed**: Work is split across multiple machines (or multiple cores on one machine)
- **In-memory**: Intermediate results stay in RAM instead of being written to HDFS
- **Lazy**: Nothing executes until you explicitly ask for a result
- **Polyglot**: APIs in Python (PySpark), Scala, Java, R, and SQL
- **Unified**: Batch processing, streaming, ML, and graph processing in one framework

In this module, we use PySpark -- the Python API for Spark. It is the most popular interface
for data engineering work and the standard in Databricks environments.

## The Dataset: NYC Taxi & Limousine Commission (TLC)

This module uses real NYC TLC trip record data, one of the most widely used public datasets
for data engineering education and benchmarking. The data includes:

- **Yellow taxi trips** (`yellow_tripdata_YYYY-MM.parquet`) -- the iconic NYC yellow cabs
- **Green taxi trips** (`green_tripdata_YYYY-MM.parquet`) -- street-hail in outer boroughs
- **For-hire vehicle trips** (`fhvhv_tripdata_YYYY-MM.parquet`) -- Uber, Lyft, etc.
- **Taxi zone lookup** (`taxi_zone_lookup.csv`) -- 265 zones mapping LocationID to borough/zone
- **Vendor codes** (`vendors.csv`) -- vendor ID to name mapping
- **Rate codes** (`rate_codes.csv`) -- rate code descriptions (standard, JFK, Newark, etc.)
- **Payment types** (`payment_types.csv`) -- cash, credit card, no charge, etc.
- **NYC weather** (`nyc_weather_2023.csv`) -- daily weather for correlation analysis

The data follows a star schema pattern common in analytics:
- **Fact table**: Trip records (millions of rows)
- **Dimension tables**: Zones, vendors, rate codes, payment types (small lookup tables)

## Spark Architecture

Understanding the architecture is not optional. Every performance problem you encounter in
Spark traces back to how work is distributed across these components.

### Driver

The driver is the process that runs your main program. It:

- Converts your PySpark code into a logical plan
- Optimizes the logical plan into a physical plan (via Catalyst optimizer)
- Splits the physical plan into stages and tasks
- Sends tasks to executors
- Collects results back

When you call `spark = SparkSession.builder.master("local[*]").getOrCreate()`, the driver
runs on your local machine and uses all available CPU cores as executors.

### Executors

Executors are JVM processes that run on worker nodes. Each executor:

- Receives tasks from the driver
- Executes the tasks on partitions of data
- Reports results back to the driver
- Caches data in memory when instructed

In `local[*]` mode, the executors are threads within the same JVM as the driver. In a cluster,
they are separate processes on separate machines.

### Partitions

Data in Spark is split into partitions. Each partition is a chunk of rows that one task
processes independently. This is the fundamental unit of parallelism:

- More partitions = more parallelism (up to the number of cores)
- Too few partitions = some cores sit idle
- Too many partitions = overhead from scheduling exceeds computation time
- Default partition count for shuffles: `spark.sql.shuffle.partitions` = 200

```
+------------------+     +------------------+     +------------------+
|   Partition 0    |     |   Partition 1    |     |   Partition 2    |
|  rows 0-999     |     |  rows 1000-1999  |     |  rows 2000-2999  |
|  (processed by   |     |  (processed by   |     |  (processed by   |
|   Task 0)        |     |   Task 1)        |     |   Task 2)        |
+------------------+     +------------------+     +------------------+
```

## Lazy Evaluation and the Query Plan

This is the single most important concept in Spark. Nothing happens until you trigger an
action.

### Transformations (Lazy)

Transformations define what you want to do but do not execute anything:

```python
filtered = df.filter(df.PULocationID == 132)           # nothing happens
selected = filtered.select("trip_distance", "fare_amount")  # nothing happens
grouped = selected.groupBy().avg("fare_amount")         # nothing happens
```

Each transformation adds a step to the logical plan. Spark records the recipe but does not
cook anything.

### Actions (Eager)

Actions trigger execution of the entire plan:

```python
grouped.show()      # NOW Spark reads data, filters, selects, groups, and displays
grouped.count()     # executes the plan and returns a number
grouped.collect()   # executes and brings all results to the driver
grouped.write.parquet("output/")  # executes and writes to disk
```

### Why Lazy Evaluation Matters

Because Spark sees the entire plan before executing, it can optimize holistically:

- **Predicate pushdown**: Move filters as early as possible to reduce data read
- **Column pruning**: Only read columns that are actually used
- **Join reordering**: Start with the smaller table
- **Constant folding**: Pre-compute expressions that do not depend on data

You can inspect the plan with `.explain()`:

```python
df.filter(df.PULocationID == 132).select("trip_distance").explain(True)
```

## Transformations vs Actions

### Narrow Transformations

Each output partition depends on only one input partition. No data moves between partitions.
These are fast:

- `select()` -- choose columns
- `filter()` / `where()` -- filter rows
- `withColumn()` -- add or modify a column
- `drop()` -- remove a column

### Wide Transformations

Output partitions depend on multiple input partitions. Data must move across the network
(shuffle). These are expensive:

- `groupBy()` -- group rows by key
- `join()` -- combine two DataFrames
- `orderBy()` / `sort()` -- global sort
- `repartition()` -- redistribute data
- `distinct()` -- remove duplicates

Every wide transformation creates a **shuffle boundary** -- a stage break where data is
serialized, sent over the network, and deserialized. Shuffles are the primary bottleneck in
Spark jobs.

### Common Actions

| Action | What it does |
|--------|-------------|
| `show(n)` | Display first n rows |
| `count()` | Return number of rows |
| `collect()` | Return all rows to driver (dangerous for large data) |
| `take(n)` | Return first n rows to driver |
| `first()` | Return first row |
| `write` | Write to storage |
| `foreach()` | Apply function to each row |

## Broadcast Joins vs Sort-Merge Joins

### Sort-Merge Join (default for large-large)

Both DataFrames are shuffled so matching keys end up on the same partition, then sorted and
merged. This works for any size data but requires a full shuffle of both sides.

### Broadcast Join (small-large)

The smaller DataFrame is copied to every executor. No shuffle required for the larger
DataFrame. Dramatically faster when one side is small.

```python
from pyspark.sql.functions import broadcast

# Broadcast the small zone lookup table when joining with millions of trips
result = trips_df.join(broadcast(zones_df), trips_df.PULocationID == zones_df.LocationID)
```

Spark automatically broadcasts DataFrames smaller than `spark.sql.autoBroadcastJoinThreshold`
(default: 10 MB). You can force it with the `broadcast()` hint.

**When to use each:**
- Table < 10 MB: Auto-broadcast (default behavior)
- Table 10 MB - 1 GB: Consider explicit `broadcast()` if memory allows
- Both tables large: Sort-merge join (no choice)
- Frequent joins on same key: Bucketing to avoid repeated shuffles

## Partitioning and Bucketing

### Partitioning (on disk)

Writing data partitioned by a column creates a directory structure:

```python
df.write.partitionBy("pickup_date").parquet("output/trips/")
```

Creates:
```
output/trips/
  pickup_date=2023-01-01/
    part-00000.parquet
  pickup_date=2023-01-02/
    part-00000.parquet
  ...
```

When you later filter by `pickup_date = '2023-01-15'`, Spark reads only that directory.
This is called **partition pruning** and it can reduce I/O by orders of magnitude.

**Rules of thumb:**
- Partition by columns you frequently filter on (date, borough)
- Avoid high-cardinality columns (DOLocationID with 265 values may be borderline)
- Aim for partition files between 128 MB and 1 GB

## Databricks Connection

The PySpark code in this module runs locally with `master("local[*]")`. The same logic works
on Databricks with minimal changes. Here is how the key patterns translate:

### SparkSession

On Databricks, the SparkSession is pre-configured and available as `spark`. You do not need
to create one:

```python
# Local (this module)
spark = SparkSession.builder.master("local[*]").appName("TaxiAnalytics").getOrCreate()

# Databricks -- spark is already available, just use it:
# spark  (no setup required)
```

### Reading Data

```python
# Local: read from filesystem
df = spark.read.parquet("data/raw/yellow_tripdata_2023-01.parquet")

# Databricks: read from cloud storage or Unity Catalog
df = spark.read.parquet("dbfs:/mnt/raw/yellow_tripdata_2023-01.parquet")
df = spark.read.format("delta").load("abfss://container@storage.dfs.core.windows.net/raw/trips")
df = spark.table("catalog.schema.yellow_trips")  # Unity Catalog
```

### Displaying Results

```python
# Local
df.show(10)

# Databricks -- use display() for rich formatting, charts, and pagination
display(df)
```

### File Utilities

```python
# Local: use pathlib or os
from pathlib import Path
Path("output/").mkdir(exist_ok=True)

# Databricks: use dbutils
dbutils.fs.ls("/mnt/raw/")
dbutils.fs.mkdirs("/mnt/output/")
dbutils.fs.rm("/mnt/output/old_table", recurse=True)
```

### Delta Lake

```python
# Local: requires delta-spark package and explicit configuration
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Databricks: Delta is the default format -- no configuration needed
df.write.format("delta").saveAsTable("catalog.schema.trips_silver")
```

### Databricks-Specific Features

- **Photon Engine**: C++ vectorized execution engine. Enable on cluster config for 2-8x speedup.
- **Adaptive Query Execution (AQE)**: Enabled by default on Databricks.
- **Delta Live Tables (DLT)**: Declarative pipelines for medallion architecture.
- **Unity Catalog**: Centralized governance -- `catalog.schema.table` naming.
- **Serverless SQL Warehouses**: Run SQL queries without managing clusters.

### DLT Equivalent of Medallion Pipeline

```python
# In a Databricks DLT notebook, Exercise 08 (medallion) becomes declarative:
import dlt
from pyspark.sql.functions import col

@dlt.table(comment="Raw yellow taxi trips")
def bronze_trips():
    return spark.read.parquet("/mnt/raw/yellow_tripdata_*.parquet")

@dlt.table(comment="Cleaned trips with valid fares")
@dlt.expect_or_drop("valid_fare", "fare_amount > 0")
def silver_trips():
    return (
        dlt.read("bronze_trips")
        .filter(col("trip_distance") > 0)
        .withColumn("trip_duration_minutes",
            (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60)
    )

@dlt.table(comment="Hourly revenue by borough")
def gold_hourly_revenue():
    return (
        dlt.read("silver_trips")
        .groupBy("pickup_borough", "pickup_hour")
        .agg(sum("total_amount").alias("total_revenue"))
    )
```

## What You Will Build

In this module, you will use PySpark to process NYC taxi trip data:

- **Millions of taxi trips** with pickup/dropoff locations, fares, tips, and timestamps
- **265 taxi zones** across 5 boroughs
- **Multiple trip types** -- yellow cab, green cab, and for-hire vehicles
- **Weather data** for correlation analysis
- **Dimension tables** for vendors, rate codes, and payment types

You will start with basic DataFrame operations and progressively build a full
Bronze-Silver-Gold medallion pipeline in PySpark, learning optimization techniques along the
way. Every exercise includes comments showing the Databricks equivalent.

## Prerequisites

- Python 3.9+
- PySpark: `pip install pyspark`
- Delta Lake: `pip install delta-spark`
- Java 11 or 17 (required by Spark's JVM)

Verify your installation:

```bash
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[*]').getOrCreate(); print(spark.version)"
```
