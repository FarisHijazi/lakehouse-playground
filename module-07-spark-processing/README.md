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

## RDD vs DataFrame vs Dataset

Spark has evolved through three APIs. Understanding the differences matters because you will
encounter all three in production code.

### RDD (Resilient Distributed Dataset)

The original Spark abstraction. An RDD is an immutable, distributed collection of objects.
You operate on it with functional transformations: `map`, `filter`, `reduce`, `flatMap`.

```python
rdd = sc.textFile("users.csv")
rdd.filter(lambda line: "premium" in line).count()
```

RDDs give you full control but no optimization. Spark cannot inspect a lambda function to
optimize it. The optimizer is blind.

### DataFrame

A distributed collection of rows organized into named columns -- like a table. DataFrames use
the Catalyst optimizer, which can reorder operations, push down predicates, and generate
efficient JVM bytecode.

```python
df = spark.read.csv("users.csv", header=True, inferSchema=True)
df.filter(df.subscription_type == "premium").count()
```

DataFrames are the standard API for data engineering. Use them unless you have a specific
reason not to.

### Dataset (Scala/Java only)

A typed version of DataFrame available in Scala and Java. Not available in PySpark because
Python is dynamically typed. In PySpark, DataFrame is the primary API.

**Bottom line**: Use DataFrames. They are faster than RDDs (because of Catalyst and Tungsten
optimization), easier to read, and compatible with Spark SQL.

## Lazy Evaluation and the Query Plan

This is the single most important concept in Spark. Nothing happens until you trigger an
action.

### Transformations (Lazy)

Transformations define what you want to do but do not execute anything:

```python
filtered = df.filter(df.country == "SA")          # nothing happens
selected = filtered.select("user_id", "platform") # nothing happens
grouped = selected.groupBy("platform").count()     # nothing happens
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
df.filter(df.country == "SA").select("user_id").explain(True)
# Shows: Parsed Logical Plan -> Analyzed Logical Plan -> Optimized Logical Plan -> Physical Plan
```

## Transformations vs Actions

### Narrow Transformations

Each output partition depends on only one input partition. No data moves between partitions.
These are fast:

- `select()` -- choose columns
- `filter()` / `where()` -- filter rows
- `withColumn()` -- add or modify a column
- `drop()` -- remove a column
- `map()` (RDD) -- transform each element

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

## Spark SQL

Spark SQL lets you query DataFrames using SQL syntax. This is not a separate engine -- it uses
the same Catalyst optimizer and Tungsten execution engine as the DataFrame API.

```python
# Register a DataFrame as a temporary view
df.createOrReplaceTempView("users")

# Query with SQL
result = spark.sql("""
    SELECT country, subscription_type, COUNT(*) as user_count
    FROM users
    WHERE age > 25
    GROUP BY country, subscription_type
    ORDER BY user_count DESC
""")
result.show()
```

When to use SQL vs DataFrame API:

- **SQL**: Complex queries with CTEs, subqueries, window functions -- often more readable
- **DataFrame API**: Programmatic logic, conditional transformations, reusable pipelines
- **Performance**: Identical. Both go through the same optimizer.

## Partitioning and Bucketing

### Partitioning (on disk)

Writing data partitioned by a column creates a directory structure:

```python
df.write.partitionBy("country").parquet("output/users/")
```

Creates:
```
output/users/
  country=SA/
    part-00000.parquet
  country=AE/
    part-00000.parquet
  country=KW/
    part-00000.parquet
```

When you later filter by `country = 'SA'`, Spark reads only the `country=SA/` directory.
This is called **partition pruning** and it can reduce I/O by orders of magnitude.

**Rules of thumb:**
- Partition by columns you frequently filter on (date, country, category)
- Avoid high-cardinality columns (user_id with millions of values = millions of tiny files)
- Aim for partition files between 128 MB and 1 GB

### Bucketing (in-memory organization)

Bucketing pre-sorts data into a fixed number of buckets by a column's hash value. This avoids
shuffles during joins on that column:

```python
df.write.bucketBy(16, "user_id").sortBy("user_id").saveAsTable("users_bucketed")
```

If both sides of a join are bucketed by the same column with the same number of buckets, Spark
performs a **bucket join** with zero shuffle.

## Broadcast Joins vs Sort-Merge Joins

### Sort-Merge Join (default for large-large)

Both DataFrames are shuffled so matching keys end up on the same partition, then sorted and
merged. This works for any size data but requires a full shuffle of both sides.

### Broadcast Join (small-large)

The smaller DataFrame is copied to every executor. No shuffle required for the larger
DataFrame. Dramatically faster when one side is small.

```python
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_df), "join_key")
```

Spark automatically broadcasts DataFrames smaller than `spark.sql.autoBroadcastJoinThreshold`
(default: 10 MB). You can force it with the `broadcast()` hint.

**When to use each:**
- Table < 10 MB: Auto-broadcast (default behavior)
- Table 10 MB - 1 GB: Consider explicit `broadcast()` if memory allows
- Both tables large: Sort-merge join (no choice)
- Frequent joins on same key: Bucketing to avoid repeated shuffles

## Caching and Persistence

When you reuse a DataFrame multiple times, cache it to avoid recomputation:

```python
df_cached = df.filter(df.country == "SA").cache()

# First action materializes the cache
df_cached.count()

# Subsequent actions read from cache (fast)
df_cached.groupBy("platform").count().show()
df_cached.select("user_id").distinct().count()

# Release memory when done
df_cached.unpersist()
```

### Storage Levels

| Level | Memory | Disk | Serialized |
|-------|--------|------|-----------|
| `MEMORY_ONLY` | Yes | No | No |
| `MEMORY_AND_DISK` | Yes | Spill to disk | No |
| `MEMORY_ONLY_SER` | Yes | No | Yes (smaller) |
| `DISK_ONLY` | No | Yes | Yes |

`.cache()` is shorthand for `.persist(StorageLevel.MEMORY_AND_DISK)`.

**When to cache:**
- DataFrame is reused multiple times
- DataFrame is expensive to compute (complex joins, aggregations)
- DataFrame fits in memory

**When not to cache:**
- DataFrame is used only once
- DataFrame is too large for memory (causes spill and GC pressure)
- The computation is simple (re-reading Parquet is fast)

## Spark on Databricks vs Standalone

### Standalone (local mode)

What we use in this module:

```python
spark = SparkSession.builder.master("local[*]").appName("PodcastAnalytics").getOrCreate()
```

- Runs in a single JVM on your machine
- Good for development, testing, and small datasets
- No cluster management, no YARN, no Kubernetes
- Limited by your machine's RAM and CPU

### Databricks

A managed Spark platform that handles cluster provisioning, auto-scaling, job scheduling, and
notebook collaboration:

- **Clusters**: Spin up and down on demand
- **Notebooks**: Interactive development with inline visualization
- **Unity Catalog**: Centralized governance for tables and permissions
- **Delta Lake**: Deeply integrated as the default storage format
- **Photon**: C++ execution engine that accelerates Spark SQL
- **Auto-scaling**: Adds/removes executors based on workload

The PySpark code you write locally works on Databricks with minimal changes -- mainly replacing
file paths with cloud storage paths (S3, ADLS, GCS) or catalog references.

## Delta Lake Integration with Spark

Delta Lake adds ACID transactions, schema enforcement, and time travel to Parquet files. It is
the storage layer of the lakehouse architecture.

### Writing Delta Tables

```python
df.write.format("delta").mode("overwrite").save("/path/to/delta/table")
```

### MERGE (Upsert)

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/delta/table")
delta_table.alias("target").merge(
    new_data.alias("source"),
    "target.user_id = source.user_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Time Travel

```python
# Read a previous version
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/path/to/delta/table")

# Read as of a timestamp
df_old = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("/path/to/delta/table")
```

### VACUUM

```python
delta_table = DeltaTable.forPath(spark, "/path/to/delta/table")
delta_table.vacuum(168)  # Delete files older than 168 hours (7 days)
```

## Performance Tuning: Shuffle, Skew, Spill

### Shuffle

Shuffles move data across partitions. Every `groupBy`, `join`, `orderBy`, and `distinct`
triggers a shuffle. To minimize shuffle cost:

1. **Reduce data before shuffling**: Filter and select columns early
2. **Use broadcast joins**: Eliminate shuffle for the large side
3. **Tune partition count**: `spark.sql.shuffle.partitions` (default 200, often too high for
   small data, too low for large data)
4. **Coalesce instead of repartition**: `coalesce(n)` reduces partitions without a full shuffle

### Data Skew

Skew occurs when some partitions have vastly more data than others. One task takes 10 minutes
while 199 tasks finish in 10 seconds. Solutions:

1. **Salt the key**: Add a random prefix to the skewed key, join, then remove
2. **Broadcast the smaller side**: Avoid shuffle entirely
3. **AQE (Adaptive Query Execution)**: Spark 3.x can detect skew at runtime and split large
   partitions (`spark.sql.adaptive.enabled = true`)
4. **Filter out the skewed values**: Handle them separately

### Spill

Spill occurs when a partition does not fit in memory and must be written to disk temporarily.
Signs of spill:

- Slow tasks in the Spark UI showing "Spill (Memory)" and "Spill (Disk)"
- High GC (garbage collection) time

Solutions:
1. Increase executor memory: `spark.executor.memory`
2. Increase partitions: More partitions = smaller partitions = less memory per partition
3. Reduce data size: Filter earlier, select fewer columns
4. Avoid `collect()` and `toPandas()` on large DataFrames

## What You Will Build

In this module, you will use PySpark to process the podcast platform dataset:

- **10 podcasts** with Arabic and English metadata
- **784 episodes** across multiple seasons
- **5,000 users** with messy data (inconsistent date formats, missing values, mixed gender labels)
- **~200,000 listening events** in daily JSONL files
- **50,000 CDN log entries** with network performance data
- **Ad events** with revenue and engagement data

You will start with basic DataFrame operations and progressively build a full
Bronze-Silver-Gold medallion pipeline in PySpark, learning optimization techniques along the
way.

## Prerequisites

- Python 3.9+
- PySpark: `pip install pyspark`
- Delta Lake: `pip install delta-spark`
- Java 11 or 17 (required by Spark's JVM)

Verify your installation:

```bash
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[*]').getOrCreate(); print(spark.version)"
```
