# Module 07: Spark Processing - Exercises

These exercises use PySpark to process the podcast platform dataset. Each exercise builds on
the previous one, progressing from basic operations to a full medallion pipeline.

All solutions run locally with:

```python
spark = SparkSession.builder.master("local[*]").appName("PodcastAnalytics").getOrCreate()
```

Data lives at `../../data/raw/` relative to the solutions directory.

---

## Exercise 1: Create a SparkSession and Read Raw Data

**Goal**: Set up a SparkSession and load every raw data file into DataFrames.

**Tasks**:

1. Create a SparkSession with `local[*]` master and app name `PodcastAnalytics`.
2. Read `podcasts.json` as a DataFrame. Note that it is a JSON array (use `multiLine=True`).
3. Read `episodes.json` as a DataFrame (also `multiLine=True`).
4. Read `users.csv` with header and inferred schema.
5. Read all JSONL files from `listening_events/` directory. Since these are newline-delimited
   JSON, standard `spark.read.json()` works without `multiLine`.
6. Read `cdn_logs.csv` with header and inferred schema.
7. Read `ad_events.json` with `multiLine=True`.
8. Print the count of rows in each DataFrame.

**Hints**:
- Use `pathlib.Path(__file__).parent.parent.parent / "data" / "raw"` for the data path.
- For JSONL files, pass the directory path or a glob like `str(path / "listening_events" / "*.jsonl")`.

**Expected output**: Six DataFrames with row counts matching the raw files (10 podcasts, 784
episodes, 5000 users, ~204k listening events, 50k CDN logs, ad events).

---

## Exercise 2: Explore Data with Spark

**Goal**: Use Spark's built-in methods to understand each dataset's structure and content.

**Tasks**:

1. Call `printSchema()` on every DataFrame to see column names and types.
2. Use `.show(5, truncate=False)` to see sample rows without truncation.
3. Run `.describe()` on numeric columns (e.g., `listened_seconds`, `duration_seconds`, `age`).
4. Count distinct values for key columns: `df.select("column").distinct().count()`.
5. Check for nulls: `df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])`.
6. Examine the `users` DataFrame closely -- identify messy fields (inconsistent date formats
   in `signup_date`, mixed gender labels, missing cities).

**Questions to answer**:
- How many distinct podcasts appear in the episodes table?
- How many distinct users appear in the listening events?
- What are the unique event types?
- What platforms are represented?
- How many null values exist in the `city` column of users?

---

## Exercise 3: Transformations - filter, select, withColumn, when/otherwise

**Goal**: Practice narrow transformations that do not trigger shuffles.

**Tasks**:

1. **Filter**: Get only listening events from Saudi Arabia (`country == "SA"`). Count them.
2. **Select**: From the filtered events, select only `user_id`, `episode_id`, `event_type`,
   and `listened_seconds`.
3. **withColumn**: Add a column `listened_minutes` that converts `listened_seconds` to minutes
   (divide by 60, round to 2 decimals).
4. **when/otherwise**: Categorize `listened_minutes` into engagement tiers:
   - `< 1` minute: `"bounce"`
   - `1-10` minutes: `"short"`
   - `10-30` minutes: `"medium"`
   - `> 30` minutes: `"long"`
5. **Standardize gender**: In the users DataFrame, clean the `gender` column. Map variations
   like `"male"`, `"m"`, `"M"`, `"Male"` to `"M"` and `"female"`, `"f"`, `"F"`, `"Female"`
   to `"F"`. Map anything else to `"Unknown"`.
6. **Parse dates**: The `signup_date` column in users has multiple formats (`YYYY-MM-DD`,
   `DD-MM-YYYY`, `DD/MM/YYYY`). Use `coalesce` with multiple `to_date` calls to parse them
   into a single consistent date column.

**Key concepts**: These are all narrow transformations. Call `.explain()` on one result and
observe that there is no `Exchange` (shuffle) node in the plan.

---

## Exercise 4: Aggregations - groupBy, agg, pivot, rollup, cube

**Goal**: Perform aggregations that trigger shuffles and understand the cost.

**Tasks**:

1. **groupBy + count**: Count listening events per `event_type`.
2. **groupBy + agg**: For each podcast (join with episodes first to get `podcast_id`), compute:
   - Total listening seconds
   - Average listening seconds per event
   - Count of distinct users
   - Count of distinct episodes
3. **pivot**: Create a pivot table of event counts by `platform` (rows) and `event_type`
   (columns).
4. **rollup**: Use `rollup("country", "platform")` on listening events to get subtotals and
   grand totals for event counts.
5. **cube**: Use `cube("country", "event_type")` to get all possible subtotal combinations.
6. Compare the explain plans for a simple `groupBy` vs a `rollup` vs a `cube`. Note the
   number of shuffles.

**Hints**:
- `agg` accepts multiple aggregation expressions: `agg(sum("col1"), avg("col2"), countDistinct("col3"))`.
- After `pivot`, null values appear where no data exists for that combination -- use `fillna(0)`.

---

## Exercise 5: Joins - Join Users with Listening Events

**Goal**: Practice different join types and handle null values from joins.

**Tasks**:

1. **Inner join**: Join `listening_events` with `users` on `user_id`. How many rows result?
   Are any events lost (users that exist in events but not in the users table)?
2. **Left join**: Left join `listening_events` with `users` to keep all events even if the
   user is missing. Check how many events have null user data.
3. **Join with episodes**: Join the result with `episodes` on `episode_id` to enrich events
   with episode metadata (title, podcast_id, duration_seconds).
4. **Join with podcasts**: Further join with `podcasts` on `podcast_id` to get podcast names.
5. **Anti join**: Find users who have never listened to anything. Use a left anti join of
   `users` against `listening_events`.
6. **Cross join** (careful!): Cross join the 10 podcasts with 5 distinct platforms to create
   a reference table of all podcast-platform combinations (50 rows).
7. **Broadcast join**: The podcasts DataFrame has only 10 rows. Use `broadcast()` to hint
   Spark to broadcast it during the join with episodes. Compare the explain plan with and
   without the broadcast hint.

**Key concept**: Check the explain plan. For the broadcast join, you should see
`BroadcastHashJoin` instead of `SortMergeJoin`.

---

## Exercise 6: Window Functions in Spark

**Goal**: Use window functions for ranking, running totals, and comparisons within groups.

**Tasks**:

1. **Row number**: For each user, number their listening events chronologically
   (`row_number()` over `user_id` ordered by `timestamp`).
2. **Rank**: Rank podcasts by total listening time. Use `rank()` to handle ties.
3. **Dense rank**: Rank users by number of episodes listened to (distinct episode count).
   Use `dense_rank()`.
4. **Running total**: For each user, compute a running total of `listened_seconds` ordered
   by timestamp.
5. **Lag/Lead**: For each user's events, add columns showing:
   - `prev_event_type`: the event type of their previous listening event
   - `next_episode_id`: the episode they listen to next
   - `time_since_last_event`: difference in seconds between current and previous event timestamp
6. **Percent rank**: Compute each user's percentile rank by total listening time across all
   users.
7. **Moving average**: For each podcast, compute a 7-day moving average of daily listen counts.

**Hints**:
```python
from pyspark.sql.window import Window

user_window = Window.partitionBy("user_id").orderBy("timestamp")
rank_window = Window.orderBy(desc("total_seconds"))
moving_window = Window.partitionBy("podcast_id").orderBy("date").rowsBetween(-6, 0)
```

---

## Exercise 7: UDFs (User Defined Functions)

**Goal**: Create custom transformations when built-in functions are not enough.

**Tasks**:

1. **Simple UDF**: Create a UDF that categorizes `app_version` strings (e.g., `"4.13.27"`)
   into major version groups: `"v3"`, `"v4"`, `"v5"`, etc.
2. **Multi-value UDF**: Create a UDF that takes a user's `country` code and returns the
   full region name (e.g., `"SA"` -> `"Gulf"`, `"EG"` -> `"North Africa"`,
   `"US"` -> `"International"`).
3. **Pandas UDF (Vectorized)**: Create a Pandas UDF that computes the completion percentage
   of an episode: `listened_seconds / duration_seconds * 100`. Vectorized UDFs are much
   faster than row-at-a-time UDFs.
4. **UDF for data cleaning**: Create a UDF that normalizes Arabic and English names by
   stripping extra whitespace and fixing common encoding issues.
5. Apply each UDF to the relevant DataFrame and show sample results.
6. **Performance comparison**: Time a built-in Spark expression vs an equivalent Python UDF
   on the listening events. Observe the difference. Why are built-in functions faster?

**Key concept**: UDFs are a black box to the Catalyst optimizer. They prevent predicate pushdown,
column pruning, and other optimizations. Use built-in functions whenever possible.

---

## Exercise 8: Write Optimized Parquet with Partitioning

**Goal**: Write DataFrames to Parquet format with partitioning for efficient downstream reads.

**Tasks**:

1. **Basic write**: Write the listening events DataFrame to Parquet format at
   `data/processed/listening_events_parquet/`.
2. **Partitioned write**: Write listening events partitioned by `country`:
   `df.write.partitionBy("country").parquet(path)`. Examine the directory structure.
3. **Multi-level partitioning**: Write listening events partitioned by `country` and
   `event_type`. Examine the nested directory structure.
4. **Partition pruning test**: Read back the partitioned data and filter by `country == "SA"`.
   Call `.explain()` and verify that `PartitionFilters` appears in the physical plan,
   confirming Spark only reads the relevant partition.
5. **Coalesce before writing**: The default write may create too many small files. Use
   `coalesce(4)` before writing to control the number of output files per partition.
6. **Overwrite mode**: Write the same data again with `.mode("overwrite")` and verify it
   replaces the previous output.

**Questions**:
- How many files are created without coalescing vs with `coalesce(4)`?
- What happens if you partition by a high-cardinality column like `user_id`?
  (Do not actually do this -- just reason about it.)

---

## Exercise 9: Spark SQL - Register Temp Views and Query

**Goal**: Use SQL syntax to query DataFrames by registering them as temporary views.

**Tasks**:

1. Register all DataFrames as temporary views:
   ```python
   users_df.createOrReplaceTempView("users")
   events_df.createOrReplaceTempView("listening_events")
   episodes_df.createOrReplaceTempView("episodes")
   podcasts_df.createOrReplaceTempView("podcasts")
   ```

2. **Basic query**: Select the top 10 most-listened-to episodes by total listening seconds.

3. **Join query**: Write a SQL query that joins listening events with episodes and podcasts
   to show total listening hours per podcast, ordered descending.

4. **CTE query**: Use a Common Table Expression to find users whose total listening time
   exceeds the average.

5. **Window function in SQL**: Rank users within each country by their total listening time.

6. **Subquery**: Find episodes that have never been listened to (not present in listening
   events).

7. **CASE WHEN**: Categorize users by engagement level based on their event count:
   - `< 10` events: `"inactive"`
   - `10-50`: `"casual"`
   - `50-200`: `"active"`
   - `> 200`: `"power_user"`

8. Compare a complex query written in SQL vs the equivalent DataFrame API. Verify both
   produce the same result and the same physical plan.

---

## Exercise 10: Build the Full Bronze -> Silver -> Gold Pipeline in PySpark

**Goal**: Implement the medallion architecture as a complete PySpark pipeline.

**Tasks**:

### Bronze Layer
1. Read all raw data files as-is into DataFrames.
2. Add metadata columns: `_source_file`, `_ingested_at` (current timestamp), `_batch_id`.
3. Write each DataFrame to Parquet in `data/spark_output/bronze/` without transformations.

### Silver Layer
4. Read from Bronze.
5. Clean users:
   - Standardize `gender` to `"M"`, `"F"`, or `"Unknown"`.
   - Parse all `signup_date` formats into a consistent date column.
   - Fill missing `city` values with `"Unknown"`.
   - Cast `age` to integer and filter out invalid ages (< 13 or > 120).
6. Clean listening events:
   - Cast `listened_seconds` to integer.
   - Filter out events with `listened_seconds <= 0`.
   - Parse `timestamp` to proper timestamp type.
   - Deduplicate by `event_id`.
7. Enrich events by joining with episodes (to get `podcast_id`, `duration_seconds`).
8. Write cleaned DataFrames to `data/spark_output/silver/`.

### Gold Layer
9. **Podcast performance**: Aggregate by podcast -- total listens, unique listeners, total
   hours, average completion rate.
10. **User engagement**: Aggregate by user -- total events, distinct podcasts, total listening
    hours, favorite podcast (most listened), days since last listen.
11. **Daily metrics**: Aggregate by date -- daily active users, total events, total listening
    hours, events by type.
12. Write gold tables to `data/spark_output/gold/`.

**Validation**: Print row counts and sample data at each layer to verify correctness.

---

## Exercise 11: Performance - Explain Plans, Repartition vs Coalesce

**Goal**: Understand how Spark executes queries and how to optimize them.

**Tasks**:

1. **Simple explain**: Run `.explain()` on a filter + select query. Identify the scan,
   filter, and projection nodes.
2. **Extended explain**: Run `.explain(True)` on a join + aggregation query. Trace the query
   through all four plan stages: Parsed, Analyzed, Optimized, Physical.
3. **Predicate pushdown**: Filter Parquet data by a partition column. Verify in the plan
   that the filter is pushed down to the scan (appears as `PushedFilters`).
4. **Repartition vs Coalesce**:
   - Check `df.rdd.getNumPartitions()` for the listening events DataFrame.
   - Use `repartition(8)` and check partitions again. Note that `explain()` shows an
     `Exchange` (shuffle).
   - Use `coalesce(4)` and check partitions. Note that `explain()` shows no `Exchange`.
   - Explain when to use each: `repartition` for increasing partitions or rebalancing;
     `coalesce` for decreasing partitions without shuffle.
5. **AQE (Adaptive Query Execution)**: Enable AQE and run a skewed join. Check if Spark
   automatically optimizes the partition sizes.
   ```python
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
   ```
6. **Cache effect**: Time a multi-use query with and without caching. Show the difference.

---

## Exercise 12: Delta Lake Operations (MERGE, Time Travel, VACUUM)

**Goal**: Use Delta Lake for ACID transactions, upserts, and data versioning.

**Prerequisites**: `pip install delta-spark`

**Tasks**:

1. **Setup**: Create a SparkSession with Delta Lake support:
   ```python
   builder = SparkSession.builder \
       .master("local[*]") \
       .appName("DeltaLake") \
       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
   ```

2. **Write Delta table**: Write the users DataFrame as a Delta table to
   `data/spark_output/delta/users/`.

3. **Read and query**: Read the Delta table back and verify the data.

4. **MERGE (upsert)**: Create a small DataFrame with updated user data (some existing users
   with changed fields, some new users). Use `DeltaTable.merge()` to upsert.

5. **Time travel**: After the merge, read version 0 (original) and the current version.
   Compare row counts and show changes.

6. **History**: Use `DeltaTable.forPath(spark, path).history()` to see the table's change log.

7. **Schema evolution**: Add a new column to the incoming data and write with
   `.option("mergeSchema", "true")`.

8. **VACUUM**: Run `vacuum(0)` to clean up old files. Note that you must set
   `spark.databricks.delta.retentionDurationCheck.enabled` to `false` to vacuum with 0 hours
   retention (only for testing -- never do this in production).

**Key concept**: Delta Lake turns your data lake into a lakehouse by adding the reliability
guarantees of a database to the scalability of object storage.
