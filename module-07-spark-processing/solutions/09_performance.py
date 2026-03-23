"""
Exercise 11: Performance - Explain Plans, Repartition vs Coalesce
==================================================================
Understanding Spark execution and optimization techniques.
"""

import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, broadcast, col, count, sum as spark_sum


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("PodcastAnalytics-Perf")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    data_dir = Path(__file__).resolve().parent.parent.parent / "data" / "raw"
    output_dir = Path(__file__).resolve().parent.parent.parent / "data" / "spark_output"

    events_df = spark.read.json(str(data_dir / "listening_events"))
    episodes_df = spark.read.json(str(data_dir / "episodes.json"), multiLine=True)
    podcasts_df = spark.read.json(str(data_dir / "podcasts.json"), multiLine=True)

    # ----------------------------------------------------------------
    # 1. Simple explain: filter + select
    # ----------------------------------------------------------------
    print("=" * 70)
    print("1. SIMPLE EXPLAIN: filter + select")
    print("=" * 70)

    simple_query = (
        events_df
        .filter(col("country") == "SA")
        .select("user_id", "episode_id", "listened_seconds")
    )

    print("Physical plan:")
    simple_query.explain()
    print("Note: FileScan -> Filter -> Project. No Exchange (no shuffle).\n")

    # ----------------------------------------------------------------
    # 2. Extended explain: join + aggregation
    # ----------------------------------------------------------------
    print("=" * 70)
    print("2. EXTENDED EXPLAIN: join + aggregation")
    print("=" * 70)

    complex_query = (
        events_df
        .join(episodes_df, on="episode_id", how="inner")
        .groupBy("podcast_id")
        .agg(
            spark_sum("listened_seconds").alias("total_seconds"),
            count("*").alias("total_events"),
        )
    )

    print("Full plan (all stages):")
    complex_query.explain(True)
    print()

    # ----------------------------------------------------------------
    # 3. Predicate pushdown on Parquet
    # ----------------------------------------------------------------
    print("=" * 70)
    print("3. PREDICATE PUSHDOWN ON PARQUET")
    print("=" * 70)

    # Write partitioned parquet first
    parquet_path = str(output_dir / "perf_test" / "events_by_country")
    (
        events_df
        .write
        .mode("overwrite")
        .partitionBy("country")
        .parquet(parquet_path)
    )
    print(f"Wrote partitioned Parquet to: {parquet_path}\n")

    # Read with partition filter
    parquet_df = spark.read.parquet(parquet_path)
    filtered = parquet_df.filter(col("country") == "SA")

    print("Plan with partition pruning:")
    filtered.explain()
    print("Look for 'PartitionFilters: [isnotnull(country), (country = SA)]'")
    print("This means Spark skips reading non-SA directories entirely.\n")

    # ----------------------------------------------------------------
    # 4. Repartition vs Coalesce
    # ----------------------------------------------------------------
    print("=" * 70)
    print("4. REPARTITION VS COALESCE")
    print("=" * 70)

    original_partitions = events_df.rdd.getNumPartitions()
    print(f"Original partitions: {original_partitions}")

    # Repartition (increases partitions, causes shuffle)
    repartitioned = events_df.repartition(8)
    print(f"\nAfter repartition(8): {repartitioned.rdd.getNumPartitions()} partitions")
    print("Repartition plan (note the Exchange/shuffle):")
    repartitioned.explain()

    # Coalesce (decreases partitions, NO shuffle)
    coalesced = events_df.coalesce(4)
    print(f"After coalesce(4): {coalesced.rdd.getNumPartitions()} partitions")
    print("Coalesce plan (note: NO Exchange):")
    coalesced.explain()

    # Repartition by column (useful for joins)
    repartitioned_by_col = events_df.repartition(8, "user_id")
    print(f"After repartition(8, 'user_id'): {repartitioned_by_col.rdd.getNumPartitions()} partitions")
    print("Repartition by column plan:")
    repartitioned_by_col.explain()

    print("\nWhen to use each:")
    print("  repartition(n)       - Increase partitions or rebalance skewed data (causes shuffle)")
    print("  repartition(n, col)  - Redistribute by column for join optimization (causes shuffle)")
    print("  coalesce(n)          - Decrease partitions without shuffle (only for reducing)")
    print()

    # ----------------------------------------------------------------
    # 5. AQE (Adaptive Query Execution)
    # ----------------------------------------------------------------
    print("=" * 70)
    print("5. ADAPTIVE QUERY EXECUTION (AQE)")
    print("=" * 70)

    print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
    print(f"AQE coalesce: {spark.conf.get('spark.sql.adaptive.coalescePartitions.enabled')}")

    # Run a join with AQE -- Spark will automatically optimize
    aqe_result = (
        events_df
        .join(broadcast(podcasts_df), how="cross")  # small table, should auto-broadcast
        .filter(col("country") == "SA")
        .groupBy("podcast_id")
        .agg(count("*").alias("event_count"))
    )

    print("\nPlan with AQE (may show AdaptiveSparkPlan):")
    aqe_result.explain()
    aqe_result.show(5)

    # ----------------------------------------------------------------
    # 6. Cache effect: multi-use query
    # ----------------------------------------------------------------
    print("=" * 70)
    print("6. CACHING EFFECT")
    print("=" * 70)

    # Without cache
    uncached = events_df.filter(col("country") == "SA")

    t0 = time.time()
    count1 = uncached.count()
    count2 = uncached.groupBy("event_type").count().collect()
    count3 = uncached.select("user_id").distinct().count()
    uncached_time = time.time() - t0
    print(f"Without cache: {uncached_time:.2f}s")
    print(f"  count={count1}, event_types={len(count2)}, unique_users={count3}")

    # With cache
    cached = events_df.filter(col("country") == "SA").cache()

    # First call materializes the cache
    cached.count()

    t0 = time.time()
    count1 = cached.count()
    count2 = cached.groupBy("event_type").count().collect()
    count3 = cached.select("user_id").distinct().count()
    cached_time = time.time() - t0
    print(f"With cache   : {cached_time:.2f}s")
    print(f"  count={count1}, event_types={len(count2)}, unique_users={count3}")

    if uncached_time > 0:
        speedup = uncached_time / max(cached_time, 0.001)
        print(f"Speedup      : {speedup:.1f}x")

    # Clean up cache
    cached.unpersist()
    print("Cache released.")

    # ----------------------------------------------------------------
    # Summary: Performance checklist
    # ----------------------------------------------------------------
    print()
    print("=" * 70)
    print("PERFORMANCE CHECKLIST")
    print("=" * 70)
    print("""
    1. Filter early          - Push filters before joins and aggregations
    2. Select only needed    - Drop unnecessary columns to reduce data size
    3. Broadcast small tables - Use broadcast() for tables < ~100 MB
    4. Partition output      - Write Parquet partitioned by filter columns
    5. Coalesce vs repartition - Use coalesce to reduce, repartition to increase
    6. Cache when reused     - Cache DataFrames accessed multiple times
    7. Tune shuffle partitions - Set spark.sql.shuffle.partitions appropriately
    8. Enable AQE            - Let Spark adapt at runtime (Spark 3+)
    9. Avoid UDFs            - Prefer built-in functions for Catalyst optimization
   10. Monitor the Spark UI  - Check for skew, spill, and shuffle sizes
    """)

    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
