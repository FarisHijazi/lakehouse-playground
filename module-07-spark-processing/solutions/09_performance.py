"""
Exercise 9: Performance - Explain Plans, Repartition vs Coalesce
==================================================================
Understanding Spark execution and optimization techniques with NYC taxi data.

Databricks equivalent:
    - Use the Spark UI tab in Databricks for visual DAGs and stage details
    - Databricks auto-optimizes with Photon engine and adaptive execution
    - Use EXPLAIN FORMATTED in SQL for detailed plans
    - Databricks caching uses Delta caching (disk-based, faster than memory)
"""

import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, broadcast, col, count, sum as spark_sum


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TaxiAnalytics-Perf")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    data_dir = PROJECT_ROOT / "data" / "raw"
    output_dir = PROJECT_ROOT / "data" / "spark_output"

    yellow_df = spark.read.parquet(str(data_dir / "yellow_tripdata_*.parquet"))
    zones_df = spark.read.csv(
        str(data_dir / "taxi_zone_lookup.csv"), header=True, inferSchema=True
    )
    payment_types_df = spark.read.csv(
        str(data_dir / "payment_types.csv"), header=True, inferSchema=True
    )

    # ----------------------------------------------------------------
    # 1. Simple explain: filter + select
    # ----------------------------------------------------------------
    print("=" * 70)
    print("1. SIMPLE EXPLAIN: filter + select")
    print("=" * 70)

    simple_query = (
        yellow_df
        .filter((col("trip_distance") > 0) & (col("fare_amount") > 0))
        .select("PULocationID", "DOLocationID", "trip_distance", "fare_amount")
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
        yellow_df
        .join(zones_df, yellow_df.PULocationID == zones_df.LocationID, how="inner")
        .groupBy("Borough")
        .agg(
            spark_sum("fare_amount").alias("total_fare"),
            count("*").alias("total_trips"),
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

    # Write partitioned parquet first (by pickup borough via join)
    from pyspark.sql.functions import to_date

    parquet_path = str(output_dir / "perf_test" / "trips_by_date")
    (
        yellow_df
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
        .write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(parquet_path)
    )
    print(f"Wrote partitioned Parquet to: {parquet_path}\n")

    # Read with partition filter
    parquet_df = spark.read.parquet(parquet_path)
    filtered = parquet_df.filter(col("pickup_date") == "2023-01-15")

    print("Plan with partition pruning:")
    filtered.explain()
    print("Look for 'PartitionFilters' in the plan.")
    print("This means Spark skips reading non-matching date directories entirely.\n")

    # ----------------------------------------------------------------
    # 4. Repartition vs Coalesce
    # ----------------------------------------------------------------
    print("=" * 70)
    print("4. REPARTITION VS COALESCE")
    print("=" * 70)

    original_partitions = yellow_df.rdd.getNumPartitions()
    print(f"Original partitions: {original_partitions}")

    # Repartition (increases partitions, causes shuffle)
    repartitioned = yellow_df.repartition(8)
    print(f"\nAfter repartition(8): {repartitioned.rdd.getNumPartitions()} partitions")
    print("Repartition plan (note the Exchange/shuffle):")
    repartitioned.explain()

    # Coalesce (decreases partitions, NO shuffle)
    coalesced = yellow_df.coalesce(4)
    print(f"After coalesce(4): {coalesced.rdd.getNumPartitions()} partitions")
    print("Coalesce plan (note: NO Exchange):")
    coalesced.explain()

    # Repartition by column (useful for joins)
    repartitioned_by_col = yellow_df.repartition(8, "PULocationID")
    print(f"After repartition(8, 'PULocationID'): {repartitioned_by_col.rdd.getNumPartitions()} partitions")
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
        yellow_df
        .join(broadcast(zones_df), yellow_df.PULocationID == zones_df.LocationID, how="inner")
        .groupBy("Borough")
        .agg(count("*").alias("trip_count"))
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
    manhattan_trips = yellow_df.filter(col("PULocationID").isin(
        [161, 162, 163, 164, 170, 186, 234, 236, 237, 239]  # Manhattan zone IDs
    ))

    t0 = time.time()
    count1 = manhattan_trips.count()
    count2 = manhattan_trips.groupBy("PULocationID").count().collect()
    count3 = manhattan_trips.select("DOLocationID").distinct().count()
    uncached_time = time.time() - t0
    print(f"Without cache: {uncached_time:.2f}s")
    print(f"  count={count1}, pickup_zones={len(count2)}, unique_dropoffs={count3}")

    # With cache
    cached = yellow_df.filter(col("PULocationID").isin(
        [161, 162, 163, 164, 170, 186, 234, 236, 237, 239]
    )).cache()

    # First call materializes the cache
    cached.count()

    t0 = time.time()
    count1 = cached.count()
    count2 = cached.groupBy("PULocationID").count().collect()
    count3 = cached.select("DOLocationID").distinct().count()
    cached_time = time.time() - t0
    print(f"With cache   : {cached_time:.2f}s")
    print(f"  count={count1}, pickup_zones={len(count2)}, unique_dropoffs={count3}")

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
