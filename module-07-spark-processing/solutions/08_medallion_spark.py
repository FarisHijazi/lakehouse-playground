"""
Exercise 8: Full Bronze -> Silver -> Gold Pipeline in PySpark
===============================================================
Implements the medallion architecture using Spark DataFrames on NYC taxi data.

Databricks equivalent:
    - Bronze: Auto Loader (cloudFiles) for incremental ingestion
    - Silver/Gold: Delta Live Tables (DLT) for declarative pipelines
    - Use Delta format throughout for ACID transactions
    - Unity Catalog for table governance and lineage
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    dayofweek,
    desc,
    hour,
    input_file_name,
    lit,
    round as spark_round,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when,
)


def main():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TaxiAnalytics-Medallion")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    raw_dir = PROJECT_ROOT / "data" / "raw"
    output_dir = PROJECT_ROOT / "data" / "spark_output"

    bronze_dir = output_dir / "bronze"
    silver_dir = output_dir / "silver"
    gold_dir = output_dir / "gold"

    batch_id = "batch_001"

    # ==================================================================
    # BRONZE LAYER: Ingest raw data as-is with metadata
    # ==================================================================
    print("=" * 70)
    print("BRONZE LAYER: Raw ingestion with metadata")
    print("=" * 70)

    # --- Yellow Taxi Trips ---
    yellow_raw = (
        spark.read.parquet(str(raw_dir / "yellow_tripdata_*.parquet"))
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    yellow_raw.write.mode("overwrite").parquet(str(bronze_dir / "yellow_trips"))
    print(f"Bronze yellow   : {yellow_raw.count():>10,} rows")

    # --- Green Taxi Trips ---
    green_raw = (
        spark.read.parquet(str(raw_dir / "green_tripdata_*.parquet"))
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    green_raw.write.mode("overwrite").parquet(str(bronze_dir / "green_trips"))
    print(f"Bronze green    : {green_raw.count():>10,} rows")

    # --- Taxi Zone Lookup ---
    zones_raw = (
        spark.read.csv(str(raw_dir / "taxi_zone_lookup.csv"), header=True, inferSchema=True)
        .withColumn("_source_file", lit("taxi_zone_lookup.csv"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    zones_raw.write.mode("overwrite").parquet(str(bronze_dir / "zones"))
    print(f"Bronze zones    : {zones_raw.count():>10,} rows")

    # --- Payment Types ---
    payment_raw = (
        spark.read.csv(str(raw_dir / "payment_types.csv"), header=True, inferSchema=True)
        .withColumn("_source_file", lit("payment_types.csv"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    payment_raw.write.mode("overwrite").parquet(str(bronze_dir / "payment_types"))
    print(f"Bronze payments : {payment_raw.count():>10,} rows")

    # --- Weather ---
    weather_raw = (
        spark.read.csv(str(raw_dir / "nyc_weather_2023.csv"), header=True, inferSchema=True)
        .withColumn("_source_file", lit("nyc_weather_2023.csv"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    weather_raw.write.mode("overwrite").parquet(str(bronze_dir / "weather"))
    print(f"Bronze weather  : {weather_raw.count():>10,} rows")

    # --- Rate Codes ---
    rate_codes_raw = (
        spark.read.csv(str(raw_dir / "rate_codes.csv"), header=True, inferSchema=True)
        .withColumn("_source_file", lit("rate_codes.csv"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
    )
    rate_codes_raw.write.mode("overwrite").parquet(str(bronze_dir / "rate_codes"))
    print(f"Bronze rates    : {rate_codes_raw.count():>10,} rows")
    print()

    # ==================================================================
    # SILVER LAYER: Clean, standardize, enrich
    # ==================================================================
    print("=" * 70)
    print("SILVER LAYER: Cleaning and enrichment")
    print("=" * 70)

    # --- Read from Bronze ---
    yellow_bronze = spark.read.parquet(str(bronze_dir / "yellow_trips"))
    green_bronze = spark.read.parquet(str(bronze_dir / "green_trips"))
    zones_bronze = spark.read.parquet(str(bronze_dir / "zones"))
    payment_bronze = spark.read.parquet(str(bronze_dir / "payment_types"))
    weather_bronze = spark.read.parquet(str(bronze_dir / "weather"))

    # --- Clean yellow trips ---
    yellow_silver = (
        yellow_bronze
        # Filter invalid trips
        .filter(
            (col("trip_distance") > 0)
            & (col("fare_amount") > 0)
            & (col("fare_amount") < 1000)
            & (col("passenger_count") > 0)
        )
        # Add derived columns
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            spark_round(
                (col("tpep_dropoff_datetime").cast("long")
                 - col("tpep_pickup_datetime").cast("long")) / 60.0,
                2,
            ),
        )
        .withColumn(
            "tip_pct",
            when(col("fare_amount") > 0,
                 spark_round(col("tip_amount") / col("fare_amount") * 100, 2))
            .otherwise(0),
        )
        .withColumn("taxi_type", lit("yellow"))
        # Filter unreasonable durations
        .filter(
            (col("trip_duration_minutes") > 0)
            & (col("trip_duration_minutes") < 300)
        )
        # Drop bronze metadata
        .drop("_source_file", "_ingested_at", "_batch_id")
    )

    yellow_silver.write.mode("overwrite").partitionBy("pickup_date").parquet(
        str(silver_dir / "yellow_trips")
    )
    print(f"Silver yellow   : {yellow_silver.count():>10,} rows")

    # --- Clean green trips ---
    green_silver = (
        green_bronze
        .filter(
            (col("trip_distance") > 0)
            & (col("fare_amount") > 0)
            & (col("fare_amount") < 1000)
        )
        .withColumn("pickup_date", to_date(col("lpep_pickup_datetime")))
        .withColumn("pickup_hour", hour(col("lpep_pickup_datetime")))
        .withColumn("day_of_week", dayofweek(col("lpep_pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            spark_round(
                (col("lpep_dropoff_datetime").cast("long")
                 - col("lpep_pickup_datetime").cast("long")) / 60.0,
                2,
            ),
        )
        .withColumn(
            "tip_pct",
            when(col("fare_amount") > 0,
                 spark_round(col("tip_amount") / col("fare_amount") * 100, 2))
            .otherwise(0),
        )
        .withColumn("taxi_type", lit("green"))
        .filter(
            (col("trip_duration_minutes") > 0)
            & (col("trip_duration_minutes") < 300)
        )
        .drop("_source_file", "_ingested_at", "_batch_id")
    )

    green_silver.write.mode("overwrite").partitionBy("pickup_date").parquet(
        str(silver_dir / "green_trips")
    )
    print(f"Silver green    : {green_silver.count():>10,} rows")

    # --- Clean dimension tables (passthrough with minor cleanup) ---
    zones_silver = zones_bronze.drop("_source_file", "_ingested_at", "_batch_id")
    zones_silver.write.mode("overwrite").parquet(str(silver_dir / "zones"))
    print(f"Silver zones    : {zones_silver.count():>10,} rows")

    payment_silver = payment_bronze.drop("_source_file", "_ingested_at", "_batch_id")
    payment_silver.write.mode("overwrite").parquet(str(silver_dir / "payment_types"))
    print(f"Silver payments : {payment_silver.count():>10,} rows")

    weather_silver = (
        weather_bronze
        .withColumn("weather_date", to_date(col("date")))
        .drop("date", "_source_file", "_ingested_at", "_batch_id")
    )
    weather_silver.write.mode("overwrite").parquet(str(silver_dir / "weather"))
    print(f"Silver weather  : {weather_silver.count():>10,} rows")
    print()

    # ==================================================================
    # GOLD LAYER: Business-level aggregations
    # ==================================================================
    print("=" * 70)
    print("GOLD LAYER: Business aggregations")
    print("=" * 70)

    # Read from Silver
    yellow_s = spark.read.parquet(str(silver_dir / "yellow_trips"))
    green_s = spark.read.parquet(str(silver_dir / "green_trips"))
    zones_s = spark.read.parquet(str(silver_dir / "zones"))

    # --- Gold 1: Zone Performance ---
    zone_perf = (
        yellow_s
        .groupBy("PULocationID")
        .agg(
            count("*").alias("total_trips"),
            spark_round(spark_sum("fare_amount"), 2).alias("total_fare"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("tip_pct"), 2).alias("avg_tip_pct"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        )
        .join(
            zones_s.select("LocationID", "Borough", "Zone", "service_zone"),
            col("PULocationID") == col("LocationID"),
            how="left",
        )
        .drop("LocationID")
        .orderBy(desc("total_trips"))
    )

    zone_perf.write.mode("overwrite").parquet(str(gold_dir / "zone_performance"))
    print("=== Gold: Zone Performance ===")
    zone_perf.show(15, truncate=False)

    # --- Gold 2: Daily Metrics ---
    daily_metrics = (
        yellow_s
        .groupBy("pickup_date")
        .agg(
            count("*").alias("total_trips"),
            countDistinct("PULocationID").alias("active_zones"),
            spark_round(spark_sum("fare_amount"), 2).alias("total_fare"),
            spark_round(spark_sum("tip_amount"), 2).alias("total_tips"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("tip_pct"), 2).alias("avg_tip_pct"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        )
        .orderBy("pickup_date")
    )

    daily_metrics.write.mode("overwrite").parquet(str(gold_dir / "daily_metrics"))
    print("=== Gold: Daily Metrics ===")
    daily_metrics.show(20, truncate=False)

    # --- Gold 3: Hourly Patterns ---
    hourly_patterns = (
        yellow_s
        .groupBy("pickup_hour")
        .agg(
            count("*").alias("total_trips"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance"),
            spark_round(avg("tip_pct"), 2).alias("avg_tip_pct"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        )
        .orderBy("pickup_hour")
    )

    hourly_patterns.write.mode("overwrite").parquet(str(gold_dir / "hourly_patterns"))
    print("=== Gold: Hourly Patterns ===")
    hourly_patterns.show(24, truncate=False)

    # ==================================================================
    # Validation summary
    # ==================================================================
    print("=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    for layer, tables in [
        ("Bronze", ["yellow_trips", "green_trips", "zones", "payment_types", "weather", "rate_codes"]),
        ("Silver", ["yellow_trips", "green_trips", "zones", "payment_types", "weather"]),
        ("Gold", ["zone_performance", "daily_metrics", "hourly_patterns"]),
    ]:
        layer_dir = output_dir / layer.lower()
        print(f"\n{layer}:")
        for t in tables:
            try:
                df = spark.read.parquet(str(layer_dir / t))
                print(f"  {t:25s}: {df.count():>10,} rows, {len(df.columns):>3} columns")
            except Exception as e:
                print(f"  {t:25s}: ERROR - {e}")

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
