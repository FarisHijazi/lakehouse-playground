"""
Exercise 1: Spark Basics - Read and Explore NYC Taxi Data
==========================================================
Sets up PySpark locally and loads all NYC TLC trip data and dimension tables.

Databricks equivalent:
    - spark is pre-configured; no SparkSession.builder needed
    - Use display(df) instead of df.show()
    - Use dbutils.fs.ls() to browse files
    - Read from cloud storage: spark.read.parquet("dbfs:/mnt/raw/yellow_tripdata_*.parquet")
    - Or read from Unity Catalog: spark.table("catalog.schema.yellow_trips")
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when


def main():
    # ----------------------------------------------------------------
    # 1. Create a SparkSession
    # ----------------------------------------------------------------
    # Databricks: spark is already available -- skip this block.
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TaxiAnalytics")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")  # sensible default for local
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version : {spark.version}")
    print(f"App name      : {spark.sparkContext.appName}")
    print(f"Master        : {spark.sparkContext.master}")
    print()

    # ----------------------------------------------------------------
    # 2. Resolve data paths
    # ----------------------------------------------------------------
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    data_dir = PROJECT_ROOT / "data" / "raw"
    print(f"Data directory: {data_dir}")
    print()

    # Databricks equivalent:
    #   data_dir = "dbfs:/mnt/raw/"
    #   dbutils.fs.ls(data_dir)

    # ----------------------------------------------------------------
    # 3. Read yellow taxi trips (parquet -- self-describing schema)
    # ----------------------------------------------------------------
    yellow_df = spark.read.parquet(str(data_dir / "yellow_tripdata_*.parquet"))
    print("=== Yellow Taxi Trips ===")
    print(f"Row count : {yellow_df.count():,}")
    print(f"Partitions: {yellow_df.rdd.getNumPartitions()}")
    yellow_df.printSchema()
    yellow_df.show(5, truncate=False)

    # Databricks: display(yellow_df) for interactive table with charts

    # ----------------------------------------------------------------
    # 4. Read green taxi trips
    # ----------------------------------------------------------------
    green_df = spark.read.parquet(str(data_dir / "green_tripdata_*.parquet"))
    print("=== Green Taxi Trips ===")
    print(f"Row count : {green_df.count():,}")
    green_df.printSchema()
    green_df.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 5. Read FHV (for-hire vehicle) trips
    # ----------------------------------------------------------------
    fhvhv_df = spark.read.parquet(str(data_dir / "fhvhv_tripdata_*.parquet"))
    print("=== FHV High-Volume Trips ===")
    print(f"Row count : {fhvhv_df.count():,}")
    fhvhv_df.printSchema()
    fhvhv_df.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 6. Read dimension tables (CSV)
    # ----------------------------------------------------------------
    zones_df = spark.read.csv(
        str(data_dir / "taxi_zone_lookup.csv"), header=True, inferSchema=True
    )
    print("=== Taxi Zone Lookup ===")
    print(f"Row count: {zones_df.count()}")
    zones_df.printSchema()
    zones_df.show(10, truncate=False)

    vendors_df = spark.read.csv(
        str(data_dir / "vendors.csv"), header=True, inferSchema=True
    )
    print("=== Vendors ===")
    print(f"Row count: {vendors_df.count()}")
    vendors_df.show(truncate=False)

    rate_codes_df = spark.read.csv(
        str(data_dir / "rate_codes.csv"), header=True, inferSchema=True
    )
    print("=== Rate Codes ===")
    print(f"Row count: {rate_codes_df.count()}")
    rate_codes_df.show(truncate=False)

    payment_types_df = spark.read.csv(
        str(data_dir / "payment_types.csv"), header=True, inferSchema=True
    )
    print("=== Payment Types ===")
    print(f"Row count: {payment_types_df.count()}")
    payment_types_df.show(truncate=False)

    # ----------------------------------------------------------------
    # 7. Read weather data
    # ----------------------------------------------------------------
    weather_df = spark.read.csv(
        str(data_dir / "nyc_weather_2023.csv"), header=True, inferSchema=True
    )
    print("=== NYC Weather 2023 ===")
    print(f"Row count: {weather_df.count()}")
    weather_df.printSchema()
    weather_df.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 8. Describe numeric columns in yellow trips
    # ----------------------------------------------------------------
    print("=== Describe: Yellow Taxi Numeric Columns ===")
    yellow_df.select(
        "trip_distance", "fare_amount", "tip_amount", "total_amount", "passenger_count"
    ).describe().show()

    # ----------------------------------------------------------------
    # 9. Null analysis on yellow trips
    # ----------------------------------------------------------------
    print("=== Null Counts: Yellow Taxi Trips ===")
    null_counts = yellow_df.select(
        [count(when(col(c).isNull(), c)).alias(c) for c in yellow_df.columns]
    )
    null_counts.show(truncate=False)

    # Databricks: display(null_counts) shows this as a nice table

    # ----------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for name, df in [
        ("Yellow Trips", yellow_df),
        ("Green Trips", green_df),
        ("FHV Trips", fhvhv_df),
        ("Taxi Zones", zones_df),
        ("Vendors", vendors_df),
        ("Rate Codes", rate_codes_df),
        ("Payment Types", payment_types_df),
        ("Weather", weather_df),
    ]:
        print(f"  {name:20s}: {df.count():>12,} rows, {len(df.columns):>3} columns")
    print()

    spark.stop()
    print("SparkSession stopped.")


if __name__ == "__main__":
    main()
