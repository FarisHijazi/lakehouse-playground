"""
Exercise 2: Transformations on NYC Taxi Trips
===============================================
Narrow transformations: filter, select, withColumn, when/otherwise.
Add trip_duration, speed, trip classification, suspicious trip flags,
and time extraction columns.

Databricks equivalent:
    - Same PySpark code works in Databricks notebooks
    - Use display(df) instead of df.show()
    - Databricks auto-visualizes results with charts
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    dayofweek,
    hour,
    lit,
    month,
    round as spark_round,
    to_date,
    when,
)


def main():
    # Databricks: spark is pre-configured; skip SparkSession.builder
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TaxiAnalytics")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    data_dir = PROJECT_ROOT / "data" / "raw"

    # Load yellow taxi trips
    yellow_df = spark.read.parquet(str(data_dir / "yellow_tripdata_*.parquet"))
    total_rows = yellow_df.count()
    print(f"Total yellow trips loaded: {total_rows:,}")

    # ----------------------------------------------------------------
    # 1. Filter: valid trips only (distance > 0 and fare > 0)
    # ----------------------------------------------------------------
    valid_trips = yellow_df.filter(
        (col("trip_distance") > 0) & (col("fare_amount") > 0)
    )
    filtered_out = total_rows - valid_trips.count()
    print(f"\n=== Filter: Valid Trips ===")
    print(f"Valid trips   : {valid_trips.count():,}")
    print(f"Filtered out  : {filtered_out:,} ({filtered_out / max(total_rows, 1) * 100:.1f}%)")

    # ----------------------------------------------------------------
    # 2. Select: keep relevant columns
    # ----------------------------------------------------------------
    selected = valid_trips.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "passenger_count",
    )
    print("\n=== Select: Relevant Columns ===")
    selected.printSchema()
    selected.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 3. withColumn: trip_duration_minutes
    # ----------------------------------------------------------------
    with_duration = selected.withColumn(
        "trip_duration_minutes",
        spark_round(
            (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60.0,
            2,
        ),
    )
    print("=== withColumn: trip_duration_minutes ===")
    with_duration.select(
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_duration_minutes"
    ).show(5, truncate=False)

    # ----------------------------------------------------------------
    # 4. withColumn: speed_mph
    # ----------------------------------------------------------------
    with_speed = with_duration.withColumn(
        "speed_mph",
        when(
            col("trip_duration_minutes") > 0,
            spark_round(col("trip_distance") / (col("trip_duration_minutes") / 60.0), 2),
        ).otherwise(lit(None)),
    )
    print("=== withColumn: speed_mph ===")
    with_speed.select(
        "trip_distance", "trip_duration_minutes", "speed_mph"
    ).show(10, truncate=False)

    # ----------------------------------------------------------------
    # 5. when/otherwise: trip distance classification
    # ----------------------------------------------------------------
    classified = with_speed.withColumn(
        "trip_category",
        when(col("trip_distance") < 1, "short")
        .when(col("trip_distance") < 5, "medium")
        .when(col("trip_distance") < 20, "long")
        .otherwise("extra_long"),
    )
    print("=== Trip Classification ===")
    classified.groupBy("trip_category").count().orderBy("count", ascending=False).show()

    # ----------------------------------------------------------------
    # 6. Flag suspicious trips
    # ----------------------------------------------------------------
    flagged = classified.withColumn(
        "is_suspicious",
        when(col("speed_mph") > 100, lit(True))
        .when((col("trip_distance") == 0) & (col("fare_amount") > 0), lit(True))
        .when(col("trip_duration_minutes") < 0, lit(True))
        .otherwise(lit(False)),
    )
    suspicious_count = flagged.filter(col("is_suspicious")).count()
    print("=== Suspicious Trip Flags ===")
    print(f"Suspicious trips: {suspicious_count:,} ({suspicious_count / max(flagged.count(), 1) * 100:.2f}%)")
    flagged.filter(col("is_suspicious")).select(
        "trip_distance", "fare_amount", "trip_duration_minutes", "speed_mph", "is_suspicious"
    ).show(10, truncate=False)

    # ----------------------------------------------------------------
    # 7. Time extraction: hour, day_of_week, month
    # ----------------------------------------------------------------
    with_time = flagged.withColumn(
        "pickup_hour", hour(col("tpep_pickup_datetime"))
    ).withColumn(
        "pickup_day_of_week", dayofweek(col("tpep_pickup_datetime"))
    ).withColumn(
        "pickup_month", month(col("tpep_pickup_datetime"))
    ).withColumn(
        "pickup_date", to_date(col("tpep_pickup_datetime"))
    )

    print("=== Time Extraction ===")
    with_time.select(
        "tpep_pickup_datetime", "pickup_hour", "pickup_day_of_week", "pickup_month", "pickup_date"
    ).show(10, truncate=False)

    # Quick distribution check
    print("Trips by hour of day:")
    with_time.groupBy("pickup_hour").count().orderBy("pickup_hour").show(24)

    print("Trips by day of week (1=Sunday, 7=Saturday):")
    with_time.groupBy("pickup_day_of_week").count().orderBy("pickup_day_of_week").show()

    # ----------------------------------------------------------------
    # 8. Explain plan -- verify no shuffle for narrow transformations
    # ----------------------------------------------------------------
    print("=== Explain Plan: Narrow Transformations ===")
    print("(No 'Exchange' node = no shuffle)")
    with_time.filter(~col("is_suspicious")).select(
        "trip_category", "speed_mph", "pickup_hour"
    ).explain()

    # Databricks: Use the Spark UI tab in the notebook for visual DAG

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
