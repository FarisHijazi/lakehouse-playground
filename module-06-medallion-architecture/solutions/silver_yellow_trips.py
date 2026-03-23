"""
Module 06 - Exercise 2: Silver Layer -- Yellow Taxi Trips
==========================================================
Clean, validate, enrich, and deduplicate Bronze yellow taxi trips.

Databricks DLT equivalent:
    import dlt
    from pyspark.sql.functions import *

    @dlt.table(
        comment="Cleaned yellow taxi trips with computed columns",
        table_properties={"quality": "silver"}
    )
    @dlt.expect_or_drop("valid_pickup", "pickup_datetime IS NOT NULL")
    @dlt.expect_or_drop("valid_dropoff", "dropoff_datetime IS NOT NULL")
    @dlt.expect_or_drop("valid_fare", "fare_amount >= 0")
    @dlt.expect_or_quarantine("reasonable_speed", "speed_mph < 100")
    @dlt.expect_or_quarantine("reasonable_duration",
        "trip_duration_minutes BETWEEN 0.5 AND 720")
    def silver_yellow_trips():
        return (
            dlt.read("bronze_yellow_trips")
            .filter(...)
            .withColumn("trip_duration_minutes", ...)
        )

Unity Catalog target:
    nyc_taxi.silver.yellow_trips
"""

from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

# Airport location IDs
JFK_LOCATION_ID = 132
LAGUARDIA_LOCATION_ID = 138
NEWARK_LOCATION_ID = 1  # EWR


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("silver_yellow_trips")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    print("=" * 60)
    print("  SILVER LAYER: YELLOW TAXI TRIPS")
    print("=" * 60)

    spark = get_spark()

    try:
        # -------------------------------------------------------------------
        # 1. Read Bronze
        # -------------------------------------------------------------------
        # Databricks: dlt.read("bronze_yellow_trips")
        bronze_path = str(BRONZE_DIR / "yellow_trips")
        df = spark.read.parquet(bronze_path)
        bronze_count = df.count()
        print(f"\n[1] Bronze records loaded: {bronze_count:,}")
        print(f"    Columns: {df.columns}")

        # -------------------------------------------------------------------
        # 2. Filter null pickup/dropoff timestamps
        # -------------------------------------------------------------------
        # Databricks DLT: @dlt.expect_or_drop("valid_pickup",
        #     "tpep_pickup_datetime IS NOT NULL")
        before = df.count()
        df = df.filter(
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("tpep_dropoff_datetime").isNotNull()
        )
        null_ts_removed = before - df.count()
        print(f"\n[2] Null timestamp filter: {null_ts_removed:,} records removed")

        # -------------------------------------------------------------------
        # 3. Handle negative fares -- flag and remove
        # -------------------------------------------------------------------
        # Databricks DLT: @dlt.expect_or_drop("valid_fare", "fare_amount >= 0")
        before = df.count()
        df = df.filter(
            (F.col("fare_amount") >= 0)
            & (F.col("total_amount") >= 0)
        )
        neg_fare_removed = before - df.count()
        print(f"\n[3] Negative fare filter: {neg_fare_removed:,} records removed")

        # -------------------------------------------------------------------
        # 4. Deduplicate
        # -------------------------------------------------------------------
        # Use natural key: pickup time + dropoff time + locations + fare
        before = df.count()
        df = df.dropDuplicates([
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "PULocationID", "DOLocationID", "fare_amount",
        ])
        dupes_removed = before - df.count()
        print(f"\n[4] Deduplication: {dupes_removed:,} duplicate records removed")

        # -------------------------------------------------------------------
        # 5. Add computed columns
        # -------------------------------------------------------------------
        # trip_duration_minutes
        df = df.withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ) / 60.0
        )

        # speed_mph: distance / (duration in hours)
        # Guard against division by zero
        df = df.withColumn(
            "speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0)
            ).otherwise(F.lit(None).cast(DoubleType()))
        )

        # is_airport_trip: pickup or dropoff at JFK (132), LGA (138), or EWR (1)
        airport_ids = [JFK_LOCATION_ID, LAGUARDIA_LOCATION_ID, NEWARK_LOCATION_ID]
        df = df.withColumn(
            "is_airport_trip",
            F.col("PULocationID").isin(airport_ids)
            | F.col("DOLocationID").isin(airport_ids)
        )

        # is_rush_hour: weekday 7-9 AM or 5-7 PM
        # dayofweek: 1=Sunday, 7=Saturday in Spark
        df = df.withColumn(
            "is_rush_hour",
            (F.dayofweek("tpep_pickup_datetime").between(2, 6))  # Mon-Fri
            & (
                F.hour("tpep_pickup_datetime").between(7, 9)
                | F.hour("tpep_pickup_datetime").between(17, 19)
            )
        )

        # pickup_date: date portion
        df = df.withColumn(
            "pickup_date",
            F.to_date("tpep_pickup_datetime")
        )

        print(f"\n[5] Computed columns added: trip_duration_minutes, speed_mph, "
              f"is_airport_trip, is_rush_hour, pickup_date")

        # -------------------------------------------------------------------
        # 6. Validate & quarantine
        # -------------------------------------------------------------------
        # Databricks DLT: @dlt.expect_or_quarantine("reasonable_speed",
        #     "speed_mph < 100")
        valid_mask = (
            (F.col("trip_duration_minutes").between(0.5, 720))
            & (
                F.col("speed_mph").isNull()
                | (F.col("speed_mph") < 100)
            )
        )

        quarantine = df.filter(~valid_mask)
        df = df.filter(valid_mask)

        quarantine_count = quarantine.count()
        print(f"\n[6] Validation & quarantine:")
        print(f"    Records quarantined: {quarantine_count:,}")
        print(f"    Valid Silver records: {df.count():,}")

        # -------------------------------------------------------------------
        # 7. Write output
        # -------------------------------------------------------------------
        # Databricks: df.write.format("delta")
        #     .mode("overwrite")
        #     .saveAsTable("nyc_taxi.silver.yellow_trips")
        silver_path = str(SILVER_DIR / "yellow_trips")
        df.write.mode("overwrite").parquet(silver_path)
        print(f"\n[7] Written to: {silver_path}")

        if quarantine_count > 0:
            quarantine_path = str(SILVER_DIR / "yellow_trips_quarantine")
            quarantine.write.mode("overwrite").parquet(quarantine_path)
            print(f"    Quarantine: {quarantine_path}")

        # -------------------------------------------------------------------
        # Summary
        # -------------------------------------------------------------------
        final_count = df.count()
        print(f"\n{'='*60}")
        print(f"  SILVER YELLOW TRIPS SUMMARY")
        print(f"{'='*60}")
        print(f"  Bronze input:        {bronze_count:,}")
        print(f"  Null timestamps:     {null_ts_removed:,}")
        print(f"  Negative fares:      {neg_fare_removed:,}")
        print(f"  Duplicates removed:  {dupes_removed:,}")
        print(f"  Quarantined:         {quarantine_count:,}")
        print(f"  Silver output:       {final_count:,}")

        # Fare statistics
        print(f"\n  Fare statistics:")
        df.select(
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        ).show(truncate=False)

        # Airport trip percentage
        airport_pct = df.filter(F.col("is_airport_trip")).count() / max(final_count, 1) * 100
        print(f"  Airport trips: {airport_pct:.1f}%")

        # Rush hour percentage
        rush_pct = df.filter(F.col("is_rush_hour")).count() / max(final_count, 1) * 100
        print(f"  Rush hour trips: {rush_pct:.1f}%")

        print(f"\n  Sample output (first 5 rows):")
        df.select(
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "trip_distance", "fare_amount", "tip_amount",
            "trip_duration_minutes", "speed_mph",
            "is_airport_trip", "is_rush_hour",
        ).show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
