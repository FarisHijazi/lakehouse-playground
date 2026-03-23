"""
Module 06 - Exercise 3: Silver Layer -- Green Taxi Trips
=========================================================
Clean, validate, enrich, and deduplicate Bronze green taxi trips.
Green taxis operate primarily in outer boroughs and use a slightly different
schema than yellow taxis (lpep_ prefix instead of tpep_, plus trip_type column).

Databricks DLT equivalent:
    @dlt.table(
        comment="Cleaned green taxi trips",
        table_properties={"quality": "silver"}
    )
    @dlt.expect_or_drop("valid_pickup", "pickup_datetime IS NOT NULL")
    @dlt.expect_or_drop("valid_fare", "fare_amount >= 0")
    @dlt.expect_or_quarantine("reasonable_speed", "speed_mph < 100")
    def silver_green_trips():
        return (
            dlt.read("bronze_green_trips")
            .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
            .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
            ...
        )

Unity Catalog target:
    nyc_taxi.silver.green_trips
"""

from pathlib import Path

from pyspark.sql import SparkSession
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
NEWARK_LOCATION_ID = 1


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("silver_green_trips")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    print("=" * 60)
    print("  SILVER LAYER: GREEN TAXI TRIPS")
    print("=" * 60)

    spark = get_spark()

    try:
        # -------------------------------------------------------------------
        # 1. Read Bronze
        # -------------------------------------------------------------------
        bronze_path = str(BRONZE_DIR / "green_trips")
        df = spark.read.parquet(bronze_path)
        bronze_count = df.count()
        print(f"\n[1] Bronze records loaded: {bronze_count:,}")
        print(f"    Columns: {df.columns}")

        # -------------------------------------------------------------------
        # 2. Rename columns to common convention
        # -------------------------------------------------------------------
        # Green taxis use lpep_ prefix; rename to match a common interface
        # so Gold layer can union yellow and green easily.
        # We keep the original columns AND add standardized aliases.
        # Databricks DLT: .withColumnRenamed("lpep_pickup_datetime",
        #     "pickup_datetime")
        if "lpep_pickup_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        if "lpep_dropoff_datetime" in df.columns:
            df = df.withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

        print(f"\n[2] Renamed lpep_ columns to common names")

        # -------------------------------------------------------------------
        # 3. Filter null pickup/dropoff timestamps
        # -------------------------------------------------------------------
        before = df.count()
        df = df.filter(
            F.col("pickup_datetime").isNotNull()
            & F.col("dropoff_datetime").isNotNull()
        )
        null_ts_removed = before - df.count()
        print(f"\n[3] Null timestamp filter: {null_ts_removed:,} records removed")

        # -------------------------------------------------------------------
        # 4. Handle negative fares
        # -------------------------------------------------------------------
        before = df.count()
        df = df.filter(
            (F.col("fare_amount") >= 0)
            & (F.col("total_amount") >= 0)
        )
        neg_fare_removed = before - df.count()
        print(f"\n[4] Negative fare filter: {neg_fare_removed:,} records removed")

        # -------------------------------------------------------------------
        # 5. Deduplicate
        # -------------------------------------------------------------------
        before = df.count()
        df = df.dropDuplicates([
            "pickup_datetime", "dropoff_datetime",
            "PULocationID", "DOLocationID", "fare_amount",
        ])
        dupes_removed = before - df.count()
        print(f"\n[5] Deduplication: {dupes_removed:,} duplicate records removed")

        # -------------------------------------------------------------------
        # 6. Add computed columns
        # -------------------------------------------------------------------
        # trip_duration_minutes
        df = df.withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("dropoff_datetime")
                - F.unix_timestamp("pickup_datetime")
            ) / 60.0
        )

        # speed_mph
        df = df.withColumn(
            "speed_mph",
            F.when(
                F.col("trip_duration_minutes") > 0,
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60.0)
            ).otherwise(F.lit(None).cast(DoubleType()))
        )

        # is_airport_trip
        airport_ids = [JFK_LOCATION_ID, LAGUARDIA_LOCATION_ID, NEWARK_LOCATION_ID]
        df = df.withColumn(
            "is_airport_trip",
            F.col("PULocationID").isin(airport_ids)
            | F.col("DOLocationID").isin(airport_ids)
        )

        # is_rush_hour (weekday 7-9 AM or 5-7 PM)
        df = df.withColumn(
            "is_rush_hour",
            (F.dayofweek("pickup_datetime").between(2, 6))
            & (
                F.hour("pickup_datetime").between(7, 9)
                | F.hour("pickup_datetime").between(17, 19)
            )
        )

        # pickup_date
        df = df.withColumn("pickup_date", F.to_date("pickup_datetime"))

        # Green taxi specific: trip_type label
        # trip_type: 1 = street-hail, 2 = dispatch
        if "trip_type" in df.columns:
            df = df.withColumn(
                "trip_type_label",
                F.when(F.col("trip_type") == 1, "street_hail")
                .when(F.col("trip_type") == 2, "dispatch")
                .otherwise("unknown")
            )

        print(f"\n[6] Computed columns added: trip_duration_minutes, speed_mph, "
              f"is_airport_trip, is_rush_hour, pickup_date, trip_type_label")

        # -------------------------------------------------------------------
        # 7. Validate & quarantine
        # -------------------------------------------------------------------
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
        print(f"\n[7] Validation & quarantine:")
        print(f"    Records quarantined: {quarantine_count:,}")
        print(f"    Valid Silver records: {df.count():,}")

        # -------------------------------------------------------------------
        # 8. Write output
        # -------------------------------------------------------------------
        # Databricks: df.write.format("delta").mode("overwrite")
        #     .saveAsTable("nyc_taxi.silver.green_trips")
        silver_path = str(SILVER_DIR / "green_trips")
        df.write.mode("overwrite").parquet(silver_path)
        print(f"\n[8] Written to: {silver_path}")

        if quarantine_count > 0:
            quarantine_path = str(SILVER_DIR / "green_trips_quarantine")
            quarantine.write.mode("overwrite").parquet(quarantine_path)
            print(f"    Quarantine: {quarantine_path}")

        # -------------------------------------------------------------------
        # Summary
        # -------------------------------------------------------------------
        final_count = df.count()
        print(f"\n{'='*60}")
        print(f"  SILVER GREEN TRIPS SUMMARY")
        print(f"{'='*60}")
        print(f"  Bronze input:        {bronze_count:,}")
        print(f"  Null timestamps:     {null_ts_removed:,}")
        print(f"  Negative fares:      {neg_fare_removed:,}")
        print(f"  Duplicates removed:  {dupes_removed:,}")
        print(f"  Quarantined:         {quarantine_count:,}")
        print(f"  Silver output:       {final_count:,}")

        print(f"\n  Fare statistics:")
        df.select(
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
            F.round(F.avg("total_amount"), 2).alias("avg_total"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        ).show(truncate=False)

        # Trip type distribution (green taxi specific)
        if "trip_type_label" in df.columns:
            print(f"  Trip type distribution:")
            df.groupBy("trip_type_label").count().orderBy(
                F.desc("count")
            ).show(truncate=False)

        airport_pct = df.filter(F.col("is_airport_trip")).count() / max(final_count, 1) * 100
        print(f"  Airport trips: {airport_pct:.1f}%")

        print(f"\n  Sample output (first 5 rows):")
        df.select(
            "pickup_datetime", "dropoff_datetime",
            "trip_distance", "fare_amount", "tip_amount",
            "trip_duration_minutes", "speed_mph",
            "is_airport_trip", "trip_type_label",
        ).show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
