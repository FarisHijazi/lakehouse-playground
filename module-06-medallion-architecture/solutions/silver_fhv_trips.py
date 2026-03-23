"""
Module 06 - Exercise 4: Silver Layer -- FHV/Rideshare Trips
=============================================================
Clean, validate, and enrich For-Hire Vehicle (Uber/Lyft) trip data.
FHV data has a different schema from taxi data: no fare information,
different column naming conventions (camelCase), and base dispatch info.

Databricks DLT equivalent:
    @dlt.table(
        comment="Cleaned FHV/rideshare trips",
        table_properties={"quality": "silver"}
    )
    @dlt.expect_or_drop("valid_pickup", "pickup_datetime IS NOT NULL")
    @dlt.expect_or_drop("valid_dropoff", "dropoff_datetime IS NOT NULL")
    @dlt.expect_or_quarantine("reasonable_duration",
        "trip_duration_minutes BETWEEN 0.5 AND 720")
    def silver_fhv_trips():
        return (
            dlt.read("bronze_fhv_trips")
            .withColumnRenamed("dropOff_datetime", "dropoff_datetime")
            ...
        )

Unity Catalog target:
    nyc_taxi.silver.fhv_trips
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
        .appName("silver_fhv_trips")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    print("=" * 60)
    print("  SILVER LAYER: FHV/RIDESHARE TRIPS")
    print("=" * 60)

    spark = get_spark()

    try:
        # -------------------------------------------------------------------
        # 1. Read Bronze
        # -------------------------------------------------------------------
        bronze_path = str(BRONZE_DIR / "fhv_trips")
        df = spark.read.parquet(bronze_path)
        bronze_count = df.count()
        print(f"\n[1] Bronze records loaded: {bronze_count:,}")
        print(f"    Columns: {df.columns}")

        # -------------------------------------------------------------------
        # 2. Standardize column names to snake_case
        # -------------------------------------------------------------------
        # FHV data uses mixed naming: dropOff_datetime, PULocationID, etc.
        # Standardize for consistency across the Silver layer.
        rename_map = {
            "dropOff_datetime": "dropoff_datetime",
            "dropoff_datetime": "dropoff_datetime",
            "PULocationID": "PULocationID",       # keep standard TLC name
            "DOLocationID": "DOLocationID",
            "dispatching_base_num": "dispatching_base_num",
            "originating_base_num": "originating_base_num",
            "hvfhs_license_num": "hvfhs_license_num",
            "request_datetime": "request_datetime",
            "on_scene_datetime": "on_scene_datetime",
        }

        for old_name, new_name in rename_map.items():
            if old_name in df.columns and old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)

        print(f"\n[2] Standardized column names to snake_case")

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
        # 4. Deduplicate
        # -------------------------------------------------------------------
        # FHV data does not have fare, so use different dedup keys
        before = df.count()
        dedup_cols = ["pickup_datetime", "dropoff_datetime",
                      "PULocationID", "DOLocationID"]
        # Add dispatching_base_num if it exists
        if "dispatching_base_num" in df.columns:
            dedup_cols.append("dispatching_base_num")
        df = df.dropDuplicates(dedup_cols)
        dupes_removed = before - df.count()
        print(f"\n[4] Deduplication: {dupes_removed:,} duplicate records removed")

        # -------------------------------------------------------------------
        # 5. Add computed columns
        # -------------------------------------------------------------------
        # trip_duration_minutes
        df = df.withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("dropoff_datetime")
                - F.unix_timestamp("pickup_datetime")
            ) / 60.0
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

        # Identify rideshare company from license number
        # HV0002 = Juno, HV0003 = Uber, HV0004 = Via, HV0005 = Lyft
        if "hvfhs_license_num" in df.columns:
            df = df.withColumn(
                "company",
                F.when(F.col("hvfhs_license_num") == "HV0003", "Uber")
                .when(F.col("hvfhs_license_num") == "HV0005", "Lyft")
                .when(F.col("hvfhs_license_num") == "HV0002", "Juno")
                .when(F.col("hvfhs_license_num") == "HV0004", "Via")
                .otherwise("Other")
            )

        # Wait time: time from request to pickup (if request_datetime exists)
        if "request_datetime" in df.columns:
            df = df.withColumn(
                "wait_time_minutes",
                F.when(
                    F.col("request_datetime").isNotNull(),
                    (
                        F.unix_timestamp("pickup_datetime")
                        - F.unix_timestamp("request_datetime")
                    ) / 60.0
                )
            )

        print(f"\n[5] Computed columns added: trip_duration_minutes, "
              f"is_airport_trip, is_rush_hour, pickup_date, company")

        # -------------------------------------------------------------------
        # 6. Validate & quarantine
        # -------------------------------------------------------------------
        valid_mask = F.col("trip_duration_minutes").between(0.5, 720)

        quarantine = df.filter(~valid_mask)
        df = df.filter(valid_mask)

        quarantine_count = quarantine.count()
        print(f"\n[6] Validation & quarantine:")
        print(f"    Records quarantined: {quarantine_count:,}")
        print(f"    Valid Silver records: {df.count():,}")

        # -------------------------------------------------------------------
        # 7. Join with FHV bases reference (if available)
        # -------------------------------------------------------------------
        # Databricks: Could use a broadcast join for small dimension tables
        # spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")
        fhv_bases_path = BRONZE_DIR / "reference" / "fhv_bases"
        if fhv_bases_path.exists():
            bases = spark.read.parquet(str(fhv_bases_path))
            # Select relevant columns, dedup
            base_cols = [c for c in bases.columns if not c.startswith("_")]
            bases = bases.select(base_cols).dropDuplicates()

            if "dispatching_base_num" in df.columns and "base_number" in bases.columns:
                df = df.join(
                    F.broadcast(bases),
                    df["dispatching_base_num"] == bases["base_number"],
                    "left"
                ).drop("base_number")
                print(f"\n[7] Joined with FHV bases reference data")
        else:
            print(f"\n[7] FHV bases reference not found, skipping join")

        # -------------------------------------------------------------------
        # 8. Write output
        # -------------------------------------------------------------------
        # Databricks: df.write.format("delta").mode("overwrite")
        #     .saveAsTable("nyc_taxi.silver.fhv_trips")
        silver_path = str(SILVER_DIR / "fhv_trips")
        df.write.mode("overwrite").parquet(silver_path)
        print(f"\n[8] Written to: {silver_path}")

        if quarantine_count > 0:
            quarantine_path = str(SILVER_DIR / "fhv_trips_quarantine")
            quarantine.write.mode("overwrite").parquet(quarantine_path)
            print(f"    Quarantine: {quarantine_path}")

        # -------------------------------------------------------------------
        # Summary
        # -------------------------------------------------------------------
        final_count = df.count()
        print(f"\n{'='*60}")
        print(f"  SILVER FHV TRIPS SUMMARY")
        print(f"{'='*60}")
        print(f"  Bronze input:        {bronze_count:,}")
        print(f"  Null timestamps:     {null_ts_removed:,}")
        print(f"  Duplicates removed:  {dupes_removed:,}")
        print(f"  Quarantined:         {quarantine_count:,}")
        print(f"  Silver output:       {final_count:,}")

        # Duration statistics
        print(f"\n  Duration statistics:")
        df.select(
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
            F.round(F.min("trip_duration_minutes"), 2).alias("min_duration_min"),
            F.round(F.max("trip_duration_minutes"), 2).alias("max_duration_min"),
            F.round(F.percentile_approx("trip_duration_minutes", 0.5), 2).alias("median_duration_min"),
        ).show(truncate=False)

        # Company distribution (if available)
        if "company" in df.columns:
            print(f"  Company distribution:")
            df.groupBy("company").count().orderBy(F.desc("count")).show(truncate=False)

        airport_pct = df.filter(F.col("is_airport_trip")).count() / max(final_count, 1) * 100
        print(f"  Airport trips: {airport_pct:.1f}%")

        print(f"\n  Sample output (first 5 rows):")
        sample_cols = ["pickup_datetime", "dropoff_datetime",
                       "PULocationID", "DOLocationID",
                       "trip_duration_minutes", "is_airport_trip"]
        if "company" in df.columns:
            sample_cols.append("company")
        df.select(sample_cols).show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
