"""
Module 06 - Exercise 9: Schema Evolution
==========================================
Demonstrate how the medallion architecture handles schema changes, using
the real-world example of NYC TLC adding the `airport_fee` column to
yellow taxi data starting in 2019.

Databricks Delta Lake handles schema evolution natively:
    df.write.format("delta") \\
        .option("mergeSchema", "true") \\
        .mode("append") \\
        .saveAsTable("nyc_taxi.bronze.yellow_trips")

    -- Or via ALTER TABLE:
    ALTER TABLE nyc_taxi.bronze.yellow_trips
    ADD COLUMN airport_fee DOUBLE;

    -- Schema evolution in DLT is automatic:
    @dlt.table
    def bronze_yellow_trips():
        return spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load("/mnt/raw/yellow_tripdata_*.parquet")

Unity Catalog tracks schema versions automatically and provides
DESCRIBE HISTORY for audit:
    DESCRIBE HISTORY nyc_taxi.bronze.yellow_trips;
    SHOW COLUMNS IN nyc_taxi.bronze.yellow_trips;
"""

import uuid
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("schema_evolution")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    print("=" * 60)
    print("  SCHEMA EVOLUTION DEMONSTRATION")
    print("  (NYC TLC airport_fee column addition)")
    print("=" * 60)

    spark = get_spark()

    try:
        # -------------------------------------------------------------------
        # 1. Show current Bronze schema
        # -------------------------------------------------------------------
        bronze_path = str(BRONZE_DIR / "yellow_trips")
        old_df = spark.read.parquet(bronze_path)
        old_schema = old_df.schema
        old_columns = set(old_df.columns)

        print(f"\n[1] Current Bronze yellow trips schema:")
        for field in old_schema.fields:
            if not field.name.startswith("_"):
                print(f"    {field.name}: {field.dataType.simpleString()}")
        print(f"    Total columns: {len(old_schema.fields)}")
        print(f"    Total records: {old_df.count():,}")

        # Check if airport_fee already exists
        has_airport_fee = "airport_fee" in old_columns
        print(f"    airport_fee column present: {has_airport_fee}")

        # -------------------------------------------------------------------
        # 2. Simulate schema evolution: add airport_fee column
        # -------------------------------------------------------------------
        # In real life, this happened when TLC updated their data format
        # in 2019. Older files don't have airport_fee; newer ones do.
        #
        # Databricks Auto Loader handles this automatically:
        #   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        print(f"\n[2] Simulating schema evolution...")
        print(f"    Scenario: TLC adds 'airport_fee' column to yellow taxi data")
        print(f"    starting in 2019. Older data files lack this column.")

        # Create a "new batch" that includes airport_fee
        # Take a sample and add the new column
        sample_size = min(100, old_df.count())
        new_batch = old_df.limit(sample_size)

        if not has_airport_fee:
            # Add the new airport_fee column
            new_batch = new_batch.withColumn(
                "airport_fee",
                F.when(
                    F.col("PULocationID").isin(132, 138)
                    | F.col("DOLocationID").isin(132, 138),
                    F.lit(1.75)
                ).otherwise(F.lit(0.0))
            )
        else:
            print("    airport_fee already exists; simulating with a different column")
            new_batch = new_batch.withColumn(
                "congestion_surcharge_v2",
                F.lit(2.50).cast(DoubleType())
            )

        new_col = "airport_fee" if not has_airport_fee else "congestion_surcharge_v2"

        # Update metadata for the new batch
        new_batch = (
            new_batch
            .withColumn("_ingested_at", F.lit(datetime.now().isoformat()))
            .withColumn("_source_file", F.lit("yellow_tripdata_2024-06.parquet"))
            .withColumn("_batch_id", F.lit(str(uuid.uuid4())))
        )

        print(f"    New batch size: {sample_size}")
        print(f"    New column: {new_col}")

        # -------------------------------------------------------------------
        # 3. Append to Bronze using unionByName (schema union)
        # -------------------------------------------------------------------
        # Databricks Delta Lake:
        #   df.write.format("delta")
        #     .option("mergeSchema", "true")
        #     .mode("append")
        #     .save("/mnt/bronze/yellow_trips")
        #
        # Local PySpark equivalent: unionByName with allowMissingColumns
        combined = old_df.unionByName(new_batch, allowMissingColumns=True)

        print(f"\n[3] Bronze after schema evolution (unionByName):")
        print(f"    Total records: {combined.count():,}")

        # Show nulls in new column for old records
        null_count = combined.filter(F.col(new_col).isNull()).count()
        non_null_count = combined.filter(F.col(new_col).isNotNull()).count()
        print(f"    {new_col} nulls (old records): {null_count:,}")
        print(f"    {new_col} populated (new records): {non_null_count:,}")

        # Write the evolved Bronze
        evolved_path = str(BRONZE_DIR / "yellow_trips_evolved")
        combined.write.mode("overwrite").parquet(evolved_path)

        # -------------------------------------------------------------------
        # 4. Show schema diff
        # -------------------------------------------------------------------
        new_schema = combined.schema
        new_columns = set(combined.columns)
        added_cols = new_columns - old_columns
        removed_cols = old_columns - new_columns

        print(f"\n[4] Schema diff:")
        print(f"    Added columns:   {added_cols if added_cols else 'none'}")
        print(f"    Removed columns: {removed_cols if removed_cols else 'none'}")

        print(f"\n    Evolved schema:")
        for field in new_schema.fields:
            marker = " <-- NEW" if field.name in added_cols else ""
            if not field.name.startswith("_"):
                print(f"      {field.name}: {field.dataType.simpleString()}{marker}")

        # -------------------------------------------------------------------
        # 5. Update Silver with evolved schema
        # -------------------------------------------------------------------
        # Databricks DLT: Schema evolution is handled automatically when
        # you set "pipelines.reset.allowed" = "true" or use
        # @dlt.expect with the new column
        print(f"\n[5] Rebuilding Silver with evolved Bronze data...")

        df = combined

        # Same cleaning as silver_yellow_trips.py
        df = df.filter(
            F.col("tpep_pickup_datetime").isNotNull()
            & F.col("tpep_dropoff_datetime").isNotNull()
            & (F.col("fare_amount") >= 0)
            & (F.col("total_amount") >= 0)
        )

        df = df.dropDuplicates([
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "PULocationID", "DOLocationID", "fare_amount",
        ])

        # Add computed columns
        df = df.withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime")
             - F.unix_timestamp("tpep_pickup_datetime")) / 60.0
        )

        # Handle the new column: fill nulls for old records
        df = df.withColumn(
            new_col,
            F.coalesce(F.col(new_col), F.lit(0.0))
        )

        df = df.filter(F.col("trip_duration_minutes").between(0.5, 720))

        silver_evolved_path = str(SILVER_DIR / "yellow_trips_evolved")
        df.write.mode("overwrite").parquet(silver_evolved_path)

        print(f"    Silver records: {df.count():,}")
        print(f"    Written to: {silver_evolved_path}")

        # Show distribution of new column
        print(f"\n    {new_col} distribution in Silver:")
        df.groupBy(new_col).count().orderBy(F.desc("count")).show(10, truncate=False)

        # -------------------------------------------------------------------
        # 6. Gold layer backward compatibility
        # -------------------------------------------------------------------
        # Databricks: Existing Gold views/tables continue to work because
        # adding a column doesn't break existing queries.
        print(f"\n[6] Gold layer backward compatibility check...")
        print(f"    Existing Gold queries that don't reference {new_col}")
        print(f"    continue to work without modification.")

        # Existing Gold query: daily metrics (does NOT use new column)
        daily_metrics = (
            df
            .groupBy(F.to_date("tpep_pickup_datetime").alias("pickup_date"))
            .agg(
                F.count("*").alias("total_trips"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration"),
            )
            .orderBy("pickup_date")
        )

        print(f"\n    Sample Gold query (daily metrics) -- still works:")
        daily_metrics.show(5, truncate=False)

        # New Gold query USING the new column
        if new_col == "airport_fee":
            print(f"\n    New Gold query using airport_fee:")
            df.groupBy(
                F.when(F.col("airport_fee") > 0, "airport")
                .otherwise("non_airport")
                .alias("trip_type")
            ).agg(
                F.count("*").alias("trips"),
                F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
                F.round(F.sum("airport_fee"), 2).alias("total_airport_fees"),
            ).show(truncate=False)

        # -------------------------------------------------------------------
        # Summary
        # -------------------------------------------------------------------
        print(f"\n{'='*60}")
        print(f"  SCHEMA EVOLUTION COMPLETE")
        print(f"{'='*60}")
        print(f"  Key takeaways:")
        print(f"  1. Bronze: unionByName(allowMissingColumns=True) handles")
        print(f"     schema drift automatically (Databricks: mergeSchema=true)")
        print(f"  2. Silver: Fill nulls for the new column in old records")
        print(f"  3. Gold: Existing queries are backward compatible")
        print(f"  4. Databricks Auto Loader + DLT handle this automatically")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
