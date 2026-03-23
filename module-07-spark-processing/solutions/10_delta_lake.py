"""
Exercise 10: Delta Lake Operations (MERGE, Time Travel, VACUUM)
================================================================
ACID transactions, upserts, and data versioning with Delta Lake on NYC taxi data.

Requires: pip install delta-spark

Databricks equivalent:
    - Delta is the default format in Databricks -- no extra config needed
    - MERGE syntax identical in Databricks SQL
    - Time travel: SELECT * FROM my_table VERSION AS OF 0
    - VACUUM managed automatically by Databricks with predictive optimization
    - Unity Catalog tracks all table versions and lineage
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Try importing delta -- it is an optional dependency
try:
    from delta import configure_spark_with_delta_pip
    from delta.tables import DeltaTable

    HAS_DELTA = True
except ImportError:
    HAS_DELTA = False


def create_spark_session():
    """Create a SparkSession with Delta Lake support."""
    if HAS_DELTA:
        builder = (
            SparkSession.builder
            .master("local[*]")
            .appName("DeltaLake-TaxiAnalytics")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.shuffle.partitions", "8")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        print("WARNING: delta-spark not installed. Install with: pip install delta-spark")
        print("Falling back to standard SparkSession (Delta operations will fail).\n")
        spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("DeltaLake-TaxiAnalytics")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
    return spark


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    data_dir = PROJECT_ROOT / "data" / "raw"
    delta_dir = PROJECT_ROOT / "data" / "spark_output" / "delta"

    zones_path = str(delta_dir / "zones")
    trips_path = str(delta_dir / "yellow_trips")

    if not HAS_DELTA:
        print("Delta Lake is not available. Showing what the code would do.\n")
        spark.stop()
        return

    # ----------------------------------------------------------------
    # 1. Read raw zone lookup
    # ----------------------------------------------------------------
    zones_df = spark.read.csv(
        str(data_dir / "taxi_zone_lookup.csv"), header=True, inferSchema=True
    )
    print(f"Raw zones: {zones_df.count()} rows")
    zones_df.printSchema()

    # ----------------------------------------------------------------
    # 2. Write as Delta table
    # ----------------------------------------------------------------
    print("\n=== Writing Delta Table: Zones ===")
    (
        zones_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(zones_path)
    )
    print(f"Delta table written to: {zones_path}")

    # ----------------------------------------------------------------
    # 3. Read and verify
    # ----------------------------------------------------------------
    zones_delta = spark.read.format("delta").load(zones_path)
    print(f"\nDelta table row count: {zones_delta.count()}")
    zones_delta.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 4. MERGE (upsert): update existing + insert new zones
    # ----------------------------------------------------------------
    print("\n=== MERGE: Upsert zones ===")

    # Create a small batch of updates:
    #   - LocationID 1: update Borough name
    #   - LocationID 2: update Zone name
    #   - LocationID 300: brand new zone
    #   - LocationID 301: brand new zone
    update_schema = StructType([
        StructField("LocationID", IntegerType(), False),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ])

    update_data = [
        (1, "EWR", "Newark Airport - Terminal A", "Airports"),
        (2, "Queens", "Jamaica Bay West", "Boro Zone"),
        (300, "Manhattan", "Hudson Yards North", "Yellow Zone"),
        (301, "Brooklyn", "Gowanus Canal", "Boro Zone"),
    ]

    updates_df = spark.createDataFrame(update_data, schema=update_schema)
    print("Update batch:")
    updates_df.show(truncate=False)

    # Perform MERGE
    delta_table = DeltaTable.forPath(spark, zones_path)
    (
        delta_table.alias("target")
        .merge(
            updates_df.alias("source"),
            "target.LocationID = source.LocationID",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Verify
    zones_after_merge = spark.read.format("delta").load(zones_path)
    print(f"Zones after merge: {zones_after_merge.count()} (was {zones_df.count()})")
    print("\nUpdated/new records:")
    zones_after_merge.filter(
        col("LocationID").isin(1, 2, 300, 301)
    ).show(truncate=False)

    # ----------------------------------------------------------------
    # 5. Time travel: read previous version
    # ----------------------------------------------------------------
    print("\n=== Time Travel ===")

    # Version 0 = original write
    zones_v0 = spark.read.format("delta").option("versionAsOf", 0).load(zones_path)
    zones_current = spark.read.format("delta").load(zones_path)

    print(f"Version 0 row count: {zones_v0.count()}")
    print(f"Current row count  : {zones_current.count()}")
    print(f"Rows added         : {zones_current.count() - zones_v0.count()}")

    print("\nLocationID 1 in version 0:")
    zones_v0.filter(col("LocationID") == 1).show(truncate=False)

    print("LocationID 1 in current version:")
    zones_current.filter(col("LocationID") == 1).show(truncate=False)

    # ----------------------------------------------------------------
    # 6. History: view the table changelog
    # ----------------------------------------------------------------
    print("\n=== Delta Table History ===")
    history = delta_table.history()
    history.select(
        "version", "timestamp", "operation", "operationMetrics"
    ).show(truncate=False)

    # ----------------------------------------------------------------
    # 7. Schema evolution: add a new column
    # ----------------------------------------------------------------
    print("\n=== Schema Evolution ===")

    new_zone_with_extra_col = spark.createDataFrame(
        [(302, "Manhattan", "Penn Station South", "Yellow Zone", 40.7484)],
        schema=StructType(
            update_schema.fields + [StructField("latitude", DoubleType(), True)]
        ),
    )

    print("New data has an extra column 'latitude':")
    new_zone_with_extra_col.printSchema()

    (
        new_zone_with_extra_col
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(zones_path)
    )

    evolved = spark.read.format("delta").load(zones_path)
    print(f"Schema after evolution:")
    evolved.printSchema()
    print(f"Total rows: {evolved.count()}")
    evolved.filter(col("LocationID") == 302).show(truncate=False)

    # ----------------------------------------------------------------
    # 8. VACUUM: clean up old files
    # ----------------------------------------------------------------
    print("\n=== VACUUM ===")
    print("Disabling retention check for demo (NEVER do this in production)...")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    delta_table = DeltaTable.forPath(spark, zones_path)
    delta_table.vacuum(0)  # 0 hours retention = delete all old versions

    print("VACUUM complete. Old file versions removed.")
    print("NOTE: After vacuum(0), time travel to version 0 will fail.")
    print("In production, use vacuum(168) to keep 7 days of history.\n")

    # Verify current data still works
    final = spark.read.format("delta").load(zones_path)
    print(f"Final row count after VACUUM: {final.count()}")

    # Show final history
    print("\n=== Final History ===")
    DeltaTable.forPath(spark, zones_path).history().select(
        "version", "timestamp", "operation"
    ).show(truncate=False)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
