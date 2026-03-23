"""
Exercise 12: Delta Lake Operations (MERGE, Time Travel, VACUUM)
================================================================
ACID transactions, upserts, and data versioning with Delta Lake.

Requires: pip install delta-spark
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

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
            .appName("DeltaLake-PodcastAnalytics")
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
            .appName("DeltaLake-PodcastAnalytics")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
    return spark


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    data_dir = Path(__file__).resolve().parent.parent.parent / "data" / "raw"
    delta_dir = Path(__file__).resolve().parent.parent.parent / "data" / "spark_output" / "delta"

    users_path = str(delta_dir / "users")
    events_path = str(delta_dir / "listening_events")

    if not HAS_DELTA:
        print("Delta Lake is not available. Showing what the code would do.\n")
        spark.stop()
        return

    # ----------------------------------------------------------------
    # 1. Read raw users
    # ----------------------------------------------------------------
    users_df = spark.read.csv(str(data_dir / "users.csv"), header=True, inferSchema=True)
    print(f"Raw users: {users_df.count()} rows")
    users_df.printSchema()

    # ----------------------------------------------------------------
    # 2. Write as Delta table
    # ----------------------------------------------------------------
    print("\n=== Writing Delta Table: Users ===")
    (
        users_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(users_path)
    )
    print(f"Delta table written to: {users_path}")

    # ----------------------------------------------------------------
    # 3. Read and verify
    # ----------------------------------------------------------------
    users_delta = spark.read.format("delta").load(users_path)
    print(f"\nDelta table row count: {users_delta.count()}")
    users_delta.show(5, truncate=False)

    # ----------------------------------------------------------------
    # 4. MERGE (upsert): update existing + insert new users
    # ----------------------------------------------------------------
    print("\n=== MERGE: Upsert users ===")

    # Create a small batch of updates:
    #   - usr_000001: update subscription to "premium"
    #   - usr_000002: update age to 25
    #   - usr_999001: brand new user
    #   - usr_999002: brand new user
    update_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("subscription_type", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
    ])

    update_data = [
        ("usr_000001", "Zainab", "zainab_3964@hotmail.com", "QA", "Doha", "web",
         "2019-09-03", "premium", 27, "f"),
        ("usr_000002", "Yasser", "yasser_7703@gmail.com", "US", "Khobar", "web",
         "2024-09-10", "free", 25, "male"),
        ("usr_999001", "NewUser1", "new1@example.com", "SA", "Riyadh", "ios",
         "2025-01-15", "trial", 30, "M"),
        ("usr_999002", "NewUser2", "new2@example.com", "AE", "Dubai", "android",
         "2025-02-20", "free", 22, "F"),
    ]

    updates_df = spark.createDataFrame(update_data, schema=update_schema)
    print("Update batch:")
    updates_df.show(truncate=False)

    # Perform MERGE
    delta_table = DeltaTable.forPath(spark, users_path)
    (
        delta_table.alias("target")
        .merge(
            updates_df.alias("source"),
            "target.user_id = source.user_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Verify
    users_after_merge = spark.read.format("delta").load(users_path)
    print(f"Users after merge: {users_after_merge.count()} (was {users_df.count()})")
    print("\nUpdated/new records:")
    users_after_merge.filter(
        col("user_id").isin("usr_000001", "usr_000002", "usr_999001", "usr_999002")
    ).show(truncate=False)

    # ----------------------------------------------------------------
    # 5. Time travel: read previous version
    # ----------------------------------------------------------------
    print("\n=== Time Travel ===")

    # Version 0 = original write
    users_v0 = spark.read.format("delta").option("versionAsOf", 0).load(users_path)
    users_current = spark.read.format("delta").load(users_path)

    print(f"Version 0 row count: {users_v0.count()}")
    print(f"Current row count  : {users_current.count()}")
    print(f"Rows added         : {users_current.count() - users_v0.count()}")

    print("\nusr_000001 in version 0:")
    users_v0.filter(col("user_id") == "usr_000001").select(
        "user_id", "name", "subscription_type", "city"
    ).show(truncate=False)

    print("usr_000001 in current version:")
    users_current.filter(col("user_id") == "usr_000001").select(
        "user_id", "name", "subscription_type", "city"
    ).show(truncate=False)

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

    new_user_with_extra_col = spark.createDataFrame(
        [("usr_999003", "NewUser3", "new3@example.com", "KW", "Kuwait City", "web",
          "2025-03-01", "premium", 28, "M", "2025-03-01 10:00:00")],
        schema=StructType(
            update_schema.fields + [StructField("last_login", StringType(), True)]
        ),
    )

    print("New data has an extra column 'last_login':")
    new_user_with_extra_col.printSchema()

    (
        new_user_with_extra_col
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(users_path)
    )

    evolved = spark.read.format("delta").load(users_path)
    print(f"Schema after evolution:")
    evolved.printSchema()
    print(f"Total rows: {evolved.count()}")
    evolved.filter(col("user_id") == "usr_999003").show(truncate=False)

    # ----------------------------------------------------------------
    # 8. VACUUM: clean up old files
    # ----------------------------------------------------------------
    print("\n=== VACUUM ===")
    print("Disabling retention check for demo (NEVER do this in production)...")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    delta_table = DeltaTable.forPath(spark, users_path)
    delta_table.vacuum(0)  # 0 hours retention = delete all old versions

    print("VACUUM complete. Old file versions removed.")
    print("NOTE: After vacuum(0), time travel to version 0 will fail.")
    print("In production, use vacuum(168) to keep 7 days of history.\n")

    # Verify current data still works
    final = spark.read.format("delta").load(users_path)
    print(f"Final row count after VACUUM: {final.count()}")

    # Show final history
    print("\n=== Final History ===")
    DeltaTable.forPath(spark, users_path).history().select(
        "version", "timestamp", "operation"
    ).show(truncate=False)

    spark.stop()
    print("\nDone.")


if __name__ == "__main__":
    main()
