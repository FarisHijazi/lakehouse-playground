"""
Module 06 - Exercise 10: MERGE / Upsert Pattern
=================================================
Implement idempotent writes using DuckDB's MERGE, simulating Delta Lake's
MERGE INTO behavior for handling late-arriving taxi trip records.

Databricks Delta Lake equivalent:
    MERGE INTO nyc_taxi.silver.yellow_trips AS target
    USING nyc_taxi.staging.yellow_corrections AS source
    ON target.surrogate_key = source.surrogate_key
    WHEN MATCHED AND source.total_amount != target.total_amount THEN
        UPDATE SET
            fare_amount = source.fare_amount,
            tip_amount = source.tip_amount,
            total_amount = source.total_amount,
            _updated_at = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT *

    -- In PySpark:
    from delta.tables import DeltaTable

    target = DeltaTable.forPath(spark, "/mnt/silver/yellow_trips")
    target.alias("target").merge(
        source_df.alias("source"),
        "target.surrogate_key = source.surrogate_key"
    ).whenMatchedUpdate(
        condition="source.total_amount != target.total_amount",
        set={"fare_amount": "source.fare_amount", ...}
    ).whenNotMatchedInsertAll().execute()

Unity Catalog target:
    nyc_taxi.silver.yellow_trips (with MERGE INTO)
"""

from pathlib import Path

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("merge_upsert")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    print("=" * 60)
    print("  MERGE / UPSERT PATTERN WITH DUCKDB")
    print("  (Simulating Delta Lake MERGE INTO)")
    print("=" * 60)

    spark = get_spark()

    try:
        # -------------------------------------------------------------------
        # 1. Load a sample of Silver yellow trips as the "target" table
        # -------------------------------------------------------------------
        silver_path = str(SILVER_DIR / "yellow_trips")
        trips = spark.read.parquet(silver_path)

        # Take a manageable sample for the demo
        sample = trips.limit(1000).toPandas()
        print(f"\n[1] Sample of Silver yellow trips loaded: {len(sample):,}")

        conn = duckdb.connect()

        # Create target table
        conn.execute("CREATE TABLE silver_yellow_trips AS SELECT * FROM sample")
        initial_count = conn.execute(
            "SELECT COUNT(*) FROM silver_yellow_trips"
        ).fetchone()[0]
        print(f"    Target table created: {initial_count:,} rows")

        # Add a surrogate key for matching (pickup_time + locations + fare)
        # Databricks: You would typically use a hash-based surrogate key
        # or Delta Lake's built-in row tracking
        conn.execute("""
            ALTER TABLE silver_yellow_trips ADD COLUMN IF NOT EXISTS
            surrogate_key VARCHAR
        """)
        conn.execute("""
            UPDATE silver_yellow_trips
            SET surrogate_key = md5(
                COALESCE(CAST(tpep_pickup_datetime AS VARCHAR), '') ||
                COALESCE(CAST(tpep_dropoff_datetime AS VARCHAR), '') ||
                COALESCE(CAST(PULocationID AS VARCHAR), '') ||
                COALESCE(CAST(DOLocationID AS VARCHAR), '')
            )
        """)

        # -------------------------------------------------------------------
        # 2. Generate a "correction batch" with updates + new records
        # -------------------------------------------------------------------
        print(f"\n[2] Generating correction batch...")

        # Get some existing trips to "correct" (simulate fare adjustments)
        existing_sample = conn.execute("""
            SELECT * FROM silver_yellow_trips
            ORDER BY tpep_pickup_datetime
            LIMIT 50
        """).fetchdf()

        # Simulate fare corrections: increase fare by 10%
        existing_sample["fare_amount"] = (
            existing_sample["fare_amount"].astype(float) * 1.1
        ).round(2)
        existing_sample["total_amount"] = (
            existing_sample["total_amount"].astype(float) * 1.1
        ).round(2)
        existing_sample["_correction_reason"] = "fare_adjustment"

        # Create some "new" trips (simulate late-arriving records)
        # Databricks context: Late-arriving records are common in taxi data
        # because trip records may be submitted days after the trip occurred.
        new_trips = conn.execute("""
            SELECT * FROM silver_yellow_trips
            LIMIT 20
        """).fetchdf()

        # Modify to make them "new" records
        import pandas as pd
        new_trips["tpep_pickup_datetime"] = pd.to_datetime(
            new_trips["tpep_pickup_datetime"]
        ) + pd.Timedelta(hours=100)
        new_trips["tpep_dropoff_datetime"] = pd.to_datetime(
            new_trips["tpep_dropoff_datetime"]
        ) + pd.Timedelta(hours=100)
        new_trips["surrogate_key"] = (
            new_trips["tpep_pickup_datetime"].astype(str)
            + new_trips["PULocationID"].astype(str)
            + new_trips["DOLocationID"].astype(str)
        ).apply(lambda x: str(hash(x)))

        # Drop the correction reason column from new trips if it exists
        if "_correction_reason" in new_trips.columns:
            new_trips = new_trips.drop(columns=["_correction_reason"])

        # Combine into incoming batch
        corrections = existing_sample.drop(
            columns=["_correction_reason"], errors="ignore"
        )
        incoming = pd.concat([corrections, new_trips], ignore_index=True)

        print(f"    Incoming batch: {len(incoming):,} records")
        print(f"      - Corrections to existing trips: {len(corrections)}")
        print(f"      - New late-arriving trips: {len(new_trips)}")

        # Register incoming batch
        conn.execute("CREATE TABLE incoming AS SELECT * FROM incoming")

        # -------------------------------------------------------------------
        # 3. Perform MERGE (upsert)
        # -------------------------------------------------------------------
        # This is the core pattern. In Databricks, this is a single
        # MERGE INTO statement on a Delta table.
        #
        # Databricks SQL:
        #   MERGE INTO nyc_taxi.silver.yellow_trips AS target
        #   USING staging.corrections AS source
        #   ON target.surrogate_key = source.surrogate_key
        #   WHEN MATCHED THEN UPDATE SET *
        #   WHEN NOT MATCHED THEN INSERT *
        print(f"\n[3] Performing MERGE (upsert)...")
        print(f"    Databricks equivalent: MERGE INTO ... USING ... ON surrogate_key")

        # Build dynamic column lists
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'silver_yellow_trips' "
            "ORDER BY ordinal_position"
        ).fetchdf()["column_name"].tolist()

        non_key_cols = [c for c in columns if c != "surrogate_key"]
        update_set = ", ".join([f"{c} = source.{c}" for c in non_key_cols])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"source.{c}" for c in columns])

        merge_sql = f"""
        MERGE INTO silver_yellow_trips AS target
        USING incoming AS source
        ON target.surrogate_key = source.surrogate_key
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals})
        """

        conn.execute(merge_sql)

        after_merge = conn.execute(
            "SELECT COUNT(*) FROM silver_yellow_trips"
        ).fetchone()[0]
        print(f"    Before MERGE: {initial_count:,}")
        print(f"    After MERGE:  {after_merge:,}")
        print(f"    New rows inserted: {after_merge - initial_count}")

        # -------------------------------------------------------------------
        # 4. Idempotency check: run the same MERGE again
        # -------------------------------------------------------------------
        # Databricks: Delta Lake MERGE is inherently idempotent when using
        # deterministic match conditions.
        print(f"\n[4] Idempotency check: running the same MERGE again...")

        conn.execute(merge_sql)
        after_second = conn.execute(
            "SELECT COUNT(*) FROM silver_yellow_trips"
        ).fetchone()[0]

        print(f"    After 1st MERGE: {after_merge:,}")
        print(f"    After 2nd MERGE: {after_second:,}")

        if after_merge == after_second:
            print(f"    PASS: Row count unchanged -- upsert is idempotent")
        else:
            print(f"    FAIL: Row count changed -- upsert is NOT idempotent")

        # -------------------------------------------------------------------
        # 5. Verify corrections were applied
        # -------------------------------------------------------------------
        print(f"\n[5] Verification -- sample of corrected records:")
        sample_check = conn.execute("""
            SELECT
                tpep_pickup_datetime,
                PULocationID,
                DOLocationID,
                fare_amount,
                total_amount
            FROM silver_yellow_trips
            ORDER BY tpep_pickup_datetime
            LIMIT 5
        """).fetchdf()
        print(sample_check.to_string(index=False))

        # -------------------------------------------------------------------
        # 6. Audit trail
        # -------------------------------------------------------------------
        # Databricks: Delta Lake provides DESCRIBE HISTORY for full audit trail
        # DESCRIBE HISTORY nyc_taxi.silver.yellow_trips
        print(f"\n[6] Audit trail:")
        print(f"    Records in target before: {initial_count:,}")
        print(f"    Incoming batch size:      {len(incoming):,}")
        print(f"    Records updated:          {len(corrections):,}")
        print(f"    Records inserted:         {after_merge - initial_count:,}")
        print(f"    Final record count:       {after_second:,}")
        print()
        print(f"    Databricks equivalent audit:")
        print(f"      DESCRIBE HISTORY nyc_taxi.silver.yellow_trips")
        print(f"      -- Shows version, timestamp, operation, operationMetrics")

        # -------------------------------------------------------------------
        # 7. Write final result
        # -------------------------------------------------------------------
        result = conn.execute("SELECT * FROM silver_yellow_trips").fetchdf()
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        result.to_parquet(
            str(GOLD_DIR / "merge_upsert_demo.parquet"),
            engine="pyarrow",
            index=False,
        )
        print(f"\n[7] Final result written to: {GOLD_DIR / 'merge_upsert_demo.parquet'}")

        conn.close()

        print(f"\n{'='*60}")
        print(f"  MERGE / UPSERT PATTERN COMPLETE")
        print(f"{'='*60}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
