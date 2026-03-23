"""
Module 06 - Exercise 1: Bronze Layer
=====================================
Land all raw NYC taxi data sources into the Bronze layer as Parquet files.
No cleaning, no transformations -- just faithful copies with ingestion metadata.

Databricks equivalent:
    In production, you would use Auto Loader (cloudFiles) to stream new files
    into Bronze Delta tables. Auto Loader tracks processed files automatically.

    @dlt.table(comment="Raw yellow taxi trips")
    def bronze_yellow_trips():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "/mnt/schema/yellow")
            .load("/mnt/raw/yellow_tripdata_*.parquet")
            .select("*",
                    "_metadata.file_path".alias("_source_file"),
                    "_metadata.file_modification_time".alias("_file_mod_time"))
        )
"""

import uuid
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

BATCH_ID = str(uuid.uuid4())
INGESTED_AT = datetime.now().isoformat()


def get_spark() -> SparkSession:
    """Create a local SparkSession."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("bronze_layer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def add_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """Add standard ingestion metadata columns.

    Databricks equivalent:
        Auto Loader adds _metadata automatically. You can also use:
        df.withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", input_file_name())
    """
    return (
        df
        .withColumn("_ingested_at", F.lit(INGESTED_AT).cast(StringType()))
        .withColumn("_source_file", F.lit(source_file).cast(StringType()))
        .withColumn("_batch_id", F.lit(BATCH_ID).cast(StringType()))
    )


def print_summary(name: str, df: DataFrame) -> None:
    """Print summary statistics for a dataset."""
    count = df.count()
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")
    print(f"  Rows:    {count:,}")
    print(f"  Columns: {df.columns}")
    # Show null counts for non-metadata columns
    non_meta_cols = [c for c in df.columns if not c.startswith("_")]
    if non_meta_cols:
        null_exprs = [
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in non_meta_cols
        ]
        null_counts = df.select(null_exprs).collect()[0]
        nulls = {c: null_counts[c] for c in non_meta_cols if null_counts[c] and null_counts[c] > 0}
        if nulls:
            print(f"  Nulls:")
            for col, cnt in nulls.items():
                print(f"    {col}: {cnt:,}")
    print(f"  Sample (first 3 rows):")
    df.show(3, truncate=40)


# ---------------------------------------------------------------------------
# 1. Yellow Taxi Trips (Parquet files)
# ---------------------------------------------------------------------------
def ingest_yellow_trips(spark: SparkSession) -> None:
    """Ingest yellow taxi trip parquet files.

    Databricks Auto Loader equivalent:
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load("s3://bucket/raw/yellow_tripdata_*.parquet")
    """
    print("\n>>> Ingesting yellow taxi trip data ...")
    raw_path = str(RAW_DIR / "yellow_tripdata_*.parquet")
    df = spark.read.parquet(raw_path)
    df = add_metadata(df, "yellow_tripdata_*.parquet")

    out_dir = str(BRONZE_DIR / "yellow_trips")
    df.write.mode("overwrite").parquet(out_dir)
    print_summary("Bronze: Yellow Taxi Trips", df)


# ---------------------------------------------------------------------------
# 2. Green Taxi Trips (Parquet files)
# ---------------------------------------------------------------------------
def ingest_green_trips(spark: SparkSession) -> None:
    """Ingest green taxi trip parquet files."""
    print("\n>>> Ingesting green taxi trip data ...")
    raw_path = str(RAW_DIR / "green_tripdata_*.parquet")
    df = spark.read.parquet(raw_path)
    df = add_metadata(df, "green_tripdata_*.parquet")

    out_dir = str(BRONZE_DIR / "green_trips")
    df.write.mode("overwrite").parquet(out_dir)
    print_summary("Bronze: Green Taxi Trips", df)


# ---------------------------------------------------------------------------
# 3. FHV / Rideshare Trips (Parquet files)
# ---------------------------------------------------------------------------
def ingest_fhv_trips(spark: SparkSession) -> None:
    """Ingest For-Hire Vehicle (Uber/Lyft) trip parquet files."""
    print("\n>>> Ingesting FHV/rideshare trip data ...")
    raw_path = str(RAW_DIR / "fhvhv_tripdata_*.parquet")
    df = spark.read.parquet(raw_path)
    df = add_metadata(df, "fhvhv_tripdata_*.parquet")

    out_dir = str(BRONZE_DIR / "fhv_trips")
    df.write.mode("overwrite").parquet(out_dir)
    print_summary("Bronze: FHV/Rideshare Trips", df)


# ---------------------------------------------------------------------------
# 4. Taxi Zone Lookup (CSV)
# ---------------------------------------------------------------------------
def ingest_taxi_zones(spark: SparkSession) -> None:
    """Ingest taxi zone lookup CSV -- read as strings to avoid type coercion.

    Databricks equivalent:
        spark.read.format("csv").option("header", "true")
            .option("inferSchema", "false")  # schema-on-read
            .load("/mnt/raw/taxi_zone_lookup.csv")
    """
    print("\n>>> Ingesting taxi_zone_lookup.csv ...")
    csv_path = str(RAW_DIR / "taxi_zone_lookup.csv")
    df = spark.read.csv(csv_path, header=True, inferSchema=False)
    df = add_metadata(df, "taxi_zone_lookup.csv")

    out_dir = str(BRONZE_DIR / "taxi_zones")
    df.write.mode("overwrite").parquet(out_dir)
    print_summary("Bronze: Taxi Zones", df)


# ---------------------------------------------------------------------------
# 5. Reference Data (vendors, rate_codes, payment_types, fhv_bases)
# ---------------------------------------------------------------------------
def ingest_reference_data(spark: SparkSession) -> None:
    """Ingest reference/dimension CSV files."""
    print("\n>>> Ingesting reference data CSVs ...")
    ref_files = ["vendors.csv", "rate_codes.csv", "payment_types.csv", "fhv_bases.csv"]

    for fname in ref_files:
        fpath = RAW_DIR / fname
        if not fpath.exists():
            print(f"    Skipping {fname} (not found)")
            continue

        table_name = fname.replace(".csv", "")
        print(f"  Reading {fname} ...")
        df = spark.read.csv(str(fpath), header=True, inferSchema=False)
        df = add_metadata(df, fname)

        out_dir = str(BRONZE_DIR / "reference" / table_name)
        df.write.mode("overwrite").parquet(out_dir)
        count = df.count()
        print(f"    {table_name}: {count:,} rows -> {out_dir}")


# ---------------------------------------------------------------------------
# 6. Weather Data (CSV)
# ---------------------------------------------------------------------------
def ingest_weather(spark: SparkSession) -> None:
    """Ingest NYC weather data."""
    print("\n>>> Ingesting nyc_weather_2023.csv ...")
    csv_path = str(RAW_DIR / "nyc_weather_2023.csv")
    df = spark.read.csv(csv_path, header=True, inferSchema=False)
    df = add_metadata(df, "nyc_weather_2023.csv")

    out_dir = str(BRONZE_DIR / "weather")
    df.write.mode("overwrite").parquet(out_dir)
    print_summary("Bronze: NYC Weather", df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("=" * 60)
    print("  BRONZE LAYER INGESTION -- NYC TAXI DATA")
    print(f"  Batch ID: {BATCH_ID}")
    print(f"  Ingested at: {INGESTED_AT}")
    print("=" * 60)
    print()
    print("  Databricks equivalent: This entire script would be a single")
    print("  DLT pipeline with Auto Loader reading from cloud storage.")
    print("  Each function below maps to a @dlt.table() definition.")

    spark = get_spark()

    try:
        ingest_yellow_trips(spark)
        ingest_green_trips(spark)
        ingest_fhv_trips(spark)
        ingest_taxi_zones(spark)
        ingest_reference_data(spark)
        ingest_weather(spark)

        print("\n" + "=" * 60)
        print("  BRONZE LAYER COMPLETE")
        print(f"  Output directory: {BRONZE_DIR}")
        print("=" * 60)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
