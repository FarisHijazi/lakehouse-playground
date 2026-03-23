#!/usr/bin/env python3
"""
Load real NYC TLC taxi data into Postgres.

This is the solution script for Module 01. It demonstrates:
  - Connecting to Postgres with psycopg2
  - Loading Parquet files (trip data) via pyarrow + COPY
  - Loading CSV reference data (zones, vendors, rates, weather)
  - Bulk loading with COPY via StringIO (10-50x faster than row-by-row INSERT)
  - Proper error handling, logging, and retry logic

The data loaded here is real, unmodified NYC TLC trip records — all the
messiness (null passenger counts, negative fares, impossible timestamps)
is authentic.

Usage:
    python load_data.py

Prerequisites:
    pip install psycopg2-binary pyarrow
"""

import csv
import io
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "user": os.getenv("POSTGRES_USER", "lakehouse"),
    "password": os.getenv("POSTGRES_PASSWORD", "lakehouse123"),
    "dbname": os.getenv("POSTGRES_DB", "nyc_taxi"),
}

# Path to raw data -- works from module-01-docker-postgres/ directory
RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------


def get_connection():
    """Create a database connection with retry logic."""
    max_retries = 5
    retry_delay = 2

    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            logger.info("Connected to Postgres at %s:%s/%s", DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries:
                logger.warning("Connection attempt %d/%d failed: %s. Retrying in %ds...", attempt, max_retries, e, retry_delay)
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect after %d attempts.", max_retries)
                raise


# ---------------------------------------------------------------------------
# Parquet → Postgres loader (COPY-based, fast)
# ---------------------------------------------------------------------------


def escape_copy_value(val) -> str:
    """Escape a value for Postgres COPY TEXT format."""
    if val is None:
        return "\\N"
    s = str(val)
    # Escape backslashes, tabs, and newlines
    s = s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
    return s


def load_parquet_to_table(conn, parquet_path: Path, table_name: str, column_mapping: dict):
    """
    Load a Parquet file into a Postgres table using COPY.

    Args:
        conn: psycopg2 connection
        parquet_path: Path to the .parquet file
        table_name: Target Postgres table
        column_mapping: Dict mapping parquet column names -> postgres column names.
                        Only columns in this mapping are loaded.
    """
    logger.info("Loading %s → %s", parquet_path.name, table_name)
    start = time.time()

    # Read parquet using pyarrow (memory-efficient: reads column-by-column)
    parquet_cols = list(column_mapping.keys())
    table = pq.read_table(parquet_path, columns=parquet_cols)
    n_rows = len(table)
    logger.info("  Read %s rows from parquet", f"{n_rows:,}")

    # Convert to pandas for easier row iteration
    df = table.to_pandas()

    # Rename columns to match Postgres schema
    df = df.rename(columns=column_mapping)
    pg_cols = list(column_mapping.values())

    # Build COPY buffer
    buffer = io.StringIO()
    for _, row in df.iterrows():
        line = "\t".join(escape_copy_value(row[col]) for col in pg_cols)
        buffer.write(line + "\n")

    buffer.seek(0)
    cols_str = ", ".join(pg_cols)

    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY {table_name} ({cols_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')",
            buffer,
        )

    conn.commit()
    elapsed = time.time() - start
    rate = n_rows / elapsed if elapsed > 0 else 0
    logger.info("  Loaded %s rows in %.1fs (%s rows/sec)", f"{n_rows:,}", elapsed, f"{rate:,.0f}")


# ---------------------------------------------------------------------------
# CSV loaders (for reference/dimension tables)
# ---------------------------------------------------------------------------


def load_csv_to_table(conn, csv_path: Path, table_name: str, label: str):
    """Load a CSV file into a Postgres table using COPY."""
    if not csv_path.exists():
        logger.warning("File not found, skipping: %s", csv_path)
        return

    logger.info("Loading %s from %s", label, csv_path.name)

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)

    # Build COPY buffer
    buffer = io.StringIO()
    for row in rows:
        line = "\t".join(escape_copy_value(v if v else None) for v in row)
        buffer.write(line + "\n")

    buffer.seek(0)
    cols_str = ", ".join(header)

    with conn.cursor() as cur:
        # Truncate + reload for dimension tables (they're small and idempotent)
        cur.execute(f"TRUNCATE TABLE {table_name} CASCADE")
        cur.copy_expert(
            f"COPY {table_name} ({cols_str}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')",
            buffer,
        )

    conn.commit()
    logger.info("  Loaded %d %s records", len(rows), label)


# ---------------------------------------------------------------------------
# Column mappings: Parquet column names → Postgres column names
# ---------------------------------------------------------------------------

YELLOW_COLUMNS = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
}

GREEN_COLUMNS = {
    "VendorID": "vendor_id",
    "lpep_pickup_datetime": "lpep_pickup_datetime",
    "lpep_dropoff_datetime": "lpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "ehail_fee": "ehail_fee",
    "trip_type": "trip_type",
}

FHV_COLUMNS = {
    "hvfhs_license_num": "hvfhs_license_num",
    "dispatching_base_num": "dispatching_base_num",
    "originating_base_num": "originating_base_num",
    "request_datetime": "request_datetime",
    "on_scene_datetime": "on_scene_datetime",
    "pickup_datetime": "pickup_datetime",
    "dropoff_datetime": "dropoff_datetime",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "trip_miles": "trip_miles",
    "trip_time": "trip_time",
    "base_passenger_fare": "base_passenger_fare",
    "tolls": "tolls",
    "bcf": "bcf",
    "sales_tax": "sales_tax",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "tips": "tips",
    "driver_pay": "driver_pay",
    "shared_request_flag": "shared_request_flag",
    "shared_match_flag": "shared_match_flag",
    "access_a_ride_flag": "access_a_ride_flag",
    "wav_request_flag": "wav_request_flag",
    "wav_match_flag": "wav_match_flag",
}

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_loads(conn):
    """Print row counts for all tables to verify successful loading."""
    tables = [
        "taxi_zones", "vendors", "rate_codes", "payment_types", "fhv_bases",
        "yellow_taxi_trips", "green_taxi_trips", "fhv_trips", "daily_weather",
    ]

    logger.info("=" * 60)
    logger.info("VERIFICATION: Row counts")
    logger.info("=" * 60)

    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608 -- table names are hardcoded
            count = cur.fetchone()[0]
            logger.info("  %-25s %12s rows", table, f"{count:,}")

    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    """Load all raw NYC taxi data into Postgres."""
    logger.info("Starting data load from %s", RAW_DATA_DIR)

    if not RAW_DATA_DIR.exists():
        logger.error("Raw data directory not found: %s", RAW_DATA_DIR)
        logger.error("Run 'python scripts/download_data.py' first to download the data.")
        sys.exit(1)

    conn = get_connection()

    try:
        # ----- Dimension tables first (small, fast) -----
        load_csv_to_table(conn, RAW_DATA_DIR / "taxi_zone_lookup.csv", "taxi_zones", "taxi zones")
        load_csv_to_table(conn, RAW_DATA_DIR / "vendors.csv", "vendors", "vendors")
        load_csv_to_table(conn, RAW_DATA_DIR / "rate_codes.csv", "rate_codes", "rate codes")
        load_csv_to_table(conn, RAW_DATA_DIR / "payment_types.csv", "payment_types", "payment types")
        load_csv_to_table(conn, RAW_DATA_DIR / "fhv_bases.csv", "fhv_bases", "FHV bases")
        load_csv_to_table(conn, RAW_DATA_DIR / "nyc_weather_2023.csv", "daily_weather", "daily weather")

        # ----- Trip data (large, takes minutes) -----
        # Yellow taxi
        for parquet_file in sorted(RAW_DATA_DIR.glob("yellow_tripdata_*.parquet")):
            load_parquet_to_table(conn, parquet_file, "yellow_taxi_trips", YELLOW_COLUMNS)

        # Green taxi
        for parquet_file in sorted(RAW_DATA_DIR.glob("green_tripdata_*.parquet")):
            load_parquet_to_table(conn, parquet_file, "green_taxi_trips", GREEN_COLUMNS)

        # For-hire vehicle (Uber/Lyft)
        for parquet_file in sorted(RAW_DATA_DIR.glob("fhvhv_tripdata_*.parquet")):
            load_parquet_to_table(conn, parquet_file, "fhv_trips", FHV_COLUMNS)

        verify_loads(conn)
        logger.info("All data loaded successfully.")

    except Exception:
        logger.exception("Data load failed")
        conn.rollback()
        sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
