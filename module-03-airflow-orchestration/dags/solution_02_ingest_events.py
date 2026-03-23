"""
Solution 02: Ingest Yellow Taxi Trips (Raw Parquet -> Bronze Parquet)
=====================================================================
This DAG reads raw yellow taxi trip Parquet files and converts them to
partitioned Bronze-layer Parquet, organized by year and month.

Key patterns demonstrated:
  - Using {{ ds }} (logical date) to build file paths -> idempotent processing
  - Partitioning output by year/month for efficient downstream queries
  - Passing metadata (row counts) between tasks via XComs
  - Graceful handling of missing source files with AirflowSkipException
  - Overwriting output to guarantee idempotency (re-running produces same result)

Data flow:
  /opt/airflow/data/raw/yellow_tripdata_YYYY-MM.parquet
    -->
  /opt/airflow/data/processed/bronze/yellow_taxi_trips/year=YYYY/month=MM/trips.parquet

Run as a standalone script to verify syntax:
    python solution_02_ingest_events.py
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATA_DIR = "/opt/airflow/data"
RAW_DIR = f"{DATA_DIR}/raw"
BRONZE_DIR = f"{DATA_DIR}/processed/bronze/yellow_taxi_trips"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def check_source_file(ds: str, **context):
    """
    Verify that the raw Parquet file exists for the given logical date's month.

    If the file does not exist, we raise AirflowSkipException so that
    downstream tasks are skipped rather than marked as failed. This is
    the recommended pattern for handling expected gaps in data (e.g., we
    only have data for certain months).

    Args:
        ds: The logical date string (YYYY-MM-DD), injected by Airflow.
    """
    # Extract year-month from ds to match file naming convention
    year_month = ds[:7]  # "YYYY-MM"
    source_path = f"{RAW_DIR}/yellow_tripdata_{year_month}.parquet"
    if not os.path.exists(source_path):
        raise AirflowSkipException(
            f"Source file not found: {source_path}. "
            f"No yellow taxi data for {year_month}. Skipping."
        )
    # Report file size for observability
    size_mb = os.path.getsize(source_path) / (1024 * 1024)
    print(f"Source file found: {source_path} ({size_mb:.2f} MB)")
    return source_path


def ingest_to_bronze(ds: str, ti, **context):
    """
    Read the raw Parquet file and write it to the Bronze layer, partitioned
    by year and month.

    Bronze layer convention: data is stored as-is from the source, with no
    transformations other than reorganizing into the medallion directory
    structure. We partition by year/month for efficient querying.

    Idempotency: We overwrite the output file if it already exists, so
    re-running this task for the same month produces identical output.

    Args:
        ds: The logical date string (YYYY-MM-DD), injected by Airflow.
        ti: The TaskInstance object, used for XCom push.
    """
    # Import heavy libraries inside the function to keep the DAG file
    # lightweight at parse time. This is an Airflow best practice.
    import pandas as pd

    year_month = ds[:7]
    year, month = year_month.split("-")
    source_path = f"{RAW_DIR}/yellow_tripdata_{year_month}.parquet"

    # Read raw Parquet
    print(f"Reading: {source_path}")
    df = pd.read_parquet(source_path)
    print(f"Read {len(df)} records with columns: {list(df.columns)}")

    # Create output directory (partitioned by year and month)
    output_dir = f"{BRONZE_DIR}/year={year}/month={month}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/trips.parquet"

    # Write to Parquet with Snappy compression (the default and fastest option)
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    output_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Wrote: {output_path} ({output_size_mb:.2f} MB)")

    # Push metadata via XCom so downstream tasks can use it.
    # IMPORTANT: Only push small values (counts, paths, status strings).
    # Never push DataFrames or large objects through XComs.
    ti.xcom_push(key="row_count", value=len(df))
    ti.xcom_push(key="output_path", value=output_path)

    return len(df)


def log_record_count(ti, ds: str, **context):
    """
    Pull the row count from XComs and log it.

    This task demonstrates XCom pull and provides observability into how
    much data was processed. In production, you might send this metric
    to a monitoring system (Datadog, Prometheus, etc.).

    Args:
        ti: The TaskInstance object, used for XCom pull.
        ds: The logical date string (YYYY-MM-DD).
    """
    row_count = ti.xcom_pull(task_ids="ingest_to_bronze", key="row_count")
    output_path = ti.xcom_pull(task_ids="ingest_to_bronze", key="output_path")

    print("=" * 60)
    print(f"  Ingestion Summary for {ds[:7]}")
    print("=" * 60)
    print(f"  Trips ingested   : {row_count}")
    print(f"  Output file      : {output_path}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ingest_yellow_taxi_trips",
    description="Ingest raw yellow taxi Parquet into Bronze layer (partitioned by year/month)",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),     # First available data month
    schedule="@monthly",
    catchup=False,
    tags=["module-03", "bronze", "ingestion", "yellow-taxi"],
) as dag:

    check = PythonOperator(
        task_id="check_source_file",
        python_callable=check_source_file,
    )

    ingest = PythonOperator(
        task_id="ingest_to_bronze",
        python_callable=ingest_to_bronze,
    )

    log_count = PythonOperator(
        task_id="log_record_count",
        python_callable=log_record_count,
    )

    # Dependencies: check file exists -> ingest -> log results
    check >> ingest >> log_count


# ---------------------------------------------------------------------------
# Standalone execution for syntax checking
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    dag.test()
