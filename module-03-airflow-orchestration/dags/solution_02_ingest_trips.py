"""
Solution 02: Ingest Taxi Trips (Raw Parquet -> Bronze Parquet)
==============================================================
This DAG reads raw monthly NYC taxi trip Parquet files and writes them to the
Bronze layer, partitioned by year and month.

Key patterns demonstrated:
  - Using {{ ds }} (logical date) to derive year/month -> idempotent processing
  - Partitioning output by year and month for efficient downstream queries
  - Passing metadata (row counts) between tasks via XComs
  - Graceful handling of missing source files with AirflowSkipException
  - Fan-out pattern: yellow and green trips ingested in parallel
  - Overwriting output to guarantee idempotency (re-running produces same result)

Data flow:
  /opt/airflow/data/raw/yellow_tripdata_YYYY-MM.parquet
    -->
  /opt/airflow/data/bronze/yellow_taxi_trips/year=YYYY/month=MM/trips.parquet

  /opt/airflow/data/raw/green_tripdata_YYYY-MM.parquet
    -->
  /opt/airflow/data/bronze/green_taxi_trips/year=YYYY/month=MM/trips.parquet

Run as a standalone script to verify syntax:
    python solution_02_ingest_trips.py
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
BRONZE_DIR = f"{DATA_DIR}/bronze"

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
# Helper: extract year and month from the logical date string
# ---------------------------------------------------------------------------
def _year_month(ds: str):
    """
    Extract year and month from the Airflow logical date string.
    ds is in YYYY-MM-DD format, so we parse the first 7 characters
    for the file name and split for partition values.
    """
    year = ds[:4]
    month = ds[5:7]
    year_month = ds[:7]  # e.g., "2023-01"
    return year, month, year_month


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def check_source_files(ds: str, **context):
    """
    Verify that raw Parquet files exist for the given logical month.

    We check for both yellow and green taxi files. If neither exists, we
    skip the entire pipeline. If only one exists, we note which is missing
    and let the downstream tasks handle it individually.

    Args:
        ds: The logical date string (YYYY-MM-DD), injected by Airflow.
    """
    _, _, year_month = _year_month(ds)

    yellow_path = f"{RAW_DIR}/yellow_tripdata_{year_month}.parquet"
    green_path = f"{RAW_DIR}/green_tripdata_{year_month}.parquet"

    yellow_exists = os.path.exists(yellow_path)
    green_exists = os.path.exists(green_path)

    if not yellow_exists and not green_exists:
        raise AirflowSkipException(
            f"No source files found for {year_month}. "
            f"Checked: {yellow_path}, {green_path}. Skipping."
        )

    if yellow_exists:
        size_mb = os.path.getsize(yellow_path) / (1024 * 1024)
        print(f"Yellow taxi file found: {yellow_path} ({size_mb:.1f} MB)")
    else:
        print(f"Yellow taxi file NOT found: {yellow_path}")

    if green_exists:
        size_mb = os.path.getsize(green_path) / (1024 * 1024)
        print(f"Green taxi file found: {green_path} ({size_mb:.1f} MB)")
    else:
        print(f"Green taxi file NOT found: {green_path}")


def ingest_yellow_trips(ds: str, ti, **context):
    """
    Read the raw yellow taxi Parquet file and write it to the Bronze layer.

    Bronze layer convention: data is stored as-is from the source, with no
    transformations other than repartitioning. We organize by year/month
    for efficient downstream querying.

    Idempotency: We overwrite the output file if it already exists, so
    re-running this task for the same month produces identical output.

    Args:
        ds: The logical date string (YYYY-MM-DD), injected by Airflow.
        ti: The TaskInstance object, used for XCom push.
    """
    # Import heavy libraries inside the function to keep the DAG file
    # lightweight at parse time. This is an Airflow best practice.
    import pandas as pd

    year, month, year_month = _year_month(ds)
    source_path = f"{RAW_DIR}/yellow_tripdata_{year_month}.parquet"

    if not os.path.exists(source_path):
        raise AirflowSkipException(
            f"Yellow taxi file not found: {source_path}. Skipping."
        )

    # Read the raw Parquet file
    print(f"Reading: {source_path}")
    df = pd.read_parquet(source_path)
    print(f"Read {len(df):,} yellow taxi records with columns: {list(df.columns)}")

    # Add a taxi_type column for downstream convenience
    df["taxi_type"] = "yellow"

    # Create output directory (partitioned by year/month)
    output_dir = f"{BRONZE_DIR}/yellow_taxi_trips/year={year}/month={month}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/trips.parquet"

    # Write to Parquet with Snappy compression (default and fastest option)
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    output_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Wrote: {output_path} ({output_size_mb:.1f} MB)")

    # Push metadata via XCom so downstream tasks can use it.
    # IMPORTANT: Only push small values (counts, paths, status strings).
    # Never push DataFrames or large objects through XComs.
    ti.xcom_push(key="yellow_row_count", value=len(df))
    ti.xcom_push(key="yellow_output_path", value=output_path)


def ingest_green_trips(ds: str, ti, **context):
    """
    Read the raw green taxi Parquet file and write it to the Bronze layer.

    Green taxis operate primarily in the outer boroughs and have a slightly
    different schema than yellow taxis (e.g., ehail_fee, trip_type columns).

    Args:
        ds: The logical date string (YYYY-MM-DD), injected by Airflow.
        ti: The TaskInstance object, used for XCom push.
    """
    import pandas as pd

    year, month, year_month = _year_month(ds)
    source_path = f"{RAW_DIR}/green_tripdata_{year_month}.parquet"

    if not os.path.exists(source_path):
        raise AirflowSkipException(
            f"Green taxi file not found: {source_path}. Skipping."
        )

    print(f"Reading: {source_path}")
    df = pd.read_parquet(source_path)
    print(f"Read {len(df):,} green taxi records with columns: {list(df.columns)}")

    # Add a taxi_type column for downstream convenience
    df["taxi_type"] = "green"

    output_dir = f"{BRONZE_DIR}/green_taxi_trips/year={year}/month={month}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/trips.parquet"

    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    output_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Wrote: {output_path} ({output_size_mb:.1f} MB)")

    ti.xcom_push(key="green_row_count", value=len(df))
    ti.xcom_push(key="green_output_path", value=output_path)


def log_record_counts(ti, ds: str, **context):
    """
    Pull the row counts from XComs and log a summary.

    This task demonstrates XCom pull and provides observability into how
    much data was processed. In production, you might send these metrics
    to a monitoring system (Datadog, Prometheus, etc.).

    Args:
        ti: The TaskInstance object, used for XCom pull.
        ds: The logical date string (YYYY-MM-DD).
    """
    _, _, year_month = _year_month(ds)

    yellow_count = ti.xcom_pull(task_ids="ingest_yellow_trips", key="yellow_row_count")
    green_count = ti.xcom_pull(task_ids="ingest_green_trips", key="green_row_count")
    yellow_path = ti.xcom_pull(task_ids="ingest_yellow_trips", key="yellow_output_path")
    green_path = ti.xcom_pull(task_ids="ingest_green_trips", key="green_output_path")

    print("=" * 60)
    print(f"  Ingestion Summary for {year_month}")
    print("=" * 60)
    if yellow_count is not None:
        print(f"  Yellow taxi records : {yellow_count:,}")
        print(f"  Yellow output      : {yellow_path}")
    else:
        print(f"  Yellow taxi records : SKIPPED (no source file)")
    if green_count is not None:
        print(f"  Green taxi records  : {green_count:,}")
        print(f"  Green output       : {green_path}")
    else:
        print(f"  Green taxi records  : SKIPPED (no source file)")
    total = (yellow_count or 0) + (green_count or 0)
    print(f"  Total records       : {total:,}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ingest_taxi_trips",
    description="Ingest raw monthly taxi trip Parquet files into Bronze layer (partitioned by year/month)",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["module-03", "bronze", "ingestion"],
) as dag:

    check = PythonOperator(
        task_id="check_source_files",
        python_callable=check_source_files,
    )

    yellow = PythonOperator(
        task_id="ingest_yellow_trips",
        python_callable=ingest_yellow_trips,
    )

    green = PythonOperator(
        task_id="ingest_green_trips",
        python_callable=ingest_green_trips,
    )

    log_counts = PythonOperator(
        task_id="log_record_counts",
        python_callable=log_record_counts,
        trigger_rule="none_failed",
    )

    # Dependencies: check files -> ingest yellow & green in parallel -> log results
    #
    #                 +--> ingest_yellow_trips --+
    #   check -----+                             +--> log_record_counts
    #                 +--> ingest_green_trips  --+
    check >> [yellow, green] >> log_counts


# ---------------------------------------------------------------------------
# Standalone execution for syntax checking
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    dag.test()
