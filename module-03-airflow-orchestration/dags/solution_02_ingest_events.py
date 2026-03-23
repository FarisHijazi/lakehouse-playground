"""
Solution 02: Ingest Listening Events (Raw JSONL -> Bronze Parquet)
==================================================================
This DAG reads raw JSONL listening-event files and converts them to Parquet
in the Bronze layer, partitioned by date.

Key patterns demonstrated:
  - Using {{ ds }} (logical date) to build file paths -> idempotent processing
  - Partitioning output by date for efficient downstream queries
  - Passing metadata (row counts) between tasks via XComs
  - Graceful handling of missing source files with AirflowSkipException
  - Overwriting output to guarantee idempotency (re-running produces same result)

Data flow:
  /opt/airflow/data/raw/listening_events/events_YYYY-MM-DD.jsonl
    -->
  /opt/airflow/data/processed/bronze/listening_events/date=YYYY-MM-DD/events.parquet

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
RAW_EVENTS_DIR = f"{DATA_DIR}/raw/listening_events"
BRONZE_EVENTS_DIR = f"{DATA_DIR}/processed/bronze/listening_events"

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
    Verify that the raw JSONL file exists for the given logical date.

    If the file does not exist, we raise AirflowSkipException so that
    downstream tasks are skipped rather than marked as failed. This is
    the recommended pattern for handling expected gaps in data (weekends,
    holidays, etc.).

    Args:
        ds: The logical date string (YYYY-MM-DD), injected by Airflow.
    """
    source_path = f"{RAW_EVENTS_DIR}/events_{ds}.jsonl"
    if not os.path.exists(source_path):
        raise AirflowSkipException(
            f"Source file not found: {source_path}. "
            f"No listening events for {ds}. Skipping."
        )
    # Report file size for observability
    size_mb = os.path.getsize(source_path) / (1024 * 1024)
    print(f"Source file found: {source_path} ({size_mb:.2f} MB)")
    return source_path


def ingest_to_bronze(ds: str, ti, **context):
    """
    Read the raw JSONL file and write it as a Parquet file in the Bronze layer.

    Bronze layer convention: data is stored as-is from the source, with no
    transformations other than format conversion. We add a partition column
    (date=YYYY-MM-DD) in the directory structure for efficient querying.

    Idempotency: We overwrite the output file if it already exists, so
    re-running this task for the same date produces identical output.

    Args:
        ds: The logical date string (YYYY-MM-DD), injected by Airflow.
        ti: The TaskInstance object, used for XCom push.
    """
    # Import heavy libraries inside the function to keep the DAG file
    # lightweight at parse time. This is an Airflow best practice.
    import pandas as pd

    source_path = f"{RAW_EVENTS_DIR}/events_{ds}.jsonl"

    # Read JSONL (one JSON object per line)
    print(f"Reading: {source_path}")
    df = pd.read_json(source_path, lines=True)
    print(f"Read {len(df)} records with columns: {list(df.columns)}")

    # Create output directory (partitioned by date)
    output_dir = f"{BRONZE_EVENTS_DIR}/date={ds}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/events.parquet"

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
    print(f"  Ingestion Summary for {ds}")
    print("=" * 60)
    print(f"  Records ingested : {row_count}")
    print(f"  Output file      : {output_path}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ingest_listening_events",
    description="Ingest raw JSONL listening events into Bronze Parquet (partitioned by date)",
    default_args=default_args,
    start_date=datetime(2018, 1, 10),     # First available data date
    schedule="@daily",
    catchup=False,
    tags=["module-03", "bronze", "ingestion"],
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
