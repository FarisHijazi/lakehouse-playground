"""
Solution 03: Bronze to Silver (Clean, Deduplicate, Validate)
=============================================================
This DAG reads Bronze Parquet data, applies data quality rules, and writes
clean data to the Silver layer.

Key patterns demonstrated:
  - Data cleaning: deduplication, null handling, value validation
  - Quality metrics: tracking how many rows were dropped and why
  - XCom for passing quality reports between tasks
  - Separation of concerns: check -> clean -> write -> report

Cleaning rules applied:
  1. Drop duplicate event_id values (keep first occurrence)
  2. Drop rows where user_id or episode_id is null
  3. Clamp negative listened_seconds to 0
  4. Drop rows with unknown event_type values
  5. Add processed_at timestamp for lineage tracking

Data flow:
  /opt/airflow/data/processed/bronze/listening_events/date=YYYY-MM-DD/events.parquet
    -->
  /opt/airflow/data/processed/silver/listening_events/date=YYYY-MM-DD/events.parquet

Run as a standalone script to verify syntax:
    python solution_03_bronze_to_silver.py
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
BRONZE_EVENTS_DIR = f"{DATA_DIR}/processed/bronze/listening_events"
SILVER_EVENTS_DIR = f"{DATA_DIR}/processed/silver/listening_events"

VALID_EVENT_TYPES = {"play", "pause", "resume", "complete", "skip"}

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
def check_bronze_data(ds: str, **context):
    """
    Verify that the Bronze Parquet file exists for the given date.
    Skip downstream tasks if no Bronze data is available.
    """
    bronze_path = f"{BRONZE_EVENTS_DIR}/date={ds}/events.parquet"
    if not os.path.exists(bronze_path):
        raise AirflowSkipException(
            f"Bronze data not found: {bronze_path}. "
            f"Run the ingestion DAG for {ds} first."
        )
    print(f"Bronze data found: {bronze_path}")


def clean_and_deduplicate(ds: str, ti, **context):
    """
    Read Bronze data, apply cleaning rules, and write to Silver.

    This function demonstrates a common pattern in data engineering:
    track every transformation step so you can report on data quality.
    We count rows removed at each stage and push these metrics to XComs.

    Cleaning rules:
      1. Deduplicate on event_id
      2. Drop nulls in required fields (user_id, episode_id)
      3. Clamp negative listened_seconds to 0
      4. Filter to known event_type values
      5. Add processed_at timestamp
    """
    import pandas as pd

    bronze_path = f"{BRONZE_EVENTS_DIR}/date={ds}/events.parquet"
    print(f"Reading Bronze data: {bronze_path}")
    df = pd.read_parquet(bronze_path)
    total_rows_in = len(df)
    print(f"Bronze rows: {total_rows_in}")

    # --- Step 1: Deduplicate on event_id ---
    # In real streaming systems, duplicate events are common due to at-least-once
    # delivery guarantees. We keep the first occurrence.
    rows_before = len(df)
    df = df.drop_duplicates(subset=["event_id"], keep="first")
    duplicates_removed = rows_before - len(df)
    print(f"Step 1 - Duplicates removed: {duplicates_removed}")

    # --- Step 2: Drop rows with null required fields ---
    # user_id and episode_id are required for any meaningful analysis.
    rows_before = len(df)
    df = df.dropna(subset=["user_id", "episode_id"])
    nulls_dropped = rows_before - len(df)
    print(f"Step 2 - Null user_id/episode_id dropped: {nulls_dropped}")

    # --- Step 3: Clamp negative listened_seconds ---
    # Negative values are data errors. We clamp to 0 rather than dropping
    # the row, since the event itself is still valid.
    if "listened_seconds" in df.columns:
        negative_count = (df["listened_seconds"] < 0).sum()
        df.loc[df["listened_seconds"] < 0, "listened_seconds"] = 0
        print(f"Step 3 - Negative listened_seconds clamped: {negative_count}")
    else:
        negative_count = 0

    # --- Step 4: Filter to valid event types ---
    # Unknown event types indicate upstream schema changes or data corruption.
    if "event_type" in df.columns:
        rows_before = len(df)
        invalid_types = df[~df["event_type"].isin(VALID_EVENT_TYPES)]["event_type"].unique()
        df = df[df["event_type"].isin(VALID_EVENT_TYPES)]
        invalid_type_count = rows_before - len(df)
        if len(invalid_types) > 0:
            print(f"Step 4 - Invalid event types found: {list(invalid_types)}")
        print(f"Step 4 - Invalid event_type rows dropped: {invalid_type_count}")
    else:
        invalid_type_count = 0

    # --- Step 5: Add processed_at timestamp ---
    # This column records when the data was processed, useful for lineage
    # and debugging. We use the current wall-clock time (not execution_date)
    # because this represents processing time, not data time.
    df["processed_at"] = pd.Timestamp.now(tz="UTC")

    total_rows_out = len(df)
    print(f"Silver rows: {total_rows_out} (dropped {total_rows_in - total_rows_out} total)")

    # --- Write Silver output ---
    output_dir = f"{SILVER_EVENTS_DIR}/date={ds}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/events.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Wrote: {output_path}")

    # Push quality metrics to XComs for the reporting task
    quality_metrics = {
        "total_rows_in": int(total_rows_in),
        "total_rows_out": int(total_rows_out),
        "duplicates_removed": int(duplicates_removed),
        "nulls_dropped": int(nulls_dropped),
        "negative_values_clamped": int(negative_count),
        "invalid_types_dropped": int(invalid_type_count),
    }
    ti.xcom_push(key="quality_metrics", value=quality_metrics)
    ti.xcom_push(key="output_path", value=output_path)


def log_quality_metrics(ti, ds: str, **context):
    """
    Pull and display quality metrics from the cleaning step.

    In production, you would send these metrics to a monitoring system
    and potentially trigger alerts if the drop rate exceeds a threshold
    (e.g., "more than 10% of rows dropped -> alert the on-call engineer").
    """
    metrics = ti.xcom_pull(
        task_ids="clean_and_deduplicate", key="quality_metrics"
    )
    output_path = ti.xcom_pull(
        task_ids="clean_and_deduplicate", key="output_path"
    )

    if metrics is None:
        print("No quality metrics available (task may have been skipped).")
        return

    total_in = metrics["total_rows_in"]
    total_out = metrics["total_rows_out"]
    drop_rate = ((total_in - total_out) / total_in * 100) if total_in > 0 else 0

    print("=" * 60)
    print(f"  Data Quality Report for {ds}")
    print("=" * 60)
    print(f"  Rows in (Bronze)          : {total_in:,}")
    print(f"  Rows out (Silver)         : {total_out:,}")
    print(f"  Overall drop rate         : {drop_rate:.1f}%")
    print(f"  ---")
    print(f"  Duplicates removed        : {metrics['duplicates_removed']:,}")
    print(f"  Null required fields      : {metrics['nulls_dropped']:,}")
    print(f"  Negative values clamped   : {metrics['negative_values_clamped']:,}")
    print(f"  Invalid event types       : {metrics['invalid_types_dropped']:,}")
    print(f"  ---")
    print(f"  Output file               : {output_path}")
    print("=" * 60)

    # Example: alert if drop rate is too high
    if drop_rate > 20:
        print(
            f"WARNING: Drop rate of {drop_rate:.1f}% exceeds 20% threshold. "
            f"Investigate data quality for {ds}."
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="bronze_to_silver_events",
    description="Clean and deduplicate Bronze listening events into the Silver layer",
    default_args=default_args,
    start_date=datetime(2018, 1, 10),
    schedule="@daily",
    catchup=False,
    tags=["module-03", "silver", "cleaning"],
) as dag:

    check = PythonOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_data,
    )

    clean = PythonOperator(
        task_id="clean_and_deduplicate",
        python_callable=clean_and_deduplicate,
    )

    report = PythonOperator(
        task_id="log_quality_metrics",
        python_callable=log_quality_metrics,
    )

    # Linear dependency chain
    check >> clean >> report


# ---------------------------------------------------------------------------
# Standalone execution for syntax checking
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    dag.test()
