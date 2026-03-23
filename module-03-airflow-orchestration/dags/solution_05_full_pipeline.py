"""
Solution 05: Full Yellow Taxi Pipeline with TaskGroups
=======================================================
This DAG combines the entire medallion architecture pipeline into a single DAG,
using TaskGroups to organize the Bronze, Silver, and Gold stages.

Key patterns demonstrated:
  - TaskGroups: logical grouping of related tasks (collapsible in the UI)
  - End-to-end pipeline: ingest -> clean -> aggregate in one DAG
  - XCom for passing metrics across TaskGroup boundaries
  - Failure callbacks for production alerting
  - Summary task that reports on the entire pipeline run

When to use a single DAG vs. multiple DAGs:
  - Single DAG: simpler to manage, atomic success/failure, good for tightly
    coupled stages that always run together.
  - Multiple DAGs: better isolation, independent scheduling, reusable stages,
    preferred when stages may run at different frequencies.
  This solution demonstrates the single-DAG approach with TaskGroups for
  organization. The individual DAGs from solutions 02-04 show the multi-DAG
  approach.

Data flow:
  Raw Parquet -> Bronze Parquet -> Silver Parquet -> Gold Parquet (3 tables)

Run as a standalone script to verify syntax:
    python solution_05_full_pipeline.py
"""

import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATA_DIR = "/opt/airflow/data"
RAW_DIR = f"{DATA_DIR}/raw"
BRONZE_DIR = f"{DATA_DIR}/processed/bronze/yellow_taxi_trips"
SILVER_DIR = f"{DATA_DIR}/processed/silver/yellow_taxi_trips"
GOLD_DIR = f"{DATA_DIR}/processed/gold"

# Fare amount thresholds for data quality
MAX_FARE_AMOUNT = 500.0
MAX_TRIP_DISTANCE = 200.0  # miles

# ---------------------------------------------------------------------------
# Default args with production-ready settings
# ---------------------------------------------------------------------------


def task_failure_callback(context):
    """
    Called when any task in this DAG fails.

    In production, this function would send alerts via Slack, PagerDuty, or
    email. Here we print a structured alert message that could be picked up
    by a log aggregator.

    The context dict contains everything about the failed task:
      - task_instance: the TaskInstance object
      - execution_date: the logical date
      - exception: the exception that caused the failure
      - dag_run: the DagRun object
    """
    ti = context["task_instance"]
    exception = context.get("exception", "Unknown error")
    print("!" * 60)
    print(f"  TASK FAILURE ALERT")
    print(f"  DAG        : {ti.dag_id}")
    print(f"  Task       : {ti.task_id}")
    print(f"  Date       : {context['ds']}")
    print(f"  Try number : {ti.try_number}")
    print(f"  Exception  : {exception}")
    print("!" * 60)
    # In production: send_slack_alert(...), send_pagerduty_alert(...), etc.


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": task_failure_callback,
}


# ===========================================================================
# BRONZE LAYER TASKS
# ===========================================================================

def check_source_file(ds: str, **context):
    """Verify the raw Parquet file exists for the logical date's month."""
    year_month = ds[:7]
    source_path = f"{RAW_DIR}/yellow_tripdata_{year_month}.parquet"
    if not os.path.exists(source_path):
        raise AirflowSkipException(
            f"No source file for {year_month}: {source_path}. Skipping entire pipeline."
        )
    size_mb = os.path.getsize(source_path) / (1024 * 1024)
    print(f"Source file found: {source_path} ({size_mb:.2f} MB)")


def ingest_to_bronze(ds: str, ti, **context):
    """Read raw Parquet and write Bronze Parquet, partitioned by year/month."""
    import pandas as pd

    year_month = ds[:7]
    year, month = year_month.split("-")
    source_path = f"{RAW_DIR}/yellow_tripdata_{year_month}.parquet"
    df = pd.read_parquet(source_path)
    print(f"Read {len(df)} raw records")

    output_dir = f"{BRONZE_DIR}/year={year}/month={month}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/trips.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Bronze output: {output_path} ({len(df)} rows)")

    ti.xcom_push(key="bronze_row_count", value=len(df))


# ===========================================================================
# SILVER LAYER TASKS
# ===========================================================================

def clean_and_deduplicate(ds: str, ti, **context):
    """Clean Bronze data: deduplicate, validate, handle outliers, add metadata."""
    import pandas as pd

    year_month = ds[:7]
    year, month = year_month.split("-")
    bronze_path = f"{BRONZE_DIR}/year={year}/month={month}/trips.parquet"
    df = pd.read_parquet(bronze_path)
    rows_in = len(df)

    # Deduplicate on composite key (taxi data has no single unique ID)
    dedup_cols = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                  "PULocationID", "DOLocationID", "fare_amount"]
    df = df.drop_duplicates(subset=dedup_cols, keep="first")
    dupes = rows_in - len(df)

    # Drop rows with null required fields
    before = len(df)
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID"])
    nulls = before - len(df)

    # Clamp negative fare amounts to 0
    if "fare_amount" in df.columns:
        negatives = int((df["fare_amount"] < 0).sum())
        df.loc[df["fare_amount"] < 0, "fare_amount"] = 0
    else:
        negatives = 0

    # Cap extreme outliers
    outliers = 0
    if "fare_amount" in df.columns:
        extreme = df["fare_amount"] > MAX_FARE_AMOUNT
        outliers += int(extreme.sum())
        df.loc[extreme, "fare_amount"] = MAX_FARE_AMOUNT
    if "trip_distance" in df.columns:
        extreme = df["trip_distance"] > MAX_TRIP_DISTANCE
        outliers += int(extreme.sum())
        df.loc[extreme, "trip_distance"] = MAX_TRIP_DISTANCE

    # Add processing timestamp
    df["processed_at"] = pd.Timestamp.now(tz="UTC")

    rows_out = len(df)
    print(f"Silver cleaning: {rows_in} -> {rows_out} rows")
    print(f"  Duplicates: {dupes}, Nulls: {nulls}, "
          f"Negatives clamped: {negatives}, Outliers capped: {outliers}")

    # Write Silver output
    output_dir = f"{SILVER_DIR}/year={year}/month={month}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/trips.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)

    ti.xcom_push(key="silver_row_count", value=rows_out)
    ti.xcom_push(key="rows_dropped", value=rows_in - rows_out)


# ===========================================================================
# GOLD LAYER TASKS
# ===========================================================================

def _read_silver(ds: str):
    """Helper to read Silver data."""
    import pandas as pd
    year_month = ds[:7]
    year, month = year_month.split("-")
    silver_path = f"{SILVER_DIR}/year={year}/month={month}/trips.parquet"
    return pd.read_parquet(silver_path)


def _write_gold(df, table_name: str, ds: str):
    """Helper to write Gold data."""
    year_month = ds[:7]
    year, month = year_month.split("-")
    output_dir = f"{GOLD_DIR}/{table_name}/year={year}/month={month}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/metrics.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Gold [{table_name}]: {len(df)} rows -> {output_path}")
    return len(df)


def build_daily_trip_metrics(ds: str, ti, **context):
    """Aggregate by day: trip counts, revenue, average fare and distance."""
    import pandas as pd
    df = _read_silver(ds)

    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

    metrics = df.groupby("pickup_date").agg(
        total_trips=("VendorID", "count"),
        total_revenue=("total_amount", "sum"),
        avg_fare=("fare_amount", "mean"),
        avg_distance=("trip_distance", "mean"),
        avg_tip=("tip_amount", "mean"),
    ).reset_index()

    metrics["avg_fare"] = metrics["avg_fare"].round(2)
    metrics["avg_distance"] = metrics["avg_distance"].round(2)
    metrics["avg_tip"] = metrics["avg_tip"].round(2)
    metrics["total_revenue"] = metrics["total_revenue"].round(2)

    count = _write_gold(metrics, "daily_trip_metrics", ds)
    ti.xcom_push(key="gold_daily_count", value=count)


def build_zone_metrics(ds: str, ti, **context):
    """Aggregate by pickup zone: trip counts, revenue, avg fare."""
    df = _read_silver(ds)

    metrics = df.groupby("PULocationID").agg(
        total_trips=("VendorID", "count"),
        total_revenue=("total_amount", "sum"),
        avg_fare=("fare_amount", "mean"),
        avg_distance=("trip_distance", "mean"),
    ).reset_index()

    metrics = metrics.rename(columns={"PULocationID": "pu_location_id"})
    metrics["avg_fare"] = metrics["avg_fare"].round(2)
    metrics["avg_distance"] = metrics["avg_distance"].round(2)
    metrics["total_revenue"] = metrics["total_revenue"].round(2)

    count = _write_gold(metrics, "zone_trip_metrics", ds)
    ti.xcom_push(key="gold_zone_count", value=count)


def build_payment_metrics(ds: str, ti, **context):
    """Aggregate by payment type: trip counts, revenue, tip analysis."""
    df = _read_silver(ds)

    metrics = df.groupby("payment_type").agg(
        total_trips=("VendorID", "count"),
        total_revenue=("total_amount", "sum"),
        avg_fare=("fare_amount", "mean"),
        avg_tip=("tip_amount", "mean"),
    ).reset_index()

    metrics["avg_fare"] = metrics["avg_fare"].round(2)
    metrics["avg_tip"] = metrics["avg_tip"].round(2)
    metrics["total_revenue"] = metrics["total_revenue"].round(2)

    count = _write_gold(metrics, "payment_type_metrics", ds)
    ti.xcom_push(key="gold_payment_count", value=count)


# ===========================================================================
# SUMMARY TASK
# ===========================================================================

def pipeline_summary(ds: str, ti, **context):
    """
    Print a summary report of the entire pipeline run.

    This task pulls XCom values from tasks across all TaskGroups to give
    a unified view of data flow through the medallion layers.
    """
    # Pull metrics from each layer.
    # Note the task_ids include the TaskGroup prefix (e.g., "bronze_layer.ingest_to_bronze").
    bronze_count = ti.xcom_pull(
        task_ids="bronze_layer.ingest_to_bronze", key="bronze_row_count"
    )
    silver_count = ti.xcom_pull(
        task_ids="silver_layer.clean_and_deduplicate", key="silver_row_count"
    )
    rows_dropped = ti.xcom_pull(
        task_ids="silver_layer.clean_and_deduplicate", key="rows_dropped"
    )
    gold_daily = ti.xcom_pull(
        task_ids="gold_layer.build_daily_trip_metrics", key="gold_daily_count"
    )
    gold_zone = ti.xcom_pull(
        task_ids="gold_layer.build_zone_metrics", key="gold_zone_count"
    )
    gold_payment = ti.xcom_pull(
        task_ids="gold_layer.build_payment_metrics", key="gold_payment_count"
    )

    print("=" * 60)
    print(f"  PIPELINE SUMMARY: {ds[:7]}")
    print("=" * 60)
    print(f"  Bronze (raw -> parquet)    : {bronze_count:,} rows")
    print(f"  Silver (cleaned)          : {silver_count:,} rows")
    print(f"  Rows dropped in cleaning  : {rows_dropped:,}")
    if bronze_count and bronze_count > 0:
        retention = (silver_count / bronze_count) * 100
        print(f"  Data retention rate       : {retention:.1f}%")
    print(f"  Gold tables produced:")
    print(f"    - Daily trip metrics    : {gold_daily} rows")
    print(f"    - Zone metrics          : {gold_zone} rows")
    print(f"    - Payment type metrics  : {gold_payment} rows")
    print("=" * 60)


# ===========================================================================
# DAG DEFINITION
# ===========================================================================
with DAG(
    dag_id="full_yellow_taxi_pipeline",
    description=(
        "End-to-end pipeline: Raw Parquet -> Bronze -> Silver -> Gold, "
        "organized with TaskGroups"
    ),
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["module-03", "pipeline", "medallion", "yellow-taxi"],
) as dag:

    # -----------------------------------------------------------------------
    # Bronze Layer TaskGroup
    # -----------------------------------------------------------------------
    # TaskGroups appear as collapsible boxes in the Airflow Graph view.
    # They help organize complex DAGs without creating separate DAG files.
    # -----------------------------------------------------------------------
    with TaskGroup("bronze_layer", tooltip="Ingest raw Parquet to Bronze layer") as bronze:
        check_file = PythonOperator(
            task_id="check_source_file",
            python_callable=check_source_file,
        )
        ingest = PythonOperator(
            task_id="ingest_to_bronze",
            python_callable=ingest_to_bronze,
        )
        check_file >> ingest

    # -----------------------------------------------------------------------
    # Silver Layer TaskGroup
    # -----------------------------------------------------------------------
    with TaskGroup("silver_layer", tooltip="Clean, deduplicate, validate") as silver:
        clean = PythonOperator(
            task_id="clean_and_deduplicate",
            python_callable=clean_and_deduplicate,
        )

    # -----------------------------------------------------------------------
    # Gold Layer TaskGroup
    # -----------------------------------------------------------------------
    with TaskGroup("gold_layer", tooltip="Aggregate metrics for analytics") as gold:
        daily_metrics = PythonOperator(
            task_id="build_daily_trip_metrics",
            python_callable=build_daily_trip_metrics,
        )
        zone_metrics = PythonOperator(
            task_id="build_zone_metrics",
            python_callable=build_zone_metrics,
        )
        payment_metrics = PythonOperator(
            task_id="build_payment_metrics",
            python_callable=build_payment_metrics,
        )
        # All three run in parallel within the Gold TaskGroup

    # -----------------------------------------------------------------------
    # Summary Task
    # -----------------------------------------------------------------------
    summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
        # trigger_rule="none_failed" ensures this runs even if some Gold tasks
        # were skipped (e.g., due to missing data). Use "all_success" if you
        # want strict success checking.
        trigger_rule="none_failed",
    )

    # -----------------------------------------------------------------------
    # Cross-group dependencies
    # -----------------------------------------------------------------------
    # This creates the high-level flow:
    #   bronze_layer >> silver_layer >> gold_layer >> summary
    #
    # In the Graph view, each TaskGroup appears as a single box that can
    # be expanded to show internal tasks.
    # -----------------------------------------------------------------------
    bronze >> silver >> gold >> summary


# ---------------------------------------------------------------------------
# Standalone execution for syntax checking
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    dag.test()
