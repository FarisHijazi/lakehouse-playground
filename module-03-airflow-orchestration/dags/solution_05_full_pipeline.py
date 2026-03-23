"""
Solution 05: Full Listening Events Pipeline with TaskGroups
============================================================
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
  Raw JSONL -> Bronze Parquet -> Silver Parquet -> Gold Parquet (3 tables)

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
RAW_EVENTS_DIR = f"{DATA_DIR}/raw/listening_events"
BRONZE_EVENTS_DIR = f"{DATA_DIR}/processed/bronze/listening_events"
SILVER_EVENTS_DIR = f"{DATA_DIR}/processed/silver/listening_events"
GOLD_DIR = f"{DATA_DIR}/processed/gold"

VALID_EVENT_TYPES = {"play", "pause", "resume", "complete", "skip"}

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
    """Verify the raw JSONL file exists for the logical date."""
    source_path = f"{RAW_EVENTS_DIR}/events_{ds}.jsonl"
    if not os.path.exists(source_path):
        raise AirflowSkipException(
            f"No source file for {ds}: {source_path}. Skipping entire pipeline."
        )
    size_mb = os.path.getsize(source_path) / (1024 * 1024)
    print(f"Source file found: {source_path} ({size_mb:.2f} MB)")


def ingest_to_bronze(ds: str, ti, **context):
    """Read raw JSONL and write Bronze Parquet."""
    import pandas as pd

    source_path = f"{RAW_EVENTS_DIR}/events_{ds}.jsonl"
    df = pd.read_json(source_path, lines=True)
    print(f"Read {len(df)} raw records")

    output_dir = f"{BRONZE_EVENTS_DIR}/date={ds}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/events.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Bronze output: {output_path} ({len(df)} rows)")

    ti.xcom_push(key="bronze_row_count", value=len(df))


# ===========================================================================
# SILVER LAYER TASKS
# ===========================================================================

def clean_and_deduplicate(ds: str, ti, **context):
    """Clean Bronze data: deduplicate, validate, add metadata."""
    import pandas as pd

    bronze_path = f"{BRONZE_EVENTS_DIR}/date={ds}/events.parquet"
    df = pd.read_parquet(bronze_path)
    rows_in = len(df)

    # Deduplicate on event_id
    df = df.drop_duplicates(subset=["event_id"], keep="first")
    dupes = rows_in - len(df)

    # Drop null required fields
    before = len(df)
    df = df.dropna(subset=["user_id", "episode_id"])
    nulls = before - len(df)

    # Clamp negative listened_seconds
    if "listened_seconds" in df.columns:
        negatives = int((df["listened_seconds"] < 0).sum())
        df.loc[df["listened_seconds"] < 0, "listened_seconds"] = 0
    else:
        negatives = 0

    # Filter valid event types
    if "event_type" in df.columns:
        before = len(df)
        df = df[df["event_type"].isin(VALID_EVENT_TYPES)]
        invalid_types = before - len(df)
    else:
        invalid_types = 0

    # Add processing timestamp
    df["processed_at"] = pd.Timestamp.now(tz="UTC")

    rows_out = len(df)
    print(f"Silver cleaning: {rows_in} -> {rows_out} rows")
    print(f"  Duplicates: {dupes}, Nulls: {nulls}, "
          f"Negatives: {negatives}, Invalid types: {invalid_types}")

    # Write Silver output
    output_dir = f"{SILVER_EVENTS_DIR}/date={ds}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/events.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)

    ti.xcom_push(key="silver_row_count", value=rows_out)
    ti.xcom_push(key="rows_dropped", value=rows_in - rows_out)


# ===========================================================================
# GOLD LAYER TASKS
# ===========================================================================

def _read_silver(ds: str):
    """Helper to read Silver data."""
    import pandas as pd
    silver_path = f"{SILVER_EVENTS_DIR}/date={ds}/events.parquet"
    return pd.read_parquet(silver_path)


def _write_gold(df, table_name: str, ds: str):
    """Helper to write Gold data."""
    output_dir = f"{GOLD_DIR}/{table_name}/date={ds}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/metrics.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Gold [{table_name}]: {len(df)} rows -> {output_path}")
    return len(df)


def build_episode_metrics(ds: str, ti, **context):
    """Aggregate by episode: listens, listeners, seconds, completion rate."""
    import pandas as pd
    df = _read_silver(ds)

    metrics = df.groupby("episode_id").agg(
        total_listens=("event_id", "count"),
        unique_listeners=("user_id", "nunique"),
        total_seconds=("listened_seconds", "sum"),
        avg_listen_seconds=("listened_seconds", "mean"),
    ).reset_index()

    completion = (
        df.groupby("episode_id")["event_type"]
        .apply(lambda x: (x == "complete").sum() / len(x))
        .reset_index()
        .rename(columns={"event_type": "completion_rate"})
    )
    metrics = metrics.merge(completion, on="episode_id", how="left")
    metrics["avg_listen_seconds"] = metrics["avg_listen_seconds"].round(1)
    metrics["completion_rate"] = metrics["completion_rate"].round(4)

    count = _write_gold(metrics, "daily_episode_metrics", ds)
    ti.xcom_push(key="gold_episode_count", value=count)


def build_platform_metrics(ds: str, ti, **context):
    """Aggregate by platform: listens, users, seconds."""
    df = _read_silver(ds)

    metrics = df.groupby("platform").agg(
        total_listens=("event_id", "count"),
        unique_users=("user_id", "nunique"),
        total_seconds=("listened_seconds", "sum"),
    ).reset_index()

    count = _write_gold(metrics, "daily_platform_metrics", ds)
    ti.xcom_push(key="gold_platform_count", value=count)


def build_country_metrics(ds: str, ti, **context):
    """Aggregate by country: listens, users, seconds."""
    df = _read_silver(ds)

    metrics = df.groupby("country").agg(
        total_listens=("event_id", "count"),
        unique_users=("user_id", "nunique"),
        total_seconds=("listened_seconds", "sum"),
    ).reset_index()

    count = _write_gold(metrics, "daily_country_metrics", ds)
    ti.xcom_push(key="gold_country_count", value=count)


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
    gold_episode = ti.xcom_pull(
        task_ids="gold_layer.build_episode_metrics", key="gold_episode_count"
    )
    gold_platform = ti.xcom_pull(
        task_ids="gold_layer.build_platform_metrics", key="gold_platform_count"
    )
    gold_country = ti.xcom_pull(
        task_ids="gold_layer.build_country_metrics", key="gold_country_count"
    )

    print("=" * 60)
    print(f"  PIPELINE SUMMARY: {ds}")
    print("=" * 60)
    print(f"  Bronze (raw -> parquet)    : {bronze_count:,} rows")
    print(f"  Silver (cleaned)          : {silver_count:,} rows")
    print(f"  Rows dropped in cleaning  : {rows_dropped:,}")
    if bronze_count and bronze_count > 0:
        retention = (silver_count / bronze_count) * 100
        print(f"  Data retention rate       : {retention:.1f}%")
    print(f"  Gold tables produced:")
    print(f"    - Episode metrics       : {gold_episode} rows")
    print(f"    - Platform metrics      : {gold_platform} rows")
    print(f"    - Country metrics       : {gold_country} rows")
    print("=" * 60)


# ===========================================================================
# DAG DEFINITION
# ===========================================================================
with DAG(
    dag_id="full_listening_pipeline",
    description=(
        "End-to-end pipeline: Raw JSONL -> Bronze -> Silver -> Gold, "
        "organized with TaskGroups"
    ),
    default_args=default_args,
    start_date=datetime(2018, 1, 10),
    schedule="@daily",
    catchup=False,
    tags=["module-03", "pipeline", "medallion"],
) as dag:

    # -----------------------------------------------------------------------
    # Bronze Layer TaskGroup
    # -----------------------------------------------------------------------
    # TaskGroups appear as collapsible boxes in the Airflow Graph view.
    # They help organize complex DAGs without creating separate DAG files.
    # -----------------------------------------------------------------------
    with TaskGroup("bronze_layer", tooltip="Ingest raw JSONL to Bronze Parquet") as bronze:
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
        ep_metrics = PythonOperator(
            task_id="build_episode_metrics",
            python_callable=build_episode_metrics,
        )
        plat_metrics = PythonOperator(
            task_id="build_platform_metrics",
            python_callable=build_platform_metrics,
        )
        country_metrics = PythonOperator(
            task_id="build_country_metrics",
            python_callable=build_country_metrics,
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
