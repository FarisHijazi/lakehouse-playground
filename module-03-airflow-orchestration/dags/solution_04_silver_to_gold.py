"""
Solution 04: Silver to Gold (Aggregate Metrics)
=================================================
This DAG reads cleaned Silver data and produces aggregated Gold-layer tables
designed for analytical consumption.

Key patterns demonstrated:
  - Fan-out: one check task feeds multiple parallel aggregation tasks
  - Gold layer design: pre-aggregated tables optimized for specific queries
  - Each aggregation is independent -> they run in parallel for speed
  - EmptyOperator as a join point after fan-out

Gold tables produced:
  1. daily_episode_metrics  - Per-episode listening stats
  2. daily_platform_metrics - Per-platform usage stats
  3. daily_country_metrics  - Per-country usage stats

Data flow:
  /opt/airflow/data/processed/silver/listening_events/date=YYYY-MM-DD/events.parquet
    -->
  /opt/airflow/data/processed/gold/daily_episode_metrics/date=YYYY-MM-DD/metrics.parquet
  /opt/airflow/data/processed/gold/daily_platform_metrics/date=YYYY-MM-DD/metrics.parquet
  /opt/airflow/data/processed/gold/daily_country_metrics/date=YYYY-MM-DD/metrics.parquet

Run as a standalone script to verify syntax:
    python solution_04_silver_to_gold.py
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATA_DIR = "/opt/airflow/data"
SILVER_EVENTS_DIR = f"{DATA_DIR}/processed/silver/listening_events"
GOLD_DIR = f"{DATA_DIR}/processed/gold"

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
# Helper: read Silver data for a given date
# ---------------------------------------------------------------------------
def _read_silver_data(ds: str):
    """
    Read the Silver Parquet file for the given date.
    Returns a pandas DataFrame.

    This helper is called by multiple tasks. Factoring shared logic into
    a helper function (rather than duplicating it) keeps the code DRY.
    """
    import pandas as pd

    silver_path = f"{SILVER_EVENTS_DIR}/date={ds}/events.parquet"
    if not os.path.exists(silver_path):
        raise AirflowSkipException(
            f"Silver data not found: {silver_path}. "
            f"Run the bronze_to_silver DAG for {ds} first."
        )
    df = pd.read_parquet(silver_path)
    print(f"Read {len(df)} Silver rows from {silver_path}")
    return df


def _write_gold_table(df, table_name: str, ds: str):
    """
    Write a Gold table to the standard directory structure.
    Returns the output path.
    """
    output_dir = f"{GOLD_DIR}/{table_name}/date={ds}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/metrics.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Wrote {len(df)} rows to {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def check_silver_data(ds: str, **context):
    """Verify Silver data exists for the given date."""
    silver_path = f"{SILVER_EVENTS_DIR}/date={ds}/events.parquet"
    if not os.path.exists(silver_path):
        raise AirflowSkipException(
            f"Silver data not found: {silver_path}. "
            f"Run the bronze_to_silver DAG for {ds} first."
        )
    size_mb = os.path.getsize(silver_path) / (1024 * 1024)
    print(f"Silver data found: {silver_path} ({size_mb:.2f} MB)")


def build_episode_metrics(ds: str, ti, **context):
    """
    Aggregate listening events by episode.

    Produces:
      - episode_id: the episode identifier
      - total_listens: count of all events
      - unique_listeners: count of distinct user_ids
      - total_seconds: sum of listened_seconds
      - avg_listen_seconds: mean listened_seconds per event
      - completion_rate: fraction of events that are "complete"

    The completion_rate metric is particularly useful for content creators
    to understand which episodes hold listener attention.
    """
    import pandas as pd

    df = _read_silver_data(ds)

    metrics = df.groupby("episode_id").agg(
        total_listens=("event_id", "count"),
        unique_listeners=("user_id", "nunique"),
        total_seconds=("listened_seconds", "sum"),
        avg_listen_seconds=("listened_seconds", "mean"),
    ).reset_index()

    # Calculate completion rate: fraction of events with event_type == "complete"
    completion = (
        df.groupby("episode_id")["event_type"]
        .apply(lambda x: (x == "complete").sum() / len(x))
        .reset_index()
        .rename(columns={"event_type": "completion_rate"})
    )
    metrics = metrics.merge(completion, on="episode_id", how="left")

    # Round for readability
    metrics["avg_listen_seconds"] = metrics["avg_listen_seconds"].round(1)
    metrics["completion_rate"] = metrics["completion_rate"].round(4)

    output_path = _write_gold_table(metrics, "daily_episode_metrics", ds)
    ti.xcom_push(key="episode_metrics_count", value=len(metrics))
    print(f"Top 5 episodes by total listens:")
    print(metrics.nlargest(5, "total_listens").to_string(index=False))


def build_platform_metrics(ds: str, ti, **context):
    """
    Aggregate listening events by platform (ios, android, web, etc.).

    Produces:
      - platform: the listening platform
      - total_listens: count of all events
      - unique_users: count of distinct user_ids
      - total_seconds: sum of listened_seconds

    This table helps product teams understand platform-specific usage patterns
    and prioritize engineering investment.
    """
    df = _read_silver_data(ds)

    metrics = df.groupby("platform").agg(
        total_listens=("event_id", "count"),
        unique_users=("user_id", "nunique"),
        total_seconds=("listened_seconds", "sum"),
    ).reset_index()

    output_path = _write_gold_table(metrics, "daily_platform_metrics", ds)
    ti.xcom_push(key="platform_metrics_count", value=len(metrics))
    print(f"Platform breakdown:")
    print(metrics.to_string(index=False))


def build_country_metrics(ds: str, ti, **context):
    """
    Aggregate listening events by country.

    Produces:
      - country: the country code
      - total_listens: count of all events
      - unique_users: count of distinct user_ids
      - total_seconds: sum of listened_seconds

    This table supports geographic expansion decisions and content
    localization strategy.
    """
    df = _read_silver_data(ds)

    metrics = df.groupby("country").agg(
        total_listens=("event_id", "count"),
        unique_users=("user_id", "nunique"),
        total_seconds=("listened_seconds", "sum"),
    ).reset_index()

    output_path = _write_gold_table(metrics, "daily_country_metrics", ds)
    ti.xcom_push(key="country_metrics_count", value=len(metrics))
    print(f"Top 5 countries by total listens:")
    print(metrics.nlargest(5, "total_listens").to_string(index=False))


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="silver_to_gold_metrics",
    description="Aggregate Silver listening events into Gold metric tables",
    default_args=default_args,
    start_date=datetime(2018, 1, 10),
    schedule="@daily",
    catchup=False,
    tags=["module-03", "gold", "aggregation"],
) as dag:

    check = PythonOperator(
        task_id="check_silver_data",
        python_callable=check_silver_data,
    )

    episode_metrics = PythonOperator(
        task_id="build_episode_metrics",
        python_callable=build_episode_metrics,
    )

    platform_metrics = PythonOperator(
        task_id="build_platform_metrics",
        python_callable=build_platform_metrics,
    )

    country_metrics = PythonOperator(
        task_id="build_country_metrics",
        python_callable=build_country_metrics,
    )

    done = EmptyOperator(task_id="done")

    # Fan-out pattern:
    #                   +--> episode_metrics  --+
    #   check ------>   +--> platform_metrics --+--> done
    #                   +--> country_metrics  --+
    #
    # The three aggregation tasks run in parallel because they are
    # independent of each other. This reduces total execution time.
    check >> [episode_metrics, platform_metrics, country_metrics] >> done


# ---------------------------------------------------------------------------
# Standalone execution for syntax checking
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    dag.test()
