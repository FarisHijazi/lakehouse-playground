"""
Solution 03: Bronze to Silver (Clean, Validate, Enrich Taxi Trips)
===================================================================
This DAG reads Bronze Parquet trip data, applies data quality rules and
validation, adds derived columns, and writes clean data to the Silver layer.

Key patterns demonstrated:
  - Data cleaning: null handling, outlier filtering, value validation
  - Derived columns: trip_duration_minutes, speed_mph
  - Quality metrics: tracking how many rows were dropped and why
  - XCom for passing quality reports between tasks
  - Fan-out: yellow and green trips cleaned in parallel

Cleaning rules applied:
  1. Drop rows where pickup or dropoff datetime is null
  2. Drop rows where passenger_count is null or <= 0
  3. Filter trips with unreasonable distance (> 200 miles or < 0)
  4. Filter trips with unreasonable fares (> $1000 or < 0)
  5. Add trip_duration_minutes (dropoff_datetime - pickup_datetime)
  6. Add speed_mph (trip_distance / duration_hours)
  7. Filter trips with unreasonable speed (> 100 mph)
  8. Add processed_at timestamp for lineage tracking

Data flow:
  /opt/airflow/data/bronze/yellow_taxi_trips/year=YYYY/month=MM/trips.parquet
    -->
  /opt/airflow/data/processed/silver/yellow_taxi_trips/year=YYYY/month=MM/trips.parquet

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
BRONZE_DIR = f"{DATA_DIR}/bronze"
SILVER_DIR = f"{DATA_DIR}/processed/silver"

# Cleaning thresholds
MAX_DISTANCE_MILES = 200
MAX_FARE_AMOUNT = 1000
MAX_SPEED_MPH = 100
MIN_DURATION_MINUTES = 0.5   # 30 seconds minimum
MAX_DURATION_MINUTES = 720   # 12 hours maximum

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
# Helpers
# ---------------------------------------------------------------------------
def _year_month(ds: str):
    """Extract year, month, and year-month string from logical date."""
    return ds[:4], ds[5:7], ds[:7]


def _clean_trips(df, taxi_type: str):
    """
    Apply cleaning rules to a taxi trips DataFrame.

    This function encapsulates the cleaning logic so it can be reused
    for both yellow and green taxi trips. It returns the cleaned DataFrame
    and a dictionary of quality metrics.

    Args:
        df: pandas DataFrame of raw trip data.
        taxi_type: "yellow" or "green", used for logging.

    Returns:
        Tuple of (cleaned DataFrame, quality metrics dict).
    """
    import pandas as pd

    total_rows_in = len(df)
    metrics = {"total_rows_in": int(total_rows_in)}

    # --- Determine datetime column names ---
    # Yellow taxis use tpep_pickup_datetime / tpep_dropoff_datetime
    # Green taxis use lpep_pickup_datetime / lpep_dropoff_datetime
    if "tpep_pickup_datetime" in df.columns:
        pickup_col = "tpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime"
    elif "lpep_pickup_datetime" in df.columns:
        pickup_col = "lpep_pickup_datetime"
        dropoff_col = "lpep_dropoff_datetime"
    else:
        # Fallback: try generic names
        pickup_col = "pickup_datetime"
        dropoff_col = "dropoff_datetime"

    # Rename to standard column names for downstream consistency
    df = df.rename(columns={
        pickup_col: "pickup_datetime",
        dropoff_col: "dropoff_datetime",
    })

    # Ensure datetime types
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

    # --- Step 1: Drop rows with null datetimes ---
    rows_before = len(df)
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime"])
    null_datetimes = rows_before - len(df)
    metrics["null_datetimes_dropped"] = int(null_datetimes)
    print(f"  [{taxi_type}] Step 1 - Null datetimes dropped: {null_datetimes}")

    # --- Step 2: Drop rows with invalid passenger count ---
    if "passenger_count" in df.columns:
        rows_before = len(df)
        df = df.dropna(subset=["passenger_count"])
        df = df[df["passenger_count"] > 0]
        bad_passengers = rows_before - len(df)
    else:
        bad_passengers = 0
    metrics["bad_passengers_dropped"] = int(bad_passengers)
    print(f"  [{taxi_type}] Step 2 - Bad passenger_count dropped: {bad_passengers}")

    # --- Step 3: Filter unreasonable distances ---
    if "trip_distance" in df.columns:
        rows_before = len(df)
        df = df[
            (df["trip_distance"] >= 0) &
            (df["trip_distance"] <= MAX_DISTANCE_MILES)
        ]
        bad_distance = rows_before - len(df)
    else:
        bad_distance = 0
    metrics["bad_distance_dropped"] = int(bad_distance)
    print(f"  [{taxi_type}] Step 3 - Unreasonable distance dropped: {bad_distance}")

    # --- Step 4: Filter unreasonable fares ---
    if "fare_amount" in df.columns:
        rows_before = len(df)
        df = df[
            (df["fare_amount"] >= 0) &
            (df["fare_amount"] <= MAX_FARE_AMOUNT)
        ]
        bad_fares = rows_before - len(df)
    else:
        bad_fares = 0
    metrics["bad_fares_dropped"] = int(bad_fares)
    print(f"  [{taxi_type}] Step 4 - Unreasonable fares dropped: {bad_fares}")

    # --- Step 5: Add trip_duration_minutes ---
    # Duration is critical for calculating speed and identifying anomalies.
    df["trip_duration_minutes"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60.0
    )

    # Filter out negative or extreme durations
    rows_before = len(df)
    df = df[
        (df["trip_duration_minutes"] >= MIN_DURATION_MINUTES) &
        (df["trip_duration_minutes"] <= MAX_DURATION_MINUTES)
    ]
    bad_duration = rows_before - len(df)
    metrics["bad_duration_dropped"] = int(bad_duration)
    print(f"  [{taxi_type}] Step 5 - Bad duration dropped: {bad_duration}")

    # Round duration for readability
    df["trip_duration_minutes"] = df["trip_duration_minutes"].round(2)

    # --- Step 6: Add speed_mph ---
    # Speed = distance / time. Guard against division by zero.
    if "trip_distance" in df.columns:
        duration_hours = df["trip_duration_minutes"] / 60.0
        # Avoid division by zero by replacing 0 with NaN temporarily
        duration_hours = duration_hours.replace(0, float("nan"))
        df["speed_mph"] = (df["trip_distance"] / duration_hours).round(2)

        # Filter unreasonable speeds
        rows_before = len(df)
        df = df[df["speed_mph"].isna() | (df["speed_mph"] <= MAX_SPEED_MPH)]
        bad_speed = rows_before - len(df)
        # Fill NaN speeds with 0 (zero-duration trips that slipped through)
        df["speed_mph"] = df["speed_mph"].fillna(0)
    else:
        bad_speed = 0
        df["speed_mph"] = 0
    metrics["bad_speed_dropped"] = int(bad_speed)
    print(f"  [{taxi_type}] Step 6 - Unreasonable speed dropped: {bad_speed}")

    # --- Step 7: Add processed_at timestamp ---
    df["processed_at"] = pd.Timestamp.now(tz="UTC")

    # --- Step 8: Ensure taxi_type column exists ---
    if "taxi_type" not in df.columns:
        df["taxi_type"] = taxi_type

    total_rows_out = len(df)
    metrics["total_rows_out"] = int(total_rows_out)
    total_dropped = total_rows_in - total_rows_out
    drop_rate = (total_dropped / total_rows_in * 100) if total_rows_in > 0 else 0
    print(f"  [{taxi_type}] Final: {total_rows_in:,} -> {total_rows_out:,} "
          f"(dropped {total_dropped:,}, {drop_rate:.1f}%)")

    return df, metrics


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def check_bronze_data(ds: str, **context):
    """
    Verify that at least one Bronze Parquet file exists for the given month.
    Skip downstream tasks if no Bronze data is available.
    """
    year, month, _ = _year_month(ds)

    yellow_path = f"{BRONZE_DIR}/yellow_taxi_trips/year={year}/month={month}/trips.parquet"
    green_path = f"{BRONZE_DIR}/green_taxi_trips/year={year}/month={month}/trips.parquet"

    yellow_exists = os.path.exists(yellow_path)
    green_exists = os.path.exists(green_path)

    if not yellow_exists and not green_exists:
        raise AirflowSkipException(
            f"No Bronze data found for {year}-{month}. "
            f"Run the ingestion DAG first."
        )

    if yellow_exists:
        print(f"Bronze yellow data found: {yellow_path}")
    else:
        print(f"Bronze yellow data NOT found (will skip yellow cleaning)")

    if green_exists:
        print(f"Bronze green data found: {green_path}")
    else:
        print(f"Bronze green data NOT found (will skip green cleaning)")


def clean_yellow_trips(ds: str, ti, **context):
    """
    Read Bronze yellow taxi data, apply cleaning rules, write to Silver.

    Yellow taxis are the iconic NYC cabs that operate primarily in Manhattan
    and at airports. Their data uses tpep_pickup/dropoff_datetime columns.
    """
    import pandas as pd

    year, month, _ = _year_month(ds)
    bronze_path = f"{BRONZE_DIR}/yellow_taxi_trips/year={year}/month={month}/trips.parquet"

    if not os.path.exists(bronze_path):
        raise AirflowSkipException(f"No Bronze yellow data: {bronze_path}")

    print(f"Reading Bronze yellow data: {bronze_path}")
    df = pd.read_parquet(bronze_path)
    print(f"Bronze yellow rows: {len(df):,}")

    # Apply cleaning
    df_clean, metrics = _clean_trips(df, "yellow")

    # Write Silver output
    output_dir = f"{SILVER_DIR}/yellow_taxi_trips/year={year}/month={month}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/trips.parquet"
    df_clean.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Wrote Silver yellow: {output_path}")

    ti.xcom_push(key="yellow_quality_metrics", value=metrics)
    ti.xcom_push(key="yellow_output_path", value=output_path)


def clean_green_trips(ds: str, ti, **context):
    """
    Read Bronze green taxi data, apply cleaning rules, write to Silver.

    Green taxis (Boro taxis) were introduced in 2013 to serve areas outside
    Manhattan's core. Their data uses lpep_pickup/dropoff_datetime columns.
    """
    import pandas as pd

    year, month, _ = _year_month(ds)
    bronze_path = f"{BRONZE_DIR}/green_taxi_trips/year={year}/month={month}/trips.parquet"

    if not os.path.exists(bronze_path):
        raise AirflowSkipException(f"No Bronze green data: {bronze_path}")

    print(f"Reading Bronze green data: {bronze_path}")
    df = pd.read_parquet(bronze_path)
    print(f"Bronze green rows: {len(df):,}")

    # Apply cleaning
    df_clean, metrics = _clean_trips(df, "green")

    # Write Silver output
    output_dir = f"{SILVER_DIR}/green_taxi_trips/year={year}/month={month}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/trips.parquet"
    df_clean.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Wrote Silver green: {output_path}")

    ti.xcom_push(key="green_quality_metrics", value=metrics)
    ti.xcom_push(key="green_output_path", value=output_path)


def log_quality_metrics(ti, ds: str, **context):
    """
    Pull and display quality metrics from the cleaning steps.

    In production, you would send these metrics to a monitoring system
    and potentially trigger alerts if the drop rate exceeds a threshold
    (e.g., "more than 20% of rows dropped -> alert the on-call engineer").
    """
    _, _, year_month = _year_month(ds)

    yellow_metrics = ti.xcom_pull(
        task_ids="clean_yellow_trips", key="yellow_quality_metrics"
    )
    green_metrics = ti.xcom_pull(
        task_ids="clean_green_trips", key="green_quality_metrics"
    )

    print("=" * 60)
    print(f"  Data Quality Report for {year_month}")
    print("=" * 60)

    for label, metrics in [("Yellow", yellow_metrics), ("Green", green_metrics)]:
        if metrics is None:
            print(f"  {label} taxi: SKIPPED (no data)")
            continue

        total_in = metrics["total_rows_in"]
        total_out = metrics["total_rows_out"]
        drop_rate = ((total_in - total_out) / total_in * 100) if total_in > 0 else 0

        print(f"  {label} taxi:")
        print(f"    Rows in (Bronze)          : {total_in:,}")
        print(f"    Rows out (Silver)         : {total_out:,}")
        print(f"    Overall drop rate         : {drop_rate:.1f}%")
        print(f"    ---")
        print(f"    Null datetimes dropped    : {metrics['null_datetimes_dropped']:,}")
        print(f"    Bad passenger count       : {metrics['bad_passengers_dropped']:,}")
        print(f"    Unreasonable distance     : {metrics['bad_distance_dropped']:,}")
        print(f"    Unreasonable fares        : {metrics['bad_fares_dropped']:,}")
        print(f"    Bad duration              : {metrics['bad_duration_dropped']:,}")
        print(f"    Unreasonable speed        : {metrics['bad_speed_dropped']:,}")
        print()

        if drop_rate > 20:
            print(
                f"    WARNING: Drop rate of {drop_rate:.1f}% exceeds 20% threshold. "
                f"Investigate data quality for {year_month}."
            )

    print("=" * 60)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="bronze_to_silver_trips",
    description="Clean and validate Bronze taxi trips into the Silver layer with derived columns",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["module-03", "silver", "cleaning"],
) as dag:

    check = PythonOperator(
        task_id="check_bronze_data",
        python_callable=check_bronze_data,
    )

    yellow = PythonOperator(
        task_id="clean_yellow_trips",
        python_callable=clean_yellow_trips,
    )

    green = PythonOperator(
        task_id="clean_green_trips",
        python_callable=clean_green_trips,
    )

    report = PythonOperator(
        task_id="log_quality_metrics",
        python_callable=log_quality_metrics,
        trigger_rule="none_failed",
    )

    # Fan-out pattern: check -> clean yellow & green in parallel -> report
    #
    #                +--> clean_yellow_trips --+
    #   check ---+                             +--> log_quality_metrics
    #                +--> clean_green_trips  --+
    check >> [yellow, green] >> report


# ---------------------------------------------------------------------------
# Standalone execution for syntax checking
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    dag.test()
