"""
Solution 04: Silver to Gold (Aggregate Taxi Metrics)
=====================================================
This DAG reads cleaned Silver taxi trip data and produces aggregated Gold-layer
tables designed for analytical consumption.

Key patterns demonstrated:
  - Fan-out: one check task feeds multiple parallel aggregation tasks
  - Gold layer design: pre-aggregated tables optimized for specific queries
  - Each aggregation is independent -> they run in parallel for speed
  - EmptyOperator as a join point after fan-out
  - Combining yellow and green taxi data for unified metrics

Gold tables produced:
  1. daily_trip_metrics  - Daily trip statistics by taxi type
  2. zone_popularity     - Pickup/dropoff zone rankings
  3. revenue_summary     - Revenue breakdown by taxi type and payment type

Data flow:
  /opt/airflow/data/processed/silver/{yellow,green}_taxi_trips/year=YYYY/month=MM/trips.parquet
    -->
  /opt/airflow/data/processed/gold/daily_trip_metrics/year=YYYY/month=MM/metrics.parquet
  /opt/airflow/data/processed/gold/zone_popularity/year=YYYY/month=MM/metrics.parquet
  /opt/airflow/data/processed/gold/revenue_summary/year=YYYY/month=MM/metrics.parquet

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
SILVER_DIR = f"{DATA_DIR}/processed/silver"
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
# Helpers
# ---------------------------------------------------------------------------
def _year_month(ds: str):
    """Extract year, month, and year-month string from logical date."""
    return ds[:4], ds[5:7], ds[:7]


def _read_silver_data(ds: str):
    """
    Read Silver Parquet files for both yellow and green taxis.
    Combines them into a single DataFrame for unified analysis.

    Returns a pandas DataFrame with all trips for the month.

    This helper is called by multiple tasks. Factoring shared logic into
    a helper function (rather than duplicating it) keeps the code DRY.
    """
    import pandas as pd

    year, month, year_month = _year_month(ds)
    frames = []

    yellow_path = f"{SILVER_DIR}/yellow_taxi_trips/year={year}/month={month}/trips.parquet"
    if os.path.exists(yellow_path):
        df_yellow = pd.read_parquet(yellow_path)
        print(f"Read {len(df_yellow):,} Silver yellow rows from {yellow_path}")
        frames.append(df_yellow)
    else:
        print(f"No Silver yellow data for {year_month}")

    green_path = f"{SILVER_DIR}/green_taxi_trips/year={year}/month={month}/trips.parquet"
    if os.path.exists(green_path):
        df_green = pd.read_parquet(green_path)
        print(f"Read {len(df_green):,} Silver green rows from {green_path}")
        frames.append(df_green)
    else:
        print(f"No Silver green data for {year_month}")

    if not frames:
        raise AirflowSkipException(
            f"No Silver data found for {year_month}. "
            f"Run the bronze_to_silver DAG first."
        )

    df = pd.concat(frames, ignore_index=True)
    print(f"Combined Silver data: {len(df):,} total rows")
    return df


def _write_gold_table(df, table_name: str, ds: str):
    """
    Write a Gold table to the standard directory structure.
    Returns the output path.
    """
    year, month, _ = _year_month(ds)
    output_dir = f"{GOLD_DIR}/{table_name}/year={year}/month={month}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/metrics.parquet"
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)
    print(f"Wrote {len(df):,} rows to {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def check_silver_data(ds: str, **context):
    """Verify Silver data exists for at least one taxi type for the given month."""
    year, month, year_month = _year_month(ds)

    yellow_path = f"{SILVER_DIR}/yellow_taxi_trips/year={year}/month={month}/trips.parquet"
    green_path = f"{SILVER_DIR}/green_taxi_trips/year={year}/month={month}/trips.parquet"

    yellow_exists = os.path.exists(yellow_path)
    green_exists = os.path.exists(green_path)

    if not yellow_exists and not green_exists:
        raise AirflowSkipException(
            f"No Silver data found for {year_month}. "
            f"Run the bronze_to_silver DAG first."
        )

    if yellow_exists:
        size_mb = os.path.getsize(yellow_path) / (1024 * 1024)
        print(f"Silver yellow found: {yellow_path} ({size_mb:.1f} MB)")
    if green_exists:
        size_mb = os.path.getsize(green_path) / (1024 * 1024)
        print(f"Silver green found: {green_path} ({size_mb:.1f} MB)")


def build_daily_trip_metrics(ds: str, ti, **context):
    """
    Aggregate trip data by date and taxi type.

    Produces daily statistics that answer questions like:
      - How many trips happened each day?
      - What was the average trip distance?
      - How does weekday vs. weekend traffic compare?

    Columns:
      - pickup_date: date of the trip
      - taxi_type: yellow or green
      - total_trips: count of trips
      - total_passengers: sum of passenger counts
      - total_distance_miles: sum of trip distances
      - total_fare_amount: sum of fare amounts
      - avg_trip_duration_minutes: mean trip duration
      - avg_speed_mph: mean trip speed
    """
    import pandas as pd

    df = _read_silver_data(ds)

    # Extract the pickup date for daily grouping
    df["pickup_date"] = df["pickup_datetime"].dt.date

    metrics = df.groupby(["pickup_date", "taxi_type"]).agg(
        total_trips=("pickup_datetime", "count"),
        total_passengers=("passenger_count", "sum"),
        total_distance_miles=("trip_distance", "sum"),
        total_fare_amount=("fare_amount", "sum"),
        avg_trip_duration_minutes=("trip_duration_minutes", "mean"),
        avg_speed_mph=("speed_mph", "mean"),
    ).reset_index()

    # Round numeric columns for readability
    metrics["total_distance_miles"] = metrics["total_distance_miles"].round(1)
    metrics["total_fare_amount"] = metrics["total_fare_amount"].round(2)
    metrics["avg_trip_duration_minutes"] = metrics["avg_trip_duration_minutes"].round(2)
    metrics["avg_speed_mph"] = metrics["avg_speed_mph"].round(2)

    # Convert date objects to strings for Parquet compatibility
    metrics["pickup_date"] = metrics["pickup_date"].astype(str)

    output_path = _write_gold_table(metrics, "daily_trip_metrics", ds)
    ti.xcom_push(key="daily_metrics_count", value=len(metrics))

    print(f"\nSample daily trip metrics (first 10 rows):")
    print(metrics.head(10).to_string(index=False))


def build_zone_popularity(ds: str, ti, **context):
    """
    Aggregate trip data by pickup/dropoff zone.

    Produces zone-level metrics that answer questions like:
      - Which zones have the most pickups?
      - What is the average fare from each zone?
      - How do pickup and dropoff patterns differ by taxi type?

    Columns:
      - zone_id: the TLC location ID
      - taxi_type: yellow or green
      - total_pickups: count of trips starting in this zone
      - total_dropoffs: count of trips ending in this zone
      - avg_fare_amount: mean fare for trips from this zone
    """
    import pandas as pd

    df = _read_silver_data(ds)

    # Build pickup metrics
    pickup_col = "PULocationID" if "PULocationID" in df.columns else "pulocationid"
    dropoff_col = "DOLocationID" if "DOLocationID" in df.columns else "dolocationid"

    # Handle case where location columns might not exist
    if pickup_col not in df.columns:
        print(f"WARNING: pickup location column not found. Available: {list(df.columns)}")
        # Create a minimal output
        metrics = pd.DataFrame(columns=[
            "zone_id", "taxi_type", "total_pickups", "total_dropoffs", "avg_fare_amount"
        ])
        _write_gold_table(metrics, "zone_popularity", ds)
        ti.xcom_push(key="zone_metrics_count", value=0)
        return

    pickups = df.groupby([pickup_col, "taxi_type"]).agg(
        total_pickups=("pickup_datetime", "count"),
        avg_fare_amount=("fare_amount", "mean"),
    ).reset_index().rename(columns={pickup_col: "zone_id"})

    dropoffs = df.groupby([dropoff_col, "taxi_type"]).agg(
        total_dropoffs=("pickup_datetime", "count"),
    ).reset_index().rename(columns={dropoff_col: "zone_id"})

    # Merge pickup and dropoff metrics
    metrics = pickups.merge(
        dropoffs, on=["zone_id", "taxi_type"], how="outer"
    )
    metrics["total_pickups"] = metrics["total_pickups"].fillna(0).astype(int)
    metrics["total_dropoffs"] = metrics["total_dropoffs"].fillna(0).astype(int)
    metrics["avg_fare_amount"] = metrics["avg_fare_amount"].round(2)

    # Sort by total pickups descending
    metrics = metrics.sort_values("total_pickups", ascending=False)

    output_path = _write_gold_table(metrics, "zone_popularity", ds)
    ti.xcom_push(key="zone_metrics_count", value=len(metrics))

    print(f"\nTop 10 zones by pickups:")
    print(metrics.head(10).to_string(index=False))


def build_revenue_summary(ds: str, ti, **context):
    """
    Aggregate revenue data by taxi type and payment type.

    Produces revenue metrics that answer questions like:
      - How much total revenue was collected?
      - What percentage of riders pay by card vs. cash?
      - What is the average tip by payment method?

    Columns:
      - taxi_type: yellow or green
      - payment_type: payment method code (1=credit card, 2=cash, etc.)
      - total_trips: count of trips
      - total_fare_amount: sum of fares
      - total_tip_amount: sum of tips
      - total_total_amount: sum of total charge
      - avg_tip_percentage: mean tip as % of fare
    """
    import pandas as pd

    df = _read_silver_data(ds)

    # Check required columns
    required_cols = {"fare_amount", "payment_type", "taxi_type"}
    missing = required_cols - set(df.columns)
    if missing:
        print(f"WARNING: Missing columns for revenue summary: {missing}")
        metrics = pd.DataFrame()
        _write_gold_table(metrics, "revenue_summary", ds)
        ti.xcom_push(key="revenue_metrics_count", value=0)
        return

    # Ensure tip and total columns exist (fill with 0 if missing)
    if "tip_amount" not in df.columns:
        df["tip_amount"] = 0
    if "total_amount" not in df.columns:
        df["total_amount"] = df["fare_amount"]

    metrics = df.groupby(["taxi_type", "payment_type"]).agg(
        total_trips=("pickup_datetime", "count"),
        total_fare_amount=("fare_amount", "sum"),
        total_tip_amount=("tip_amount", "sum"),
        total_total_amount=("total_amount", "sum"),
    ).reset_index()

    # Calculate average tip percentage (tip / fare * 100), avoiding division by zero
    metrics["avg_tip_percentage"] = (
        (metrics["total_tip_amount"] / metrics["total_fare_amount"].replace(0, float("nan")))
        * 100
    ).round(2).fillna(0)

    # Round monetary columns
    metrics["total_fare_amount"] = metrics["total_fare_amount"].round(2)
    metrics["total_tip_amount"] = metrics["total_tip_amount"].round(2)
    metrics["total_total_amount"] = metrics["total_total_amount"].round(2)

    output_path = _write_gold_table(metrics, "revenue_summary", ds)
    ti.xcom_push(key="revenue_metrics_count", value=len(metrics))

    print(f"\nRevenue breakdown:")
    print(metrics.to_string(index=False))


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="silver_to_gold_metrics",
    description="Aggregate Silver taxi trips into Gold metric tables (daily stats, zone popularity, revenue)",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["module-03", "gold", "aggregation"],
) as dag:

    check = PythonOperator(
        task_id="check_silver_data",
        python_callable=check_silver_data,
    )

    daily_metrics = PythonOperator(
        task_id="build_daily_trip_metrics",
        python_callable=build_daily_trip_metrics,
    )

    zone_metrics = PythonOperator(
        task_id="build_zone_popularity",
        python_callable=build_zone_popularity,
    )

    revenue_metrics = PythonOperator(
        task_id="build_revenue_summary",
        python_callable=build_revenue_summary,
    )

    done = EmptyOperator(task_id="done")

    # Fan-out pattern:
    #                   +--> daily_trip_metrics  --+
    #   check ------>   +--> zone_popularity     --+--> done
    #                   +--> revenue_summary     --+
    #
    # The three aggregation tasks run in parallel because they are
    # independent of each other. This reduces total execution time.
    check >> [daily_metrics, zone_metrics, revenue_metrics] >> done


# ---------------------------------------------------------------------------
# Standalone execution for syntax checking
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    dag.test()
