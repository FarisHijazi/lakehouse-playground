"""
Exercise 5: Volume Anomaly Detection
======================================
Counts monthly taxi trips and flags anomalous months using:
  - Rolling average with standard deviation bands
  - Month-over-month percentage change (>50% drop/spike)
  - Comparison of yellow vs green trip volumes
"""

import re
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


def count_trips_per_file(raw_dir: Path, prefix: str) -> pd.DataFrame:
    """Count rows in each monthly parquet file to get trip counts."""
    records = []
    pattern = re.compile(rf"{prefix}_(\d{{4}}-\d{{2}})\.parquet")
    for f in sorted(raw_dir.glob(f"{prefix}_*.parquet")):
        match = pattern.match(f.name)
        if match:
            month_str = match.group(1)
            row_count = len(pd.read_parquet(f))
            records.append({"month": month_str, "trip_count": row_count})
    df = pd.DataFrame(records)
    if not df.empty:
        df["month"] = pd.to_datetime(df["month"])
        df = df.sort_values("month").reset_index(drop=True)
    return df


def detect_anomalies(monthly: pd.DataFrame) -> pd.DataFrame:
    """Add anomaly detection columns."""
    df = monthly.copy()

    if len(df) < 2:
        df["rolling_mean"] = df["trip_count"].astype(float)
        df["rolling_std"] = 0.0
        df["upper_2sigma"] = df["trip_count"].astype(float)
        df["lower_2sigma"] = 0.0
        df["sigma_anomaly"] = False
        df["prev_month_count"] = None
        df["mom_pct_change"] = None
        df["mom_anomaly"] = False
        df["is_anomaly"] = False
        return df

    # Rolling statistics (use min_periods=2 for small datasets)
    window = min(3, len(df))
    df["rolling_mean"] = df["trip_count"].rolling(window=window, min_periods=2).mean()
    df["rolling_std"] = df["trip_count"].rolling(window=window, min_periods=2).std()
    df["upper_2sigma"] = df["rolling_mean"] + 2 * df["rolling_std"]
    df["lower_2sigma"] = (df["rolling_mean"] - 2 * df["rolling_std"]).clip(lower=0)

    # Sigma anomaly
    df["sigma_anomaly"] = (
        (df["trip_count"] > df["upper_2sigma"]) |
        (df["trip_count"] < df["lower_2sigma"])
    )

    # Month-over-month percentage change
    df["prev_month_count"] = df["trip_count"].shift(1)
    df["mom_pct_change"] = (
        (df["trip_count"] - df["prev_month_count"]).abs() /
        df["prev_month_count"] * 100
    )
    df["mom_anomaly"] = df["mom_pct_change"] > 50.0

    # Combined anomaly flag
    df["is_anomaly"] = df["sigma_anomaly"] | df["mom_anomaly"]

    return df


def main():
    print("Counting trips per monthly file...")

    # Yellow trips
    yellow_monthly = count_trips_per_file(RAW_DIR, "yellow_tripdata")

    print(f"\n{'=' * 100}")
    print("  VOLUME ANOMALY DETECTION REPORT")
    print(f"{'=' * 100}")

    if not yellow_monthly.empty:
        print(f"\n  --- Yellow Tripdata ---")
        print(f"  Total files: {len(yellow_monthly)}")
        print(f"  Month range: {yellow_monthly['month'].min().date()} to "
              f"{yellow_monthly['month'].max().date()}")
        print(f"  Total trips: {yellow_monthly['trip_count'].sum():,}")
        print(f"  Monthly average: {yellow_monthly['trip_count'].mean():,.0f}")
        print(f"  Monthly std dev: {yellow_monthly['trip_count'].std():,.0f}"
              if len(yellow_monthly) > 1 else "")
        print(f"  Monthly min: {yellow_monthly['trip_count'].min():,}")
        print(f"  Monthly max: {yellow_monthly['trip_count'].max():,}")

        yellow_df = detect_anomalies(yellow_monthly)

        # Show all months with stats
        print(f"\n  {'Month':<14s} {'Count':>12s} {'Rolling Mean':>14s} "
              f"{'Lower':>12s} {'Upper':>12s} {'MoM %':>8s} {'Anomaly'}")
        print(f"  {'-' * 90}")
        for _, row in yellow_df.iterrows():
            anomaly_str = ""
            if row.get("is_anomaly"):
                reasons = []
                if row.get("sigma_anomaly"):
                    reasons.append("2-sigma")
                if row.get("mom_anomaly"):
                    reasons.append(f"MoM>{row['mom_pct_change']:.0f}%")
                anomaly_str = ", ".join(reasons)
            rm = row['rolling_mean']
            ls = row['lower_2sigma']
            us = row['upper_2sigma']
            mc = row.get('mom_pct_change', 0)
            print(f"  {str(row['month'].date()):<14s} {row['trip_count']:>12,} "
                  f"{rm:>14,.0f} " if pd.notna(rm) else f"  {str(row['month'].date()):<14s} {row['trip_count']:>12,} {'N/A':>14s} ",
                  end="")
            print(f"{ls:>12,.0f} {us:>12,.0f} " if pd.notna(ls) else f"{'N/A':>12s} {'N/A':>12s} ", end="")
            print(f"{mc:>8.1f} " if pd.notna(mc) else f"{'N/A':>8s} ", end="")
            print(anomaly_str)

        # Show anomalies
        anomalies = yellow_df[yellow_df["is_anomaly"]]
        if not anomalies.empty:
            print(f"\n  Anomalous months: {len(anomalies)}")
        else:
            print(f"\n  No anomalies detected in yellow trip volumes.")

    # Green trips
    green_monthly = count_trips_per_file(RAW_DIR, "green_tripdata")
    if not green_monthly.empty:
        print(f"\n  --- Green Tripdata ---")
        print(f"  Total files: {len(green_monthly)}")
        print(f"  Total trips: {green_monthly['trip_count'].sum():,}")
        print(f"  Monthly average: {green_monthly['trip_count'].mean():,.0f}")

        for _, row in green_monthly.iterrows():
            print(f"    {row['month'].date()}: {row['trip_count']:,} trips")

    # Cross-dataset comparison
    if not yellow_monthly.empty and not green_monthly.empty:
        print(f"\n  --- Yellow vs Green Comparison ---")
        merged = yellow_monthly.merge(green_monthly, on="month", suffixes=("_yellow", "_green"))
        for _, row in merged.iterrows():
            ratio = row["trip_count_yellow"] / row["trip_count_green"] if row["trip_count_green"] > 0 else float("inf")
            print(f"    {row['month'].date()}: yellow={row['trip_count_yellow']:,}, "
                  f"green={row['trip_count_green']:,}, ratio={ratio:.1f}x")

    print(f"\n{'=' * 100}")
    print("  VOLUME ANOMALY DETECTION COMPLETE")
    print(f"{'=' * 100}")


if __name__ == "__main__":
    main()
