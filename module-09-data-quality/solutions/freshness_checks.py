"""
Exercise 4: Freshness Checks
==============================
Detects stale data and missing monthly parquet files in the taxi trip data.
Reports freshness gaps and SLO violations.
"""

import re
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"

# Freshness SLOs: maximum acceptable days since the latest record
FRESHNESS_SLOS = {
    "yellow_tripdata": 90,   # monthly files, 90 days tolerance
    "green_tripdata": 90,
    "taxi_zone_lookup": 365,  # reference data, annual refresh
    "vendors": 365,
    "rate_codes": 365,
    "payment_types": 365,
    "nyc_weather_2023": 365,
}


def get_latest_timestamp_from_column(df: pd.DataFrame, col: str) -> datetime | None:
    """Try to parse the latest timestamp from a column."""
    series = df[col].dropna()
    parsed = pd.to_datetime(series, errors="coerce")
    valid = parsed.dropna()
    if valid.empty:
        return None
    return valid.max().to_pydatetime()


def find_missing_monthly_files(raw_dir: Path, prefix: str) -> list[str]:
    """Find gaps in the monthly parquet file sequence."""
    files = sorted(raw_dir.glob(f"{prefix}_*.parquet"))
    months = []
    pattern = re.compile(rf"{prefix}_(\d{{4}}-\d{{2}})\.parquet")
    for f in files:
        match = pattern.match(f.name)
        if match:
            months.append(match.group(1))

    if len(months) < 2:
        return []

    # Generate expected sequence from first to last month
    first = datetime.strptime(months[0], "%Y-%m")
    last = datetime.strptime(months[-1], "%Y-%m")
    month_set = set(months)

    missing = []
    current = first
    while current <= last:
        key = current.strftime("%Y-%m")
        if key not in month_set:
            missing.append(key)
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return missing


def main():
    now = datetime.now()

    print(f"{'=' * 80}")
    print("  DATA FRESHNESS REPORT")
    print(f"  Report generated: {now.isoformat()}")
    print(f"{'=' * 80}")

    # Gather latest timestamps per dataset
    freshness_data = []

    # Yellow Tripdata
    yellow_frames = []
    for f in sorted(RAW_DIR.glob("yellow_tripdata_*.parquet")):
        yellow_frames.append(pd.read_parquet(f))
    if yellow_frames:
        yellow = pd.concat(yellow_frames, ignore_index=True)
        latest = get_latest_timestamp_from_column(yellow, "tpep_pickup_datetime")
        freshness_data.append(("yellow_tripdata", "tpep_pickup_datetime", latest))

    # Green Tripdata
    green_frames = []
    for f in sorted(RAW_DIR.glob("green_tripdata_*.parquet")):
        green_frames.append(pd.read_parquet(f))
    if green_frames:
        green = pd.concat(green_frames, ignore_index=True)
        latest = get_latest_timestamp_from_column(green, "lpep_pickup_datetime")
        freshness_data.append(("green_tripdata", "lpep_pickup_datetime", latest))

    # Weather data
    weather_path = RAW_DIR / "nyc_weather_2023.csv"
    if weather_path.exists():
        weather = pd.read_csv(weather_path)
        # Try common date column names
        for col in ["date", "DATE", "datetime", "timestamp"]:
            if col in weather.columns:
                latest = get_latest_timestamp_from_column(weather, col)
                freshness_data.append(("nyc_weather_2023", col, latest))
                break

    print(f"\n  {'Dataset':<22s} {'Column':<24s} {'Latest Record':<24s} "
          f"{'Freshness (days)':<18s} {'SLO (days)':<12s} {'Status'}")
    print(f"  {'-' * 120}")

    for dataset, column, latest_ts in freshness_data:
        slo_days = FRESHNESS_SLOS.get(dataset, 0)
        if latest_ts is None:
            print(f"  {dataset:<22s} {column:<24s} {'N/A':<24s} "
                  f"{'N/A':<18s} {slo_days:<12} UNKNOWN")
            continue

        gap = now - latest_ts
        gap_days = gap.total_seconds() / 86400
        status = "PASS" if gap_days <= slo_days else "FAIL (STALE)"

        print(f"  {dataset:<22s} {column:<24s} {str(latest_ts):<24s} "
              f"{gap_days:<18.1f} {slo_days:<12} {status}")

    # Check for missing monthly files
    print(f"\n  --- Missing Monthly Files (yellow_tripdata) ---")
    missing_yellow = find_missing_monthly_files(RAW_DIR, "yellow_tripdata")
    if missing_yellow:
        print(f"  Found {len(missing_yellow)} missing months in the file sequence:")
        for m in missing_yellow:
            print(f"    {m}")
    else:
        print("  No gaps found -- all months in the sequence are present.")

    print(f"\n  --- Missing Monthly Files (green_tripdata) ---")
    missing_green = find_missing_monthly_files(RAW_DIR, "green_tripdata")
    if missing_green:
        print(f"  Found {len(missing_green)} missing months in the file sequence:")
        for m in missing_green:
            print(f"    {m}")
    else:
        print("  No gaps found -- all months in the sequence are present.")

    # File count summary
    yellow_files = sorted(RAW_DIR.glob("yellow_tripdata_*.parquet"))
    green_files = sorted(RAW_DIR.glob("green_tripdata_*.parquet"))
    print(f"\n  Yellow tripdata files: {len(yellow_files)}")
    if yellow_files:
        print(f"  Range: {yellow_files[0].name} to {yellow_files[-1].name}")
    print(f"  Green tripdata files: {len(green_files)}")
    if green_files:
        print(f"  Range: {green_files[0].name} to {green_files[-1].name}")

    print(f"\n{'=' * 80}")
    print("  FRESHNESS CHECK COMPLETE")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
