"""
Exercise 4: Freshness Checks
==============================
Detects stale data and missing partitions in the listening events.
Reports freshness gaps and SLO violations.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"

# Freshness SLOs: maximum acceptable hours since the latest record
FRESHNESS_SLOS = {
    "listening_events": 24,
    "cdn_logs": 24,
    "ad_events": 48,
    "users": 168,  # weekly sync
    "episodes": 168,
    "podcasts": 720,  # monthly
}


def get_latest_timestamp_from_column(df: pd.DataFrame, col: str) -> datetime | None:
    """Try to parse the latest timestamp from a column."""
    series = df[col].dropna().astype(str)
    parsed = pd.to_datetime(series, errors="coerce", format="mixed")
    valid = parsed.dropna()
    if valid.empty:
        return None
    return valid.max().to_pydatetime()


def find_missing_partition_dates(events_dir: Path) -> list[str]:
    """Find gaps in the daily partition sequence."""
    files = sorted(events_dir.glob("events_*.jsonl"))
    dates = []
    for f in files:
        # Extract date from filename: events_YYYY-MM-DD.jsonl
        date_str = f.stem.replace("events_", "")
        try:
            dates.append(datetime.strptime(date_str, "%Y-%m-%d").date())
        except ValueError:
            continue

    if len(dates) < 2:
        return []

    min_date = min(dates)
    max_date = max(dates)
    date_set = set(dates)

    missing = []
    current = min_date
    while current <= max_date:
        if current not in date_set:
            missing.append(current.isoformat())
        current += timedelta(days=1)

    return missing


def main():
    now = datetime.now()

    print(f"{'=' * 80}")
    print("  DATA FRESHNESS REPORT")
    print(f"  Report generated: {now.isoformat()}")
    print(f"{'=' * 80}")

    # Gather latest timestamps per dataset
    freshness_data = []

    # Users
    users = pd.read_csv(RAW_DIR / "users.csv")
    latest = get_latest_timestamp_from_column(users, "signup_date")
    freshness_data.append(("users", "signup_date", latest))

    # CDN Logs
    cdn = pd.read_csv(RAW_DIR / "cdn_logs.csv")
    latest = get_latest_timestamp_from_column(cdn, "timestamp")
    freshness_data.append(("cdn_logs", "timestamp", latest))

    # Ad Events
    ad_events = pd.DataFrame(json.loads((RAW_DIR / "ad_events.json").read_text()))
    latest = get_latest_timestamp_from_column(ad_events, "timestamp")
    freshness_data.append(("ad_events", "timestamp", latest))

    # Episodes
    episodes = pd.DataFrame(json.loads((RAW_DIR / "episodes.json").read_text()))
    latest = get_latest_timestamp_from_column(episodes, "published_at")
    freshness_data.append(("episodes", "published_at", latest))

    # Podcasts
    podcasts = pd.DataFrame(json.loads((RAW_DIR / "podcasts.json").read_text()))
    latest = get_latest_timestamp_from_column(podcasts, "created_at")
    freshness_data.append(("podcasts", "created_at", latest))

    # Listening Events -- use the latest partition filename
    events_dir = RAW_DIR / "listening_events"
    partition_files = sorted(events_dir.glob("events_*.jsonl"))
    if partition_files:
        latest_file = partition_files[-1]
        date_str = latest_file.stem.replace("events_", "")
        try:
            latest_partition = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            latest_partition = None
        freshness_data.append(("listening_events", "partition_date", latest_partition))

    print(f"\n  {'Dataset':<22s} {'Column':<18s} {'Latest Record':<24s} "
          f"{'Freshness (hrs)':<18s} {'SLO (hrs)':<12s} {'Status'}")
    print(f"  {'-' * 110}")

    for dataset, column, latest_ts in freshness_data:
        slo_hours = FRESHNESS_SLOS.get(dataset, 0)
        if latest_ts is None:
            print(f"  {dataset:<22s} {column:<18s} {'N/A':<24s} "
                  f"{'N/A':<18s} {slo_hours:<12} UNKNOWN")
            continue

        gap = now - latest_ts
        gap_hours = gap.total_seconds() / 3600
        status = "PASS" if gap_hours <= slo_hours else "FAIL (STALE)"

        print(f"  {dataset:<22s} {column:<18s} {str(latest_ts):<24s} "
              f"{gap_hours:<18.1f} {slo_hours:<12} {status}")

    # Check for missing partition dates
    print(f"\n  --- Missing Partition Dates (listening_events) ---")
    missing = find_missing_partition_dates(events_dir)
    if missing:
        print(f"  Found {len(missing)} missing dates in the partition sequence:")
        # Show first 20 and last 5
        if len(missing) <= 25:
            for d in missing:
                print(f"    {d}")
        else:
            for d in missing[:15]:
                print(f"    {d}")
            print(f"    ... ({len(missing) - 20} more) ...")
            for d in missing[-5:]:
                print(f"    {d}")
    else:
        print("  No gaps found -- all dates in the sequence are present.")

    # Partition count summary
    total_partitions = len(partition_files)
    print(f"\n  Total partition files: {total_partitions}")
    if partition_files:
        first = partition_files[0].stem.replace("events_", "")
        last = partition_files[-1].stem.replace("events_", "")
        print(f"  Date range: {first} to {last}")
        expected = (datetime.strptime(last, "%Y-%m-%d") -
                    datetime.strptime(first, "%Y-%m-%d")).days + 1
        coverage = (total_partitions / expected * 100) if expected > 0 else 0
        print(f"  Expected partitions: {expected}")
        print(f"  Coverage: {coverage:.1f}%")
        print(f"  Missing: {expected - total_partitions}")

    print(f"\n{'=' * 80}")
    print("  FRESHNESS CHECK COMPLETE")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
