"""
Exercise 5: Volume Anomaly Detection
======================================
Counts daily listening events and flags anomalous days using:
  - Rolling average with standard deviation bands (2-sigma)
  - Day-over-day percentage change (>50% drop/spike)
  - Week-over-week comparison (same weekday)
"""

from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


def count_events_per_partition(events_dir: Path) -> pd.DataFrame:
    """Count lines in each partition file to get daily event counts."""
    records = []
    for f in sorted(events_dir.glob("events_*.jsonl")):
        date_str = f.stem.replace("events_", "")
        line_count = sum(1 for line in f.open() if line.strip())
        records.append({"date": date_str, "event_count": line_count})
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def detect_anomalies(daily: pd.DataFrame) -> pd.DataFrame:
    """Add anomaly detection columns."""
    df = daily.copy()

    # Rolling 7-day statistics
    df["rolling_mean_7d"] = df["event_count"].rolling(window=7, min_periods=3).mean()
    df["rolling_std_7d"] = df["event_count"].rolling(window=7, min_periods=3).std()
    df["upper_2sigma"] = df["rolling_mean_7d"] + 2 * df["rolling_std_7d"]
    df["lower_2sigma"] = df["rolling_mean_7d"] - 2 * df["rolling_std_7d"]
    df["lower_2sigma"] = df["lower_2sigma"].clip(lower=0)

    # Sigma anomaly
    df["sigma_anomaly"] = (
        (df["event_count"] > df["upper_2sigma"]) |
        (df["event_count"] < df["lower_2sigma"])
    )

    # Day-over-day percentage change
    df["prev_day_count"] = df["event_count"].shift(1)
    df["dod_pct_change"] = (
        (df["event_count"] - df["prev_day_count"]).abs() /
        df["prev_day_count"] * 100
    )
    df["dod_anomaly"] = df["dod_pct_change"] > 50.0

    # Week-over-week (same weekday comparison)
    df["same_weekday_last_week"] = df["event_count"].shift(7)
    df["wow_pct_change"] = (
        (df["event_count"] - df["same_weekday_last_week"]).abs() /
        df["same_weekday_last_week"] * 100
    )
    df["wow_anomaly"] = df["wow_pct_change"] > 50.0

    # Combined anomaly flag
    df["is_anomaly"] = df["sigma_anomaly"] | df["dod_anomaly"]

    return df


def main():
    events_dir = RAW_DIR / "listening_events"
    print("Counting events per partition...")
    daily = count_events_per_partition(events_dir)

    print(f"\n{'=' * 100}")
    print("  VOLUME ANOMALY DETECTION REPORT")
    print(f"{'=' * 100}")
    print(f"  Total partitions: {len(daily)}")
    print(f"  Date range: {daily['date'].min().date()} to {daily['date'].max().date()}")
    print(f"  Total events: {daily['event_count'].sum():,}")
    print(f"  Daily average: {daily['event_count'].mean():.1f}")
    print(f"  Daily std dev: {daily['event_count'].std():.1f}")
    print(f"  Daily min: {daily['event_count'].min()}")
    print(f"  Daily max: {daily['event_count'].max()}")

    df = detect_anomalies(daily)

    # Show anomalous days
    anomalies = df[df["is_anomaly"]].copy()
    print(f"\n  --- Anomalous Days ({len(anomalies)} found) ---")
    if not anomalies.empty:
        print(f"\n  {'Date':<14s} {'Count':>8s} {'Rolling Mean':>14s} "
              f"{'Lower':>10s} {'Upper':>10s} {'DoD %':>8s} {'Reason'}")
        print(f"  {'-' * 90}")
        for _, row in anomalies.iterrows():
            reasons = []
            if row.get("sigma_anomaly"):
                reasons.append("2-sigma")
            if row.get("dod_anomaly"):
                reasons.append(f"DoD>{row['dod_pct_change']:.0f}%")
            reason_str = ", ".join(reasons)
            print(f"  {str(row['date'].date()):<14s} {row['event_count']:>8,} "
                  f"{row['rolling_mean_7d']:>14.1f} "
                  f"{row['lower_2sigma']:>10.1f} {row['upper_2sigma']:>10.1f} "
                  f"{row.get('dod_pct_change', 0):>8.1f} {reason_str}")
    else:
        print("  No anomalies detected.")

    # Week-over-week anomalies
    wow_anomalies = df[df["wow_anomaly"]].copy()
    print(f"\n  --- Week-over-Week Anomalies ({len(wow_anomalies)} found) ---")
    if not wow_anomalies.empty:
        print(f"\n  {'Date':<14s} {'Count':>8s} {'Same Day Last Wk':>18s} {'WoW %':>8s}")
        print(f"  {'-' * 55}")
        for _, row in wow_anomalies.head(20).iterrows():
            print(f"  {str(row['date'].date()):<14s} {row['event_count']:>8,} "
                  f"{row['same_weekday_last_week']:>18.0f} "
                  f"{row['wow_pct_change']:>8.1f}")
        if len(wow_anomalies) > 20:
            print(f"  ... and {len(wow_anomalies) - 20} more")
    else:
        print("  No week-over-week anomalies detected.")

    # Day-of-week summary
    df["weekday"] = df["date"].dt.day_name()
    weekday_stats = df.groupby("weekday")["event_count"].agg(["mean", "std", "min", "max"])
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_stats = weekday_stats.reindex(day_order)

    print(f"\n  --- Day-of-Week Statistics ---")
    print(f"  {'Day':<12s} {'Mean':>8s} {'Std':>8s} {'Min':>8s} {'Max':>8s}")
    print(f"  {'-' * 48}")
    for day, row in weekday_stats.iterrows():
        print(f"  {day:<12s} {row['mean']:>8.1f} {row['std']:>8.1f} "
              f"{row['min']:>8.0f} {row['max']:>8.0f}")

    print(f"\n{'=' * 100}")
    print("  VOLUME ANOMALY DETECTION COMPLETE")
    print(f"{'=' * 100}")


if __name__ == "__main__":
    main()
