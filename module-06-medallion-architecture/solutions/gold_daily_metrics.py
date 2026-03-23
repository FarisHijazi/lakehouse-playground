"""
Module 06 - Exercise 5: Gold Layer -- Daily Listening Metrics
==============================================================
Aggregate daily platform-wide listening metrics for dashboards.
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def main() -> None:
    print("=" * 60)
    print("  GOLD LAYER: DAILY LISTENING METRICS")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Read Silver events
    # -----------------------------------------------------------------------
    df = pd.read_parquet(SILVER_DIR / "listening_events.parquet")
    print(f"\n[1] Silver events loaded: {len(df):,}")

    # Ensure event_date is string for grouping consistency
    df["event_date"] = df["event_date"].astype(str)

    # -----------------------------------------------------------------------
    # 2. Compute daily metrics
    # -----------------------------------------------------------------------
    play_events = df[df["event_type"] == "play"]

    # All events aggregation
    all_agg = df.groupby("event_date").agg(
        dau=("user_id", "nunique"),
        total_events=("event_id", "count"),
        unique_episodes=("episode_id", "nunique"),
        unique_podcasts=("podcast_id", "nunique"),
    )

    # Play events aggregation
    play_agg = play_events.groupby("event_date").agg(
        total_listens=("event_id", "count"),
        total_listened_minutes=("listened_minutes", "sum"),
        avg_listened_minutes=("listened_minutes", "mean"),
        avg_completion_rate=("completion_rate", "mean"),
    )

    # Merge
    daily = all_agg.join(play_agg, how="left").reset_index()
    daily = daily.fillna({"total_listens": 0, "total_listened_minutes": 0})

    # Compute hours
    daily["total_listened_hours"] = (daily["total_listened_minutes"] / 60).round(2)
    daily["avg_listened_minutes"] = daily["avg_listened_minutes"].round(2)
    daily["avg_completion_rate"] = daily["avg_completion_rate"].round(4)

    # Sort by date
    daily = daily.sort_values("event_date").reset_index(drop=True)

    # -----------------------------------------------------------------------
    # 3. Rolling metrics
    # -----------------------------------------------------------------------
    daily["dau_7d_avg"] = daily["dau"].rolling(7, min_periods=1).mean().round(1)
    daily["listens_7d_avg"] = daily["total_listens"].rolling(7, min_periods=1).mean().round(1)

    # -----------------------------------------------------------------------
    # 4. Select columns & write
    # -----------------------------------------------------------------------
    out_cols = [
        "event_date", "dau", "total_events", "total_listens",
        "total_listened_hours", "avg_listened_minutes", "avg_completion_rate",
        "unique_episodes", "unique_podcasts",
        "dau_7d_avg", "listens_7d_avg",
    ]
    daily = daily[out_cols]

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    daily.to_parquet(GOLD_DIR / "daily_listening_metrics.parquet", engine="pyarrow", index=False)
    print(f"\n[4] Written to: {GOLD_DIR / 'daily_listening_metrics.parquet'}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  DAILY LISTENING METRICS SUMMARY")
    print(f"{'='*60}")
    print(f"  Date range: {daily['event_date'].min()} to {daily['event_date'].max()}")
    print(f"  Total days: {len(daily):,}")
    print(f"  Average DAU: {daily['dau'].mean():.0f}")
    peak_idx = daily["dau"].idxmax()
    print(f"  Peak DAU: {daily.loc[peak_idx, 'dau']:,} on {daily.loc[peak_idx, 'event_date']}")
    print(f"  Avg completion rate: {daily['avg_completion_rate'].mean():.3f}")

    print(f"\n  First 5 days:")
    print(daily.head().to_string(index=False))
    print(f"\n  Last 5 days:")
    print(daily.tail().to_string(index=False))


if __name__ == "__main__":
    main()
