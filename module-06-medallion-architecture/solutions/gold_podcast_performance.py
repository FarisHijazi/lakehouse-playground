"""
Module 06 - Exercise 6: Gold Layer -- Podcast Performance
==========================================================
Per-podcast aggregate performance metrics for content analytics.
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def main() -> None:
    print("=" * 60)
    print("  GOLD LAYER: PODCAST PERFORMANCE")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Read Silver events + Bronze podcast metadata
    # -----------------------------------------------------------------------
    events = pd.read_parquet(SILVER_DIR / "listening_events.parquet")
    podcasts = pd.read_parquet(BRONZE_DIR / "podcasts")
    episodes = pd.read_parquet(BRONZE_DIR / "episodes")

    print(f"\n[1] Data loaded:")
    print(f"    Silver events: {len(events):,}")
    print(f"    Podcasts:      {len(podcasts):,}")
    print(f"    Episodes:      {len(episodes):,}")

    # Filter to play events only for listen metrics
    play_events = events[events["event_type"] == "play"].copy()

    # -----------------------------------------------------------------------
    # 2. Compute per-podcast metrics
    # -----------------------------------------------------------------------
    perf = play_events.groupby("podcast_id").agg(
        total_listens=("event_id", "count"),
        unique_listeners=("user_id", "nunique"),
        total_listened_minutes=("listened_minutes", "sum"),
        avg_completion_rate=("completion_rate", "mean"),
        total_episodes=("episode_id", "nunique"),
    ).reset_index()

    perf["total_listened_hours"] = (perf["total_listened_minutes"] / 60).round(2)
    perf["avg_listens_per_episode"] = (
        perf["total_listens"] / perf["total_episodes"]
    ).round(1)
    perf["avg_completion_rate"] = perf["avg_completion_rate"].round(4)

    # -----------------------------------------------------------------------
    # 3. Listener retention rate (users who listened to >1 episode per podcast)
    # -----------------------------------------------------------------------
    user_episode_counts = (
        play_events.groupby(["podcast_id", "user_id"])["episode_id"]
        .nunique()
        .reset_index()
        .rename(columns={"episode_id": "episodes_listened"})
    )
    retention = (
        user_episode_counts.groupby("podcast_id")
        .apply(
            lambda g: (g["episodes_listened"] > 1).sum() / len(g) if len(g) > 0 else 0,
            include_groups=False,
        )
        .reset_index()
        .rename(columns={0: "listener_retention_rate"})
    )
    perf = perf.merge(retention, on="podcast_id", how="left")
    perf["listener_retention_rate"] = perf["listener_retention_rate"].round(4)

    # -----------------------------------------------------------------------
    # 4. Join with podcast metadata
    # -----------------------------------------------------------------------
    pod_meta = podcasts[["podcast_id", "name_en", "category", "language"]].drop_duplicates(
        subset=["podcast_id"]
    )
    perf = perf.merge(pod_meta, on="podcast_id", how="left")

    # -----------------------------------------------------------------------
    # 5. Rank by total listens
    # -----------------------------------------------------------------------
    perf = perf.sort_values("total_listens", ascending=False).reset_index(drop=True)
    perf["rank"] = range(1, len(perf) + 1)

    # -----------------------------------------------------------------------
    # 6. Select columns & write
    # -----------------------------------------------------------------------
    out_cols = [
        "rank", "podcast_id", "name_en", "category", "language",
        "total_listens", "unique_listeners", "total_listened_hours",
        "avg_completion_rate", "total_episodes", "avg_listens_per_episode",
        "listener_retention_rate",
    ]
    perf = perf[out_cols]

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    perf.to_parquet(GOLD_DIR / "podcast_performance.parquet", engine="pyarrow", index=False)
    print(f"\n[6] Written to: {GOLD_DIR / 'podcast_performance.parquet'}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  PODCAST PERFORMANCE SUMMARY")
    print(f"{'='*60}")
    print(f"  Total podcasts with listens: {len(perf):,}")
    print(f"  Total unique listeners: {play_events['user_id'].nunique():,}")

    print(f"\n  Top 10 podcasts by listens:")
    top10 = perf.head(10)
    print(top10[["rank", "name_en", "total_listens", "unique_listeners",
                  "avg_completion_rate", "listener_retention_rate"]].to_string(index=False))

    print(f"\n  Category breakdown:")
    cat_agg = perf.groupby("category").agg(
        podcasts=("podcast_id", "count"),
        total_listens=("total_listens", "sum"),
    ).sort_values("total_listens", ascending=False)
    print(cat_agg.to_string())


if __name__ == "__main__":
    main()
