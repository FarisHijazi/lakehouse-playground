"""
Module 06 - Exercise 3: Silver Layer -- Listening Events
=========================================================
Deduplicate, validate, enrich, and clean Bronze listening events.
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

VALID_EVENT_TYPES = {"play", "pause", "resume", "complete", "skip", "seek"}


def main() -> None:
    print("=" * 60)
    print("  SILVER LAYER: LISTENING EVENTS")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Read Bronze (partitioned -- glob all parquet files)
    # -----------------------------------------------------------------------
    events_dir = BRONZE_DIR / "listening_events"
    parquet_files = sorted(events_dir.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {events_dir}")
    df = pd.concat(
        [pd.read_parquet(f) for f in parquet_files], ignore_index=True
    )
    bronze_count = len(df)
    print(f"\n[1] Bronze records loaded: {bronze_count:,}")

    # -----------------------------------------------------------------------
    # 2. Deduplicate by event_id
    # -----------------------------------------------------------------------
    before = len(df)
    df = df.drop_duplicates(subset=["event_id"], keep="first")
    dupes = before - len(df)
    print(f"\n[2] Deduplication: {dupes:,} duplicate event_id records removed")

    # -----------------------------------------------------------------------
    # 3. Filter bot / test events
    # -----------------------------------------------------------------------
    before = len(df)
    bot_mask = df["user_id"].astype(str).str.startswith("usr_000000")
    test_mask = df["app_version"].astype(str) == "0.0.0"
    filter_mask = bot_mask | test_mask
    bot_count = filter_mask.sum()
    df = df[~filter_mask].copy()
    print(f"\n[3] Bot/test filter: {bot_count:,} events removed")
    print(f"    (bot users: {bot_mask.sum():,}, test app_version: {test_mask.sum():,})")

    # -----------------------------------------------------------------------
    # 4. Type casting
    # -----------------------------------------------------------------------
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["listened_seconds"] = pd.to_numeric(df["listened_seconds"], errors="coerce")

    # -----------------------------------------------------------------------
    # 5. Validate & quarantine
    # -----------------------------------------------------------------------
    invalid_mask = pd.Series(False, index=df.index)

    # listened_seconds must be >= 0
    neg_listen = df["listened_seconds"] < 0
    invalid_mask |= neg_listen

    # event_type must be valid
    bad_type = ~df["event_type"].isin(VALID_EVENT_TYPES)
    invalid_mask |= bad_type

    # timestamp must be valid
    bad_ts = df["timestamp"].isna()
    invalid_mask |= bad_ts

    print(f"\n[5] Validation:")
    print(f"    Negative listened_seconds: {neg_listen.sum():,}")
    print(f"    Invalid event_type: {bad_type.sum():,}")
    print(f"    Invalid timestamp: {bad_ts.sum():,}")

    quarantine = df[invalid_mask].copy()
    df = df[~invalid_mask].copy()
    print(f"    Quarantined: {len(quarantine):,}")

    # -----------------------------------------------------------------------
    # 6. Add derived columns
    # -----------------------------------------------------------------------
    df["event_date"] = df["timestamp"].dt.date
    df["event_hour"] = df["timestamp"].dt.hour
    df["listened_minutes"] = (df["listened_seconds"] / 60).round(2)

    # -----------------------------------------------------------------------
    # 7. Join with episodes for podcast_id and completion rate
    # -----------------------------------------------------------------------
    episodes = pd.read_parquet(BRONZE_DIR / "episodes")
    episodes = episodes[["episode_id", "podcast_id", "duration_seconds"]].copy()
    episodes["duration_seconds"] = pd.to_numeric(
        episodes["duration_seconds"], errors="coerce"
    )
    # Deduplicate episodes in case of duplicates in bronze
    episodes = episodes.drop_duplicates(subset=["episode_id"], keep="first")

    before_join = len(df)
    df = df.merge(episodes, on="episode_id", how="left")
    print(f"\n[7] Episode join: {before_join:,} events -> {len(df):,} after join")
    print(f"    Episodes matched: {df['podcast_id'].notna().sum():,}")
    print(f"    Episodes unmatched: {df['podcast_id'].isna().sum():,}")

    # Completion rate: listened_seconds / duration_seconds, capped at 1.0
    df["completion_rate"] = (df["listened_seconds"] / df["duration_seconds"]).clip(
        upper=1.0
    )
    df.loc[df["duration_seconds"].isna() | (df["duration_seconds"] <= 0), "completion_rate"] = None

    # -----------------------------------------------------------------------
    # 8. Select final columns & write
    # -----------------------------------------------------------------------
    keep_cols = [
        "event_id", "user_id", "episode_id", "podcast_id",
        "event_type", "timestamp", "event_date", "event_hour",
        "listened_seconds", "listened_minutes", "completion_rate",
        "platform", "country", "app_version",
    ]
    df = df[keep_cols].reset_index(drop=True)

    # Cast event_date to string for Parquet compatibility
    df["event_date"] = df["event_date"].astype(str)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_DIR / "listening_events.parquet", engine="pyarrow", index=False)
    print(f"\n[8] Written to: {SILVER_DIR / 'listening_events.parquet'}")

    if len(quarantine) > 0:
        quarantine.to_parquet(
            SILVER_DIR / "listening_events_quarantine.parquet",
            engine="pyarrow",
            index=False,
        )
        print(f"    Quarantine: {SILVER_DIR / 'listening_events_quarantine.parquet'}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  SILVER LISTENING EVENTS SUMMARY")
    print(f"{'='*60}")
    print(f"  Bronze input:        {bronze_count:,}")
    print(f"  Duplicates removed:  {dupes:,}")
    print(f"  Bot/test filtered:   {bot_count:,}")
    print(f"  Quarantined:         {len(quarantine):,}")
    print(f"  Silver output:       {len(df):,}")

    print(f"\n  Event type distribution:")
    for et, cnt in df["event_type"].value_counts().items():
        print(f"    {et}: {cnt:,}")

    play_events = df[df["event_type"] == "play"]
    if len(play_events) > 0:
        print(f"\n  Completion rate stats (play events only):")
        cr = play_events["completion_rate"].dropna()
        print(f"    Mean:   {cr.mean():.3f}")
        print(f"    Median: {cr.median():.3f}")
        print(f"    Min:    {cr.min():.3f}")
        print(f"    Max:    {cr.max():.3f}")

    print(f"\n  Sample output (first 5 rows):")
    print(df.head().to_string(index=False, max_colwidth=30))


if __name__ == "__main__":
    main()
