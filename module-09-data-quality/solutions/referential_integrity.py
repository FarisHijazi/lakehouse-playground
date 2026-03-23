"""
Exercise 3: Referential Integrity Checks
==========================================
Validates foreign key relationships across all raw datasets.
Reports orphaned records for each relationship.
"""

import json
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


def load_listening_events() -> pd.DataFrame:
    events_dir = RAW_DIR / "listening_events"
    frames = []
    for f in sorted(events_dir.glob("events_*.jsonl")):
        lines = f.read_text().strip().split("\n")
        records = [json.loads(line) for line in lines if line.strip()]
        if records:
            frames.append(pd.DataFrame(records))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def check_referential_integrity(
    child_df: pd.DataFrame,
    child_col: str,
    parent_df: pd.DataFrame,
    parent_col: str,
    child_name: str,
    parent_name: str,
) -> dict:
    """Check that all values in child_col exist in parent_col."""
    child_keys = set(child_df[child_col].dropna().unique())
    parent_keys = set(parent_df[parent_col].dropna().unique())
    orphans = child_keys - parent_keys
    total = len(child_keys)
    orphan_count = len(orphans)
    orphan_pct = (orphan_count / total * 100) if total > 0 else 0.0

    return {
        "relationship": f"{child_name}.{child_col} -> {parent_name}.{parent_col}",
        "child_distinct_keys": total,
        "parent_distinct_keys": len(parent_keys),
        "orphaned_keys": orphan_count,
        "orphan_pct": round(orphan_pct, 2),
        "passed": orphan_count == 0,
        "sample_orphans": sorted(list(orphans))[:10],
    }


def main():
    print("Loading datasets...")
    users = pd.read_csv(RAW_DIR / "users.csv")
    cdn = pd.read_csv(RAW_DIR / "cdn_logs.csv")
    podcasts = pd.DataFrame(json.loads((RAW_DIR / "podcasts.json").read_text()))
    episodes = pd.DataFrame(json.loads((RAW_DIR / "episodes.json").read_text()))
    ad_events = pd.DataFrame(json.loads((RAW_DIR / "ad_events.json").read_text()))
    events = load_listening_events()

    print(f"\n{'=' * 90}")
    print("  REFERENTIAL INTEGRITY REPORT")
    print(f"{'=' * 90}")

    checks = [
        # listening_events.user_id -> users.user_id
        (events, "user_id", users, "user_id", "listening_events", "users"),
        # listening_events.episode_id -> episodes.episode_id
        (events, "episode_id", episodes, "episode_id", "listening_events", "episodes"),
        # episodes.podcast_id -> podcasts.podcast_id
        (episodes, "podcast_id", podcasts, "podcast_id", "episodes", "podcasts"),
        # cdn_logs.event_id -> listening_events.event_id
        (cdn, "event_id", events, "event_id", "cdn_logs", "listening_events"),
        # cdn_logs.user_id -> users.user_id
        (cdn, "user_id", users, "user_id", "cdn_logs", "users"),
        # ad_events.event_id -> listening_events.event_id
        (ad_events, "event_id", events, "event_id", "ad_events", "listening_events"),
        # ad_events.user_id -> users.user_id
        (ad_events, "user_id", users, "user_id", "ad_events", "users"),
    ]

    results = []
    for child_df, child_col, parent_df, parent_col, child_name, parent_name in checks:
        result = check_referential_integrity(
            child_df, child_col, parent_df, parent_col, child_name, parent_name
        )
        results.append(result)

    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"\n  [{status}] {r['relationship']}")
        print(f"         Child distinct keys: {r['child_distinct_keys']:,}")
        print(f"         Parent distinct keys: {r['parent_distinct_keys']:,}")
        print(f"         Orphaned keys: {r['orphaned_keys']:,} ({r['orphan_pct']}%)")
        if r["sample_orphans"]:
            print(f"         Sample orphans: {r['sample_orphans']}")

    passed = sum(1 for r in results if r["passed"])
    failed = sum(1 for r in results if not r["passed"])
    print(f"\n{'-' * 90}")
    print(f"  Summary: {passed} passed, {failed} failed out of {len(results)} checks")
    print(f"{'=' * 90}")


if __name__ == "__main__":
    main()
