"""
Exercise 6: Quality Scoring System
====================================
Assigns a 0-100 weighted quality score to each dataset across five dimensions:
  - Completeness (30%): non-null rate on required columns
  - Uniqueness (20%): deduplication rate on key columns
  - Validity (25%): values passing range / pattern / accepted-value checks
  - Consistency (15%): values matching expected formats
  - Timeliness (10%): whether data meets freshness SLO
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"

WEIGHTS = {
    "completeness": 0.30,
    "uniqueness": 0.20,
    "validity": 0.25,
    "consistency": 0.15,
    "timeliness": 0.10,
}


def score_completeness(df: pd.DataFrame, required_columns: list[str]) -> float:
    """Percentage of non-null values across required columns."""
    if not required_columns:
        return 100.0
    total_cells = len(df) * len(required_columns)
    if total_cells == 0:
        return 100.0
    null_cells = sum(df[col].isnull().sum() for col in required_columns if col in df.columns)
    # Count empty strings too
    for col in required_columns:
        if col in df.columns and df[col].dtype == object:
            null_cells += (df[col] == "").sum()
    return max(0.0, (1 - null_cells / total_cells) * 100)


def score_uniqueness(df: pd.DataFrame, key_columns: list[str]) -> float:
    """Percentage of unique values in key columns (100 = no duplicates)."""
    if not key_columns:
        return 100.0
    scores = []
    for col in key_columns:
        if col not in df.columns:
            continue
        non_null = df[col].dropna()
        if len(non_null) == 0:
            scores.append(100.0)
            continue
        unique_count = non_null.nunique()
        scores.append(unique_count / len(non_null) * 100)
    return sum(scores) / len(scores) if scores else 100.0


def score_validity(df: pd.DataFrame, checks: list[dict]) -> float:
    """Percentage of values passing validity checks."""
    if not checks:
        return 100.0
    total_pass = 0
    total_checked = 0
    for check in checks:
        col = check["column"]
        if col not in df.columns:
            continue
        series = df[col].dropna()
        total_checked += len(series)
        if check["type"] == "range":
            valid = series
            if "min" in check:
                valid = valid[pd.to_numeric(valid, errors="coerce") >= check["min"]]
            if "max" in check:
                valid = valid[pd.to_numeric(valid, errors="coerce") <= check["max"]]
            total_pass += len(valid)
        elif check["type"] == "pattern":
            matches = series.astype(str).str.match(check["pattern"])
            total_pass += int(matches.sum())
        elif check["type"] == "accepted_values":
            total_pass += int(series.isin(check["values"]).sum())
    return (total_pass / total_checked * 100) if total_checked > 0 else 100.0


def score_consistency(df: pd.DataFrame, format_checks: list[dict]) -> float:
    """Percentage of values matching expected format (e.g., date format, gender codes)."""
    if not format_checks:
        return 100.0
    total_pass = 0
    total_checked = 0
    for check in format_checks:
        col = check["column"]
        if col not in df.columns:
            continue
        series = df[col].dropna().astype(str)
        total_checked += len(series)
        matches = series.str.match(check["pattern"])
        total_pass += int(matches.sum())
    return (total_pass / total_checked * 100) if total_checked > 0 else 100.0


def score_timeliness(latest_timestamp: datetime | None, max_hours: int) -> float:
    """100 if within SLO, scaled down based on how late."""
    if latest_timestamp is None:
        return 0.0
    gap_hours = (datetime.now() - latest_timestamp).total_seconds() / 3600
    if gap_hours <= max_hours:
        return 100.0
    # Scale: at 2x the SLO, score = 50; at 3x, score = 0
    ratio = gap_hours / max_hours
    return max(0.0, (1 - (ratio - 1) / 2) * 100)


def classify_score(score: float) -> str:
    if score >= 95:
        return "EXCELLENT"
    elif score >= 85:
        return "GOOD"
    elif score >= 70:
        return "FAIR"
    elif score >= 50:
        return "POOR"
    else:
        return "CRITICAL"


def score_dataset(
    name: str,
    df: pd.DataFrame,
    required_columns: list[str],
    key_columns: list[str],
    validity_checks: list[dict],
    format_checks: list[dict],
    latest_timestamp: datetime | None,
    freshness_slo_hours: int,
) -> dict:
    comp = score_completeness(df, required_columns)
    uniq = score_uniqueness(df, key_columns)
    valid = score_validity(df, validity_checks)
    cons = score_consistency(df, format_checks)
    timely = score_timeliness(latest_timestamp, freshness_slo_hours)

    overall = (
        comp * WEIGHTS["completeness"]
        + uniq * WEIGHTS["uniqueness"]
        + valid * WEIGHTS["validity"]
        + cons * WEIGHTS["consistency"]
        + timely * WEIGHTS["timeliness"]
    )

    return {
        "dataset": name,
        "completeness": round(comp, 2),
        "uniqueness": round(uniq, 2),
        "validity": round(valid, 2),
        "consistency": round(cons, 2),
        "timeliness": round(timely, 2),
        "overall": round(overall, 2),
        "classification": classify_score(overall),
    }


def get_latest_ts(df: pd.DataFrame, col: str) -> datetime | None:
    parsed = pd.to_datetime(df[col], errors="coerce", format="mixed")
    valid = parsed.dropna()
    return valid.max().to_pydatetime() if not valid.empty else None


def main():
    print(f"{'=' * 100}")
    print("  DATA QUALITY SCORECARD")
    print(f"{'=' * 100}")

    results = []

    # --- Users ---
    users = pd.read_csv(RAW_DIR / "users.csv")
    results.append(score_dataset(
        name="users",
        df=users,
        required_columns=["user_id", "name", "email", "country", "platform",
                          "signup_date", "subscription_type"],
        key_columns=["user_id", "email"],
        validity_checks=[
            {"column": "age", "type": "range", "min": 13, "max": 120},
            {"column": "subscription_type", "type": "accepted_values",
             "values": ["free", "premium", "premium_annual", "trial"]},
            {"column": "platform", "type": "accepted_values",
             "values": ["ios", "android", "web", "car_play", "smart_speaker"]},
        ],
        format_checks=[
            {"column": "signup_date", "pattern": r"^\d{4}-\d{2}-\d{2}$"},
            {"column": "gender", "pattern": r"^(m|f|male|female)$"},
            {"column": "user_id", "pattern": r"^usr_\d{6}$"},
        ],
        latest_timestamp=get_latest_ts(users, "signup_date"),
        freshness_slo_hours=168,
    ))

    # --- CDN Logs ---
    cdn = pd.read_csv(RAW_DIR / "cdn_logs.csv")
    results.append(score_dataset(
        name="cdn_logs",
        df=cdn,
        required_columns=["log_id", "event_id", "user_id", "timestamp", "cdn_node",
                          "bytes_transferred"],
        key_columns=["log_id"],
        validity_checks=[
            {"column": "startup_time_ms", "type": "range", "min": 0},
            {"column": "bytes_transferred", "type": "range", "min": 1},
            {"column": "rebuffer_ratio", "type": "range", "min": 0.0, "max": 1.0},
            {"column": "bitrate", "type": "accepted_values",
             "values": ["64kbps", "128kbps", "256kbps", "320kbps"]},
        ],
        format_checks=[
            {"column": "timestamp",
             "pattern": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"},
        ],
        latest_timestamp=get_latest_ts(cdn, "timestamp"),
        freshness_slo_hours=24,
    ))

    # --- Podcasts ---
    podcasts = pd.DataFrame(json.loads((RAW_DIR / "podcasts.json").read_text()))
    results.append(score_dataset(
        name="podcasts",
        df=podcasts,
        required_columns=["podcast_id", "name", "category", "language", "host"],
        key_columns=["podcast_id"],
        validity_checks=[],
        format_checks=[
            {"column": "podcast_id", "pattern": r"^pod_\d{3}$"},
        ],
        latest_timestamp=get_latest_ts(podcasts, "created_at"),
        freshness_slo_hours=720,
    ))

    # --- Episodes ---
    episodes = pd.DataFrame(json.loads((RAW_DIR / "episodes.json").read_text()))
    results.append(score_dataset(
        name="episodes",
        df=episodes,
        required_columns=["episode_id", "podcast_id", "title", "published_at",
                          "duration_seconds"],
        key_columns=["episode_id"],
        validity_checks=[
            {"column": "duration_seconds", "type": "range", "min": 1},
        ],
        format_checks=[
            {"column": "episode_id", "pattern": r"^ep_\d{4}$"},
        ],
        latest_timestamp=get_latest_ts(episodes, "published_at"),
        freshness_slo_hours=168,
    ))

    # --- Ad Events ---
    ad_events = pd.DataFrame(json.loads((RAW_DIR / "ad_events.json").read_text()))
    results.append(score_dataset(
        name="ad_events",
        df=ad_events,
        required_columns=["ad_event_id", "event_id", "user_id", "timestamp",
                          "ad_type", "action", "advertiser", "campaign_id"],
        key_columns=["ad_event_id"],
        validity_checks=[
            {"column": "revenue_sar", "type": "range", "min": 0},
            {"column": "duration_seconds", "type": "range", "min": 1},
            {"column": "ad_type", "type": "accepted_values",
             "values": ["pre_roll", "mid_roll", "post_roll"]},
            {"column": "action", "type": "accepted_values",
             "values": ["impression", "click", "skip", "complete"]},
        ],
        format_checks=[
            {"column": "timestamp",
             "pattern": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"},
        ],
        latest_timestamp=get_latest_ts(ad_events, "timestamp"),
        freshness_slo_hours=48,
    ))

    # --- Listening Events (sample for speed) ---
    events_dir = RAW_DIR / "listening_events"
    # Load a sample of recent partition files
    partition_files = sorted(events_dir.glob("events_*.jsonl"))
    sample_files = partition_files[-30:] if len(partition_files) > 30 else partition_files
    frames = []
    for f in sample_files:
        lines = f.read_text().strip().split("\n")
        records = [json.loads(line) for line in lines if line.strip()]
        if records:
            frames.append(pd.DataFrame(records))
    events_sample = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    latest_partition = None
    if partition_files:
        date_str = partition_files[-1].stem.replace("events_", "")
        try:
            latest_partition = datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            pass

    results.append(score_dataset(
        name="listening_events",
        df=events_sample,
        required_columns=["event_id", "user_id", "episode_id", "event_type",
                          "timestamp", "listened_seconds"],
        key_columns=["event_id"],
        validity_checks=[
            {"column": "listened_seconds", "type": "range", "min": 0, "max": 36000},
            {"column": "event_type", "type": "accepted_values",
             "values": ["play", "pause", "seek", "complete", "skip"]},
            {"column": "platform", "type": "accepted_values",
             "values": ["ios", "android", "web", "car_play", "smart_speaker"]},
        ],
        format_checks=[
            {"column": "timestamp",
             "pattern": r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"},
            {"column": "event_id",
             "pattern": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"},
        ],
        latest_timestamp=latest_partition,
        freshness_slo_hours=24,
    ))

    # Print scorecard
    print(f"\n  {'Dataset':<22s} {'Complete':>10s} {'Unique':>10s} {'Valid':>10s} "
          f"{'Consistent':>12s} {'Timely':>10s} {'Overall':>10s} {'Grade'}")
    print(f"  {'-' * 100}")
    for r in results:
        print(f"  {r['dataset']:<22s} {r['completeness']:>10.1f} {r['uniqueness']:>10.1f} "
              f"{r['validity']:>10.1f} {r['consistency']:>12.1f} {r['timeliness']:>10.1f} "
              f"{r['overall']:>10.1f} {r['classification']}")

    # Dimension weights reminder
    print(f"\n  Weights: completeness={WEIGHTS['completeness']:.0%}, "
          f"uniqueness={WEIGHTS['uniqueness']:.0%}, "
          f"validity={WEIGHTS['validity']:.0%}, "
          f"consistency={WEIGHTS['consistency']:.0%}, "
          f"timeliness={WEIGHTS['timeliness']:.0%}")

    print(f"\n{'=' * 100}")
    print("  QUALITY SCORING COMPLETE")
    print(f"{'=' * 100}")

    return results


if __name__ == "__main__":
    main()
