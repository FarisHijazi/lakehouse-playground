"""
Exercise 1: Profile All Raw Datasets
=====================================
Loads every raw dataset and produces a detailed quality profile:
  - Shape (rows, columns)
  - Null counts and percentages
  - Duplicate rows and duplicate keys
  - Basic statistics for numeric columns
  - Value distributions for string columns
  - Specific quality issues discovered
"""

import json
from pathlib import Path

import duckdb
import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


def load_listening_events() -> pd.DataFrame:
    """Load all listening event JSONL partition files into one dataframe."""
    events_dir = RAW_DIR / "listening_events"
    frames = []
    for f in sorted(events_dir.glob("events_*.jsonl")):
        lines = f.read_text().strip().split("\n")
        records = [json.loads(line) for line in lines if line.strip()]
        if records:
            frames.append(pd.DataFrame(records))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def profile_dataset(name: str, df: pd.DataFrame, key_columns: list[str] | None = None):
    """Print a detailed profile for one dataset."""
    print(f"\n{'=' * 80}")
    print(f"  DATASET: {name}")
    print(f"{'=' * 80}")
    print(f"  Rows: {len(df):,}   Columns: {len(df.columns)}")
    print(f"  Columns: {list(df.columns)}")

    # --- Nulls ---
    print(f"\n  --- Null Analysis ---")
    null_counts = df.isnull().sum()
    null_pct = (df.isnull().sum() / len(df) * 100).round(2)
    for col in df.columns:
        if null_counts[col] > 0:
            print(f"    {col}: {null_counts[col]:,} nulls ({null_pct[col]}%)")
    if null_counts.sum() == 0:
        print("    No nulls found.")

    # --- Duplicates ---
    print(f"\n  --- Duplicate Analysis ---")
    full_dupes = df.duplicated().sum()
    print(f"    Full row duplicates: {full_dupes:,}")
    if key_columns:
        for key_col in key_columns:
            if key_col in df.columns:
                dupe_count = df[key_col].dropna().duplicated().sum()
                print(f"    Duplicate {key_col}: {dupe_count:,}")

    # --- Numeric stats ---
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if numeric_cols:
        print(f"\n  --- Numeric Statistics ---")
        for col in numeric_cols:
            series = df[col].dropna()
            if len(series) == 0:
                continue
            print(f"    {col}:")
            print(f"      min={series.min()}, max={series.max()}, "
                  f"mean={series.mean():.2f}, std={series.std():.2f}")
            negatives = (series < 0).sum()
            if negatives > 0:
                print(f"      *** {negatives:,} NEGATIVE values found ***")

    # --- String value distributions ---
    string_cols = df.select_dtypes(include=["object"]).columns.tolist()
    if string_cols:
        print(f"\n  --- String Column Distributions ---")
        for col in string_cols:
            nunique = df[col].nunique()
            if nunique <= 20:
                print(f"    {col} ({nunique} unique):")
                vc = df[col].value_counts(dropna=False).head(10)
                for val, cnt in vc.items():
                    label = repr(val) if val is not None else "NULL"
                    print(f"      {label}: {cnt:,}")
            else:
                print(f"    {col}: {nunique:,} unique values")
                # Show sample
                sample = df[col].dropna().head(5).tolist()
                print(f"      Sample: {sample}")


def find_specific_issues(name: str, df: pd.DataFrame):
    """Check for known quality issues in each dataset."""
    issues = []

    if name == "users":
        # Mixed date formats
        if "signup_date" in df.columns:
            dates = df["signup_date"].dropna()
            iso_count = dates.str.match(r"^\d{4}-\d{2}-\d{2}$").sum()
            iso_t_count = dates.str.match(r"^\d{4}-\d{2}-\d{2}T").sum()
            slash_dmy = dates.str.match(r"^\d{2}/\d{2}/\d{4}$").sum()
            dash_mdy = dates.str.match(r"^\d{2}-\d{2}-\d{4}$").sum()
            other = len(dates) - iso_count - iso_t_count - slash_dmy - dash_mdy
            issues.append(f"signup_date has mixed formats: "
                          f"YYYY-MM-DD={iso_count}, YYYY-MM-DDThh:mm:ss={iso_t_count}, "
                          f"DD/MM/YYYY={slash_dmy}, MM-DD-YYYY={dash_mdy}, other={other}")

        # Inconsistent gender
        if "gender" in df.columns:
            genders = df["gender"].dropna().unique().tolist()
            issues.append(f"Gender values are inconsistent: {sorted(genders)}")
            empty_gender = (df["gender"] == "").sum()
            if empty_gender > 0:
                issues.append(f"Gender has {empty_gender} empty strings (not null)")

        # Missing emails
        if "email" in df.columns:
            null_emails = df["email"].isnull().sum()
            if null_emails > 0:
                issues.append(f"{null_emails} users have no email address")

        # Duplicate user IDs
        if "user_id" in df.columns:
            dup_ids = df["user_id"].dropna().duplicated()
            if dup_ids.sum() > 0:
                duped = df[df["user_id"].duplicated(keep=False)]["user_id"].unique()
                issues.append(f"Duplicate user_ids found: {list(duped)}")

    if name == "cdn_logs":
        if "startup_time_ms" in df.columns:
            negs = (df["startup_time_ms"] < 0).sum()
            if negs > 0:
                issues.append(f"startup_time_ms has {negs} negative values")
        if "bytes_transferred" in df.columns:
            negs = (df["bytes_transferred"] <= 0).sum()
            if negs > 0:
                issues.append(f"bytes_transferred has {negs} non-positive values")

    if name == "listening_events":
        if "listened_seconds" in df.columns:
            negs = (df["listened_seconds"] < 0).sum()
            if negs > 0:
                issues.append(f"listened_seconds has {negs} negative values")
        if "event_type" in df.columns:
            expected = {"play", "pause", "seek", "complete", "skip"}
            actual = set(df["event_type"].dropna().unique())
            unexpected = actual - expected
            if unexpected:
                issues.append(f"Unexpected event_type values: {unexpected}")
        if "event_id" in df.columns:
            dupes = df["event_id"].dropna().duplicated().sum()
            if dupes > 0:
                issues.append(f"event_id has {dupes} duplicates")

    if name == "ad_events":
        if "revenue_sar" in df.columns:
            zeros = (df["revenue_sar"] == 0).sum()
            negs = (df["revenue_sar"] < 0).sum()
            if zeros > 0:
                issues.append(f"revenue_sar has {zeros} zero values")
            if negs > 0:
                issues.append(f"revenue_sar has {negs} negative values")

    if issues:
        print(f"\n  --- Quality Issues Found ---")
        for i, issue in enumerate(issues, 1):
            print(f"    {i}. {issue}")
    else:
        print(f"\n  --- No specific issues flagged ---")


def main():
    print("=" * 80)
    print("  DATA QUALITY PROFILING REPORT")
    print("  All raw datasets at:", RAW_DIR)
    print("=" * 80)

    # 1. Users
    users = pd.read_csv(RAW_DIR / "users.csv")
    profile_dataset("users", users, key_columns=["user_id", "email"])
    find_specific_issues("users", users)

    # 2. CDN Logs
    cdn = pd.read_csv(RAW_DIR / "cdn_logs.csv")
    profile_dataset("cdn_logs", cdn, key_columns=["log_id", "event_id"])
    find_specific_issues("cdn_logs", cdn)

    # 3. Podcasts
    podcasts = pd.DataFrame(json.loads((RAW_DIR / "podcasts.json").read_text()))
    profile_dataset("podcasts", podcasts, key_columns=["podcast_id"])
    find_specific_issues("podcasts", podcasts)

    # 4. Episodes
    episodes = pd.DataFrame(json.loads((RAW_DIR / "episodes.json").read_text()))
    profile_dataset("episodes", episodes, key_columns=["episode_id"])
    find_specific_issues("episodes", episodes)

    # 5. Ad Events
    ad_events = pd.DataFrame(json.loads((RAW_DIR / "ad_events.json").read_text()))
    profile_dataset("ad_events", ad_events, key_columns=["ad_event_id", "event_id"])
    find_specific_issues("ad_events", ad_events)

    # 6. Metadata Changes
    meta = pd.DataFrame(json.loads((RAW_DIR / "metadata_changes.json").read_text()))
    profile_dataset("metadata_changes", meta)
    find_specific_issues("metadata_changes", meta)

    # 7. Listening Events (sampled summary + full stats)
    print(f"\n  Loading listening events (this may take a moment)...")
    events = load_listening_events()
    profile_dataset("listening_events", events, key_columns=["event_id", "user_id"])
    find_specific_issues("listening_events", events)

    # Summary using DuckDB for fast aggregation
    print(f"\n{'=' * 80}")
    print("  DUCKDB QUICK STATS")
    print(f"{'=' * 80}")
    con = duckdb.connect()
    con.execute("CREATE TABLE events AS SELECT * FROM events")  # noqa
    result = con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT event_id) AS unique_events,
            COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_events,
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(DISTINCT episode_id) AS unique_episodes,
            MIN(timestamp) AS earliest,
            MAX(timestamp) AS latest
        FROM events
    """).fetchone()
    labels = ["total_rows", "unique_events", "duplicate_events",
              "unique_users", "unique_episodes", "earliest", "latest"]
    for label, val in zip(labels, result):
        print(f"    {label}: {val}")

    con.close()

    print(f"\n{'=' * 80}")
    print("  PROFILING COMPLETE")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
