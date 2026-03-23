"""
Exercise 8: Quality Report Dashboard
=======================================
Generates a comprehensive data quality report combining all checks:
  - Executive summary
  - Per-dataset scorecards
  - Referential integrity results
  - Freshness status
  - Volume anomaly alerts
  - Top critical issues with recommended actions
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


def load_listening_events_sample(n_files: int = 30) -> pd.DataFrame:
    events_dir = RAW_DIR / "listening_events"
    partition_files = sorted(events_dir.glob("events_*.jsonl"))
    sample_files = partition_files[-n_files:] if len(partition_files) > n_files else partition_files
    frames = []
    for f in sample_files:
        lines = f.read_text().strip().split("\n")
        records = [json.loads(line) for line in lines if line.strip()]
        if records:
            frames.append(pd.DataFrame(records))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def check_nulls(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    issues = []
    for col in columns:
        if col not in df.columns:
            continue
        null_count = df[col].isnull().sum()
        if df[col].dtype == object:
            null_count += (df[col] == "").sum()
        if null_count > 0:
            pct = null_count / len(df) * 100
            issues.append({
                "column": col,
                "issue": "null/empty values",
                "count": int(null_count),
                "pct": round(pct, 2),
            })
    return issues


def check_duplicates(df: pd.DataFrame, key_col: str) -> dict | None:
    if key_col not in df.columns:
        return None
    dupes = df[key_col].dropna().duplicated().sum()
    if dupes > 0:
        return {
            "column": key_col,
            "issue": "duplicate keys",
            "count": int(dupes),
            "pct": round(dupes / len(df) * 100, 2),
        }
    return None


def check_negatives(df: pd.DataFrame, col: str) -> dict | None:
    if col not in df.columns:
        return None
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    negs = (series < 0).sum()
    if negs > 0:
        return {
            "column": col,
            "issue": "negative values",
            "count": int(negs),
            "pct": round(negs / len(df) * 100, 2),
        }
    return None


def check_referential(child_df, child_col, parent_df, parent_col,
                      child_name, parent_name) -> dict:
    child_keys = set(child_df[child_col].dropna().unique())
    parent_keys = set(parent_df[parent_col].dropna().unique())
    orphans = child_keys - parent_keys
    return {
        "relationship": f"{child_name}.{child_col} -> {parent_name}.{parent_col}",
        "orphan_count": len(orphans),
        "orphan_pct": round(len(orphans) / len(child_keys) * 100, 2) if child_keys else 0,
        "passed": len(orphans) == 0,
    }


def get_freshness(df: pd.DataFrame, col: str) -> float | None:
    parsed = pd.to_datetime(df[col], errors="coerce", format="mixed")
    valid = parsed.dropna()
    if valid.empty:
        return None
    gap = datetime.now() - valid.max().to_pydatetime()
    return round(gap.total_seconds() / 3600, 1)


def main():
    now = datetime.now()

    # Load datasets
    users = pd.read_csv(RAW_DIR / "users.csv")
    cdn = pd.read_csv(RAW_DIR / "cdn_logs.csv")
    podcasts = pd.DataFrame(json.loads((RAW_DIR / "podcasts.json").read_text()))
    episodes = pd.DataFrame(json.loads((RAW_DIR / "episodes.json").read_text()))
    ad_events = pd.DataFrame(json.loads((RAW_DIR / "ad_events.json").read_text()))
    events = load_listening_events_sample()

    all_issues = []  # Collect all issues for ranking

    # ===== HEADER =====
    print(f"\n{'#' * 100}")
    print(f"#{'DATA QUALITY REPORT':^98s}#")
    print(f"#{'Generated: ' + now.strftime('%Y-%m-%d %H:%M:%S'):^98s}#")
    print(f"{'#' * 100}")

    # ===== SECTION 1: Executive Summary =====
    print(f"\n{'=' * 100}")
    print("  1. EXECUTIVE SUMMARY")
    print(f"{'=' * 100}")

    datasets_info = [
        ("users", users, ["user_id", "email", "name", "country", "signup_date"], "user_id"),
        ("cdn_logs", cdn, ["log_id", "event_id", "user_id", "timestamp"], "log_id"),
        ("podcasts", podcasts, ["podcast_id", "name", "category"], "podcast_id"),
        ("episodes", episodes, ["episode_id", "podcast_id", "title"], "episode_id"),
        ("ad_events", ad_events, ["ad_event_id", "event_id", "timestamp"], "ad_event_id"),
        ("listening_events (sample)", events,
         ["event_id", "user_id", "episode_id", "event_type"], "event_id"),
    ]

    total_rows = 0
    total_issues_count = 0
    dataset_summaries = []

    for ds_name, df, req_cols, key_col in datasets_info:
        total_rows += len(df)
        null_issues = check_nulls(df, req_cols)
        dup_issue = check_duplicates(df, key_col)
        issue_count = sum(i["count"] for i in null_issues)
        if dup_issue:
            issue_count += dup_issue["count"]

        total_issues_count += issue_count
        status = "CLEAN" if issue_count == 0 else "ISSUES FOUND"

        for ni in null_issues:
            ni["dataset"] = ds_name
            ni["severity"] = "HIGH" if ni["pct"] > 5 else "MEDIUM" if ni["pct"] > 1 else "LOW"
            all_issues.append(ni)
        if dup_issue:
            dup_issue["dataset"] = ds_name
            dup_issue["severity"] = "HIGH" if dup_issue["pct"] > 1 else "MEDIUM"
            all_issues.append(dup_issue)

        dataset_summaries.append({
            "name": ds_name,
            "rows": len(df),
            "issues": issue_count,
            "status": status,
        })

    print(f"\n  Total datasets:       {len(datasets_info)}")
    print(f"  Total rows:           {total_rows:,}")
    print(f"  Total issues found:   {total_issues_count:,}")
    print(f"\n  {'Dataset':<30s} {'Rows':>10s} {'Issues':>10s} {'Status'}")
    print(f"  {'-' * 70}")
    for ds in dataset_summaries:
        print(f"  {ds['name']:<30s} {ds['rows']:>10,} {ds['issues']:>10,} {ds['status']}")

    # ===== SECTION 2: Per-Dataset Details =====
    print(f"\n{'=' * 100}")
    print("  2. PER-DATASET QUALITY DETAILS")
    print(f"{'=' * 100}")

    # Users
    print(f"\n  --- users.csv ({len(users):,} rows) ---")
    for issue in check_nulls(users, ["user_id", "email", "name", "country",
                                     "city", "signup_date", "age", "gender"]):
        print(f"    Nulls in {issue['column']}: {issue['count']:,} ({issue['pct']}%)")
    dup = check_duplicates(users, "user_id")
    if dup:
        print(f"    Duplicate user_id: {dup['count']:,}")
    # Format issues
    dates = users["signup_date"].dropna().astype(str)
    iso_pct = dates.str.match(r"^\d{4}-\d{2}-\d{2}$").mean() * 100
    print(f"    signup_date in ISO format: {iso_pct:.1f}%")
    gender_vals = users["gender"].dropna().unique().tolist()
    print(f"    gender unique values: {sorted(gender_vals)}")

    # CDN Logs
    print(f"\n  --- cdn_logs.csv ({len(cdn):,} rows) ---")
    neg_startup = check_negatives(cdn, "startup_time_ms")
    if neg_startup:
        print(f"    Negative startup_time_ms: {neg_startup['count']:,}")
        neg_startup["dataset"] = "cdn_logs"
        neg_startup["severity"] = "HIGH"
        all_issues.append(neg_startup)

    # Ad Events
    print(f"\n  --- ad_events.json ({len(ad_events):,} rows) ---")
    zero_rev = (ad_events["revenue_sar"] == 0).sum()
    neg_rev = (ad_events["revenue_sar"] < 0).sum()
    print(f"    Zero revenue_sar: {zero_rev:,}")
    print(f"    Negative revenue_sar: {neg_rev:,}")
    if zero_rev > 0:
        all_issues.append({
            "dataset": "ad_events", "column": "revenue_sar",
            "issue": "zero revenue", "count": int(zero_rev),
            "pct": round(zero_rev / len(ad_events) * 100, 2),
            "severity": "MEDIUM",
        })

    # Listening Events
    print(f"\n  --- listening_events (sample: {len(events):,} rows) ---")
    for issue in check_nulls(events, ["event_id", "user_id", "episode_id",
                                       "event_type", "listened_seconds"]):
        print(f"    Nulls in {issue['column']}: {issue['count']:,} ({issue['pct']}%)")
    dup = check_duplicates(events, "event_id")
    if dup:
        print(f"    Duplicate event_id: {dup['count']:,}")
        dup["dataset"] = "listening_events"
        dup["severity"] = "HIGH"
        all_issues.append(dup)

    # ===== SECTION 3: Referential Integrity =====
    print(f"\n{'=' * 100}")
    print("  3. REFERENTIAL INTEGRITY")
    print(f"{'=' * 100}")

    ref_checks = [
        (events, "user_id", users, "user_id", "listening_events", "users"),
        (events, "episode_id", episodes, "episode_id", "listening_events", "episodes"),
        (episodes, "podcast_id", podcasts, "podcast_id", "episodes", "podcasts"),
        (cdn, "user_id", users, "user_id", "cdn_logs", "users"),
        (ad_events, "user_id", users, "user_id", "ad_events", "users"),
    ]

    for child_df, child_col, parent_df, parent_col, child_name, parent_name in ref_checks:
        result = check_referential(child_df, child_col, parent_df, parent_col,
                                   child_name, parent_name)
        status = "PASS" if result["passed"] else "FAIL"
        print(f"  [{status}] {result['relationship']}: "
              f"{result['orphan_count']} orphans ({result['orphan_pct']}%)")
        if not result["passed"]:
            all_issues.append({
                "dataset": child_name, "column": child_col,
                "issue": f"orphan keys -> {parent_name}.{parent_col}",
                "count": result["orphan_count"],
                "pct": result["orphan_pct"],
                "severity": "HIGH" if result["orphan_pct"] > 5 else "MEDIUM",
            })

    # ===== SECTION 4: Freshness =====
    print(f"\n{'=' * 100}")
    print("  4. FRESHNESS STATUS")
    print(f"{'=' * 100}")

    freshness_items = [
        ("users", users, "signup_date", 168),
        ("cdn_logs", cdn, "timestamp", 24),
        ("ad_events", ad_events, "timestamp", 48),
        ("episodes", episodes, "published_at", 168),
    ]

    print(f"\n  {'Dataset':<22s} {'Hours Since Latest':>20s} {'SLO (hrs)':>12s} {'Status'}")
    print(f"  {'-' * 70}")
    for ds_name, df, col, slo in freshness_items:
        gap_hours = get_freshness(df, col)
        if gap_hours is not None:
            status = "PASS" if gap_hours <= slo else "STALE"
            print(f"  {ds_name:<22s} {gap_hours:>20.1f} {slo:>12} {status}")
        else:
            print(f"  {ds_name:<22s} {'N/A':>20s} {slo:>12} UNKNOWN")

    # ===== SECTION 5: Volume Anomalies =====
    print(f"\n{'=' * 100}")
    print("  5. VOLUME ANOMALY SUMMARY")
    print(f"{'=' * 100}")

    events_dir = RAW_DIR / "listening_events"
    daily_counts = []
    for f in sorted(events_dir.glob("events_*.jsonl")):
        date_str = f.stem.replace("events_", "")
        line_count = sum(1 for line in f.open() if line.strip())
        daily_counts.append({"date": date_str, "count": line_count})
    dc_df = pd.DataFrame(daily_counts)
    dc_df["date"] = pd.to_datetime(dc_df["date"])
    dc_df = dc_df.sort_values("date").reset_index(drop=True)

    dc_df["rolling_mean"] = dc_df["count"].rolling(7, min_periods=3).mean()
    dc_df["rolling_std"] = dc_df["count"].rolling(7, min_periods=3).std()
    dc_df["prev_count"] = dc_df["count"].shift(1)
    dc_df["dod_change"] = ((dc_df["count"] - dc_df["prev_count"]).abs() /
                            dc_df["prev_count"] * 100)
    dc_df["anomaly"] = (
        (dc_df["count"] > dc_df["rolling_mean"] + 2 * dc_df["rolling_std"]) |
        (dc_df["count"] < (dc_df["rolling_mean"] - 2 * dc_df["rolling_std"]).clip(lower=0)) |
        (dc_df["dod_change"] > 50)
    )

    anomaly_count = dc_df["anomaly"].sum()
    print(f"\n  Total partitions: {len(dc_df)}")
    print(f"  Anomalous days: {anomaly_count}")
    print(f"  Average daily count: {dc_df['count'].mean():.0f}")
    print(f"  Min daily count: {dc_df['count'].min()}")
    print(f"  Max daily count: {dc_df['count'].max()}")

    if anomaly_count > 0:
        print(f"\n  Recent anomalies (last 10):")
        recent = dc_df[dc_df["anomaly"]].tail(10)
        for _, row in recent.iterrows():
            print(f"    {row['date'].date()}: {row['count']} events "
                  f"(rolling avg: {row['rolling_mean']:.0f})")

    # ===== SECTION 6: Top Critical Issues =====
    print(f"\n{'=' * 100}")
    print("  6. TOP CRITICAL ISSUES AND RECOMMENDED ACTIONS")
    print(f"{'=' * 100}")

    # Sort issues by severity and count
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    all_issues.sort(key=lambda x: (severity_order.get(x.get("severity", "LOW"), 3),
                                    -x.get("count", 0)))

    recommendations = {
        "null/empty values": "Add NOT NULL constraints or default values at ingestion. "
                             "Investigate source system for missing data.",
        "duplicate keys": "Implement deduplication in the ingestion pipeline. "
                         "Use event_id as idempotency key.",
        "negative values": "Add range validation at ingestion. Reject or quarantine "
                          "records with invalid values.",
        "orphan keys": "Verify foreign key references during ETL. Flag orphan records "
                      "for investigation.",
        "zero revenue": "Review ad serving logic. Zero revenue on impressions may indicate "
                       "a billing pipeline issue.",
    }

    for i, issue in enumerate(all_issues[:10], 1):
        rec = ""
        for keyword, recommendation in recommendations.items():
            if keyword in issue.get("issue", ""):
                rec = recommendation
                break
        if not rec:
            rec = "Investigate and add automated validation."

        print(f"\n  {i}. [{issue.get('severity', 'UNKNOWN')}] {issue['dataset']}.{issue['column']}"
              f" -- {issue['issue']}")
        print(f"     Count: {issue['count']:,}  ({issue['pct']}% of rows)")
        print(f"     Action: {rec}")

    # ===== FOOTER =====
    print(f"\n{'#' * 100}")
    print(f"#{'END OF REPORT':^98s}#")
    print(f"{'#' * 100}")


if __name__ == "__main__":
    main()
