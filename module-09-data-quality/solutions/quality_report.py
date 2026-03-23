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

from datetime import datetime
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


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


def check_out_of_range(df: pd.DataFrame, col: str,
                       min_val: float, max_val: float) -> dict | None:
    if col not in df.columns:
        return None
    series = pd.to_numeric(df[col], errors="coerce").dropna()
    invalid = ((series < min_val) | (series > max_val)).sum()
    if invalid > 0:
        return {
            "column": col,
            "issue": f"values outside [{min_val}, {max_val}]",
            "count": int(invalid),
            "pct": round(invalid / len(df) * 100, 2),
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
    parsed = pd.to_datetime(df[col], errors="coerce")
    valid = parsed.dropna()
    if valid.empty:
        return None
    gap = datetime.now() - valid.max().to_pydatetime()
    return round(gap.total_seconds() / 86400, 1)


def main():
    now = datetime.now()

    # Load datasets
    yellow_frames = []
    for f in sorted(RAW_DIR.glob("yellow_tripdata_*.parquet")):
        yellow_frames.append(pd.read_parquet(f))
    yellow = pd.concat(yellow_frames, ignore_index=True) if yellow_frames else pd.DataFrame()

    green_frames = []
    for f in sorted(RAW_DIR.glob("green_tripdata_*.parquet")):
        green_frames.append(pd.read_parquet(f))
    green = pd.concat(green_frames, ignore_index=True) if green_frames else pd.DataFrame()

    zones = pd.read_csv(RAW_DIR / "taxi_zone_lookup.csv")
    vendors = pd.read_csv(RAW_DIR / "vendors.csv")
    payment_types = pd.read_csv(RAW_DIR / "payment_types.csv")
    rate_codes = pd.read_csv(RAW_DIR / "rate_codes.csv")

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
        ("yellow_tripdata", yellow,
         ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
          "DOLocationID", "fare_amount", "total_amount"],
         None),
        ("green_tripdata", green,
         ["lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID",
          "DOLocationID", "fare_amount", "total_amount"],
         None),
        ("taxi_zone_lookup", zones,
         ["LocationID", "Borough", "Zone", "service_zone"],
         "LocationID"),
        ("vendors", vendors,
         ["vendor_id", "vendor_name"],
         "vendor_id"),
        ("payment_types", payment_types,
         ["payment_type_id", "payment_type_name"],
         "payment_type_id"),
        ("rate_codes", rate_codes,
         ["rate_code_id", "rate_code_name"],
         "rate_code_id"),
    ]

    total_rows = 0
    total_issues_count = 0
    dataset_summaries = []

    for ds_name, df, req_cols, key_col in datasets_info:
        if df.empty:
            continue
        total_rows += len(df)
        null_issues = check_nulls(df, req_cols)
        dup_issue = check_duplicates(df, key_col) if key_col else None
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

    print(f"\n  Total datasets:       {len(dataset_summaries)}")
    print(f"  Total rows:           {total_rows:,}")
    print(f"  Total issues found:   {total_issues_count:,}")
    print(f"\n  {'Dataset':<30s} {'Rows':>12s} {'Issues':>10s} {'Status'}")
    print(f"  {'-' * 70}")
    for ds in dataset_summaries:
        print(f"  {ds['name']:<30s} {ds['rows']:>12,} {ds['issues']:>10,} {ds['status']}")

    # ===== SECTION 2: Per-Dataset Details =====
    print(f"\n{'=' * 100}")
    print("  2. PER-DATASET QUALITY DETAILS")
    print(f"{'=' * 100}")

    # Yellow Tripdata
    if not yellow.empty:
        print(f"\n  --- yellow_tripdata ({len(yellow):,} rows) ---")
        for issue in check_nulls(yellow, ["tpep_pickup_datetime", "tpep_dropoff_datetime",
                                           "PULocationID", "DOLocationID", "fare_amount",
                                           "passenger_count", "trip_distance"]):
            print(f"    Nulls in {issue['column']}: {issue['count']:,} ({issue['pct']}%)")

        neg_fare = check_negatives(yellow, "fare_amount")
        if neg_fare:
            print(f"    Negative fare_amount: {neg_fare['count']:,}")
            neg_fare["dataset"] = "yellow_tripdata"
            neg_fare["severity"] = "HIGH"
            all_issues.append(neg_fare)

        neg_total = check_negatives(yellow, "total_amount")
        if neg_total:
            print(f"    Negative total_amount: {neg_total['count']:,}")
            neg_total["dataset"] = "yellow_tripdata"
            neg_total["severity"] = "MEDIUM"
            all_issues.append(neg_total)

        zero_dist = (yellow["trip_distance"] == 0).sum()
        if zero_dist > 0:
            print(f"    Zero trip_distance: {zero_dist:,}")
            all_issues.append({
                "dataset": "yellow_tripdata", "column": "trip_distance",
                "issue": "zero distance trips", "count": int(zero_dist),
                "pct": round(zero_dist / len(yellow) * 100, 2),
                "severity": "MEDIUM",
            })

        oor = check_out_of_range(yellow, "PULocationID", 1, 265)
        if oor:
            print(f"    PULocationID out of range: {oor['count']:,}")
            oor["dataset"] = "yellow_tripdata"
            oor["severity"] = "HIGH"
            all_issues.append(oor)

    # Green Tripdata
    if not green.empty:
        print(f"\n  --- green_tripdata ({len(green):,} rows) ---")
        for issue in check_nulls(green, ["lpep_pickup_datetime", "lpep_dropoff_datetime",
                                          "PULocationID", "DOLocationID", "fare_amount",
                                          "passenger_count"]):
            print(f"    Nulls in {issue['column']}: {issue['count']:,} ({issue['pct']}%)")

        neg_fare = check_negatives(green, "fare_amount")
        if neg_fare:
            print(f"    Negative fare_amount: {neg_fare['count']:,}")
            neg_fare["dataset"] = "green_tripdata"
            neg_fare["severity"] = "HIGH"
            all_issues.append(neg_fare)

    # Taxi Zone Lookup
    print(f"\n  --- taxi_zone_lookup ({len(zones):,} rows) ---")
    for issue in check_nulls(zones, ["LocationID", "Borough", "Zone", "service_zone"]):
        print(f"    Nulls in {issue['column']}: {issue['count']:,} ({issue['pct']}%)")
    dup = check_duplicates(zones, "LocationID")
    if dup:
        print(f"    Duplicate LocationID: {dup['count']:,}")

    # ===== SECTION 3: Referential Integrity =====
    print(f"\n{'=' * 100}")
    print("  3. REFERENTIAL INTEGRITY")
    print(f"{'=' * 100}")

    ref_checks = []
    if not yellow.empty:
        ref_checks.extend([
            (yellow, "PULocationID", zones, "LocationID", "yellow_trips", "taxi_zones"),
            (yellow, "DOLocationID", zones, "LocationID", "yellow_trips", "taxi_zones"),
            (yellow, "VendorID", vendors, "vendor_id", "yellow_trips", "vendors"),
            (yellow, "payment_type", payment_types, "payment_type_id",
             "yellow_trips", "payment_types"),
        ])
    if not green.empty:
        ref_checks.extend([
            (green, "PULocationID", zones, "LocationID", "green_trips", "taxi_zones"),
            (green, "DOLocationID", zones, "LocationID", "green_trips", "taxi_zones"),
        ])

    for child_df, child_col, parent_df, parent_col, child_name, parent_name in ref_checks:
        if child_col not in child_df.columns:
            continue
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

    freshness_items = []
    if not yellow.empty:
        freshness_items.append(("yellow_tripdata", yellow, "tpep_pickup_datetime", 90))
    if not green.empty:
        freshness_items.append(("green_tripdata", green, "lpep_pickup_datetime", 90))

    print(f"\n  {'Dataset':<22s} {'Days Since Latest':>20s} {'SLO (days)':>12s} {'Status'}")
    print(f"  {'-' * 70}")
    for ds_name, df, col, slo in freshness_items:
        gap_days = get_freshness(df, col)
        if gap_days is not None:
            status = "PASS" if gap_days <= slo else "STALE"
            print(f"  {ds_name:<22s} {gap_days:>20.1f} {slo:>12} {status}")
        else:
            print(f"  {ds_name:<22s} {'N/A':>20s} {slo:>12} UNKNOWN")

    # ===== SECTION 5: Volume Summary =====
    print(f"\n{'=' * 100}")
    print("  5. VOLUME SUMMARY")
    print(f"{'=' * 100}")

    import re
    for prefix, label in [("yellow_tripdata", "Yellow"), ("green_tripdata", "Green")]:
        files = sorted(RAW_DIR.glob(f"{prefix}_*.parquet"))
        if files:
            print(f"\n  {label} tripdata:")
            total = 0
            for f in files:
                count = len(pd.read_parquet(f))
                total += count
                print(f"    {f.name}: {count:,} trips")
            print(f"    Total: {total:,}")

    # ===== SECTION 6: Top Critical Issues =====
    print(f"\n{'=' * 100}")
    print("  6. TOP CRITICAL ISSUES AND RECOMMENDED ACTIONS")
    print(f"{'=' * 100}")

    # Sort issues by severity and count
    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    all_issues.sort(key=lambda x: (severity_order.get(x.get("severity", "LOW"), 3),
                                    -x.get("count", 0)))

    recommendations = {
        "null/empty values": "Investigate vendor data feed for missing fields. "
                             "Add default values or reject incomplete records at ingestion.",
        "duplicate keys": "Implement deduplication in the ingestion pipeline. "
                         "Check for reprocessed parquet files.",
        "negative values": "Negative fares may represent adjustments or refunds. "
                          "Validate with TLC business rules and quarantine invalid records.",
        "orphan keys": "Verify location ID and vendor ID references during ETL. "
                      "Check for new zones or vendors not yet in lookup tables.",
        "zero distance": "Zero-distance trips may be cancellations or meter errors. "
                        "Flag for review and consider filtering from aggregates.",
        "values outside": "Validate location IDs and fare amounts at ingestion. "
                         "Reject records with clearly invalid values.",
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
