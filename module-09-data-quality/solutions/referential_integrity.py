"""
Exercise 3: Referential Integrity Checks
==========================================
Validates foreign key relationships across all raw datasets.
Reports orphaned records for each relationship.
"""

from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


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
        "sample_orphans": sorted([str(o) for o in list(orphans)])[:10],
    }


def main():
    print("Loading datasets...")

    # Load yellow trip data
    yellow_frames = []
    for f in sorted(RAW_DIR.glob("yellow_tripdata_*.parquet")):
        yellow_frames.append(pd.read_parquet(f))
    yellow = pd.concat(yellow_frames, ignore_index=True) if yellow_frames else pd.DataFrame()

    # Load green trip data
    green_frames = []
    for f in sorted(RAW_DIR.glob("green_tripdata_*.parquet")):
        green_frames.append(pd.read_parquet(f))
    green = pd.concat(green_frames, ignore_index=True) if green_frames else pd.DataFrame()

    # Load reference tables
    zones = pd.read_csv(RAW_DIR / "taxi_zone_lookup.csv")
    vendors = pd.read_csv(RAW_DIR / "vendors.csv")
    payment_types = pd.read_csv(RAW_DIR / "payment_types.csv")
    rate_codes = pd.read_csv(RAW_DIR / "rate_codes.csv")

    print(f"\n{'=' * 90}")
    print("  REFERENTIAL INTEGRITY REPORT")
    print(f"{'=' * 90}")

    checks = [
        # yellow_trips.PULocationID -> zones.LocationID
        (yellow, "PULocationID", zones, "LocationID", "yellow_trips", "taxi_zones"),
        # yellow_trips.DOLocationID -> zones.LocationID
        (yellow, "DOLocationID", zones, "LocationID", "yellow_trips", "taxi_zones"),
        # yellow_trips.VendorID -> vendors.vendor_id
        (yellow, "VendorID", vendors, "vendor_id", "yellow_trips", "vendors"),
        # yellow_trips.payment_type -> payment_types.payment_type_id
        (yellow, "payment_type", payment_types, "payment_type_id",
         "yellow_trips", "payment_types"),
        # yellow_trips.RatecodeID -> rate_codes.rate_code_id
        (yellow, "RatecodeID", rate_codes, "rate_code_id",
         "yellow_trips", "rate_codes"),
        # green_trips.PULocationID -> zones.LocationID
        (green, "PULocationID", zones, "LocationID", "green_trips", "taxi_zones"),
        # green_trips.DOLocationID -> zones.LocationID
        (green, "DOLocationID", zones, "LocationID", "green_trips", "taxi_zones"),
        # green_trips.VendorID -> vendors.vendor_id
        (green, "VendorID", vendors, "vendor_id", "green_trips", "vendors"),
        # green_trips.payment_type -> payment_types.payment_type_id
        (green, "payment_type", payment_types, "payment_type_id",
         "green_trips", "payment_types"),
    ]

    results = []
    for child_df, child_col, parent_df, parent_col, child_name, parent_name in checks:
        if child_df.empty or child_col not in child_df.columns:
            continue
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
