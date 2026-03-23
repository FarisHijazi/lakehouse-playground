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
    """Percentage of values matching expected format."""
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


def score_timeliness(latest_timestamp: datetime | None, max_days: int) -> float:
    """100 if within SLO, scaled down based on how late."""
    if latest_timestamp is None:
        return 0.0
    gap_days = (datetime.now() - latest_timestamp).total_seconds() / 86400
    if gap_days <= max_days:
        return 100.0
    # Scale: at 2x the SLO, score = 50; at 3x, score = 0
    ratio = gap_days / max_days
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
    freshness_slo_days: int,
) -> dict:
    comp = score_completeness(df, required_columns)
    uniq = score_uniqueness(df, key_columns)
    valid = score_validity(df, validity_checks)
    cons = score_consistency(df, format_checks)
    timely = score_timeliness(latest_timestamp, freshness_slo_days)

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
    parsed = pd.to_datetime(df[col], errors="coerce")
    valid = parsed.dropna()
    return valid.max().to_pydatetime() if not valid.empty else None


def main():
    print(f"{'=' * 100}")
    print("  DATA QUALITY SCORECARD")
    print(f"{'=' * 100}")

    results = []

    # --- Yellow Tripdata ---
    yellow_frames = []
    for f in sorted(RAW_DIR.glob("yellow_tripdata_*.parquet")):
        yellow_frames.append(pd.read_parquet(f))
    yellow = pd.concat(yellow_frames, ignore_index=True) if yellow_frames else pd.DataFrame()

    if not yellow.empty:
        results.append(score_dataset(
            name="yellow_tripdata",
            df=yellow,
            required_columns=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                              "PULocationID", "DOLocationID", "fare_amount",
                              "total_amount", "trip_distance"],
            key_columns=[],  # no natural unique key in trip data
            validity_checks=[
                {"column": "fare_amount", "type": "range", "min": -50, "max": 5000},
                {"column": "trip_distance", "type": "range", "min": 0, "max": 500},
                {"column": "passenger_count", "type": "range", "min": 0, "max": 9},
                {"column": "total_amount", "type": "range", "min": -100, "max": 10000},
                {"column": "PULocationID", "type": "range", "min": 1, "max": 265},
                {"column": "DOLocationID", "type": "range", "min": 1, "max": 265},
                {"column": "payment_type", "type": "accepted_values",
                 "values": [1, 2, 3, 4, 5, 6]},
            ],
            format_checks=[
                {"column": "store_and_fwd_flag", "pattern": r"^[YN]$"},
            ],
            latest_timestamp=get_latest_ts(yellow, "tpep_pickup_datetime"),
            freshness_slo_days=90,
        ))

    # --- Green Tripdata ---
    green_frames = []
    for f in sorted(RAW_DIR.glob("green_tripdata_*.parquet")):
        green_frames.append(pd.read_parquet(f))
    green = pd.concat(green_frames, ignore_index=True) if green_frames else pd.DataFrame()

    if not green.empty:
        results.append(score_dataset(
            name="green_tripdata",
            df=green,
            required_columns=["lpep_pickup_datetime", "lpep_dropoff_datetime",
                              "PULocationID", "DOLocationID", "fare_amount",
                              "total_amount", "trip_distance"],
            key_columns=[],
            validity_checks=[
                {"column": "fare_amount", "type": "range", "min": -50, "max": 5000},
                {"column": "trip_distance", "type": "range", "min": 0, "max": 500},
                {"column": "passenger_count", "type": "range", "min": 0, "max": 9},
                {"column": "PULocationID", "type": "range", "min": 1, "max": 265},
                {"column": "DOLocationID", "type": "range", "min": 1, "max": 265},
                {"column": "payment_type", "type": "accepted_values",
                 "values": [1, 2, 3, 4, 5, 6]},
            ],
            format_checks=[
                {"column": "store_and_fwd_flag", "pattern": r"^[YN]$"},
            ],
            latest_timestamp=get_latest_ts(green, "lpep_pickup_datetime"),
            freshness_slo_days=90,
        ))

    # --- Taxi Zone Lookup ---
    zones = pd.read_csv(RAW_DIR / "taxi_zone_lookup.csv")
    results.append(score_dataset(
        name="taxi_zone_lookup",
        df=zones,
        required_columns=["LocationID", "Borough", "Zone", "service_zone"],
        key_columns=["LocationID"],
        validity_checks=[
            {"column": "LocationID", "type": "range", "min": 1, "max": 265},
            {"column": "Borough", "type": "accepted_values",
             "values": ["Manhattan", "Bronx", "Brooklyn", "Queens",
                        "Staten Island", "EWR", "Unknown"]},
        ],
        format_checks=[],
        latest_timestamp=datetime.now(),  # reference data, always "fresh"
        freshness_slo_days=365,
    ))

    # --- Vendors ---
    vendors = pd.read_csv(RAW_DIR / "vendors.csv")
    results.append(score_dataset(
        name="vendors",
        df=vendors,
        required_columns=["vendor_id", "vendor_name"],
        key_columns=["vendor_id"],
        validity_checks=[],
        format_checks=[],
        latest_timestamp=datetime.now(),
        freshness_slo_days=365,
    ))

    # --- Payment Types ---
    payment_types = pd.read_csv(RAW_DIR / "payment_types.csv")
    results.append(score_dataset(
        name="payment_types",
        df=payment_types,
        required_columns=["payment_type_id", "payment_type_name"],
        key_columns=["payment_type_id"],
        validity_checks=[],
        format_checks=[],
        latest_timestamp=datetime.now(),
        freshness_slo_days=365,
    ))

    # --- Rate Codes ---
    rate_codes = pd.read_csv(RAW_DIR / "rate_codes.csv")
    results.append(score_dataset(
        name="rate_codes",
        df=rate_codes,
        required_columns=["rate_code_id", "rate_code_name"],
        key_columns=["rate_code_id"],
        validity_checks=[],
        format_checks=[],
        latest_timestamp=datetime.now(),
        freshness_slo_days=365,
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
