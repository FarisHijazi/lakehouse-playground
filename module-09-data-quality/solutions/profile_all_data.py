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

from pathlib import Path

import duckdb
import pandas as pd

DATA_DIR = Path(__file__).parent.parent.parent / "data"
RAW_DIR = DATA_DIR / "raw"


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

    if name == "yellow_tripdata":
        if "fare_amount" in df.columns:
            negs = (df["fare_amount"] < 0).sum()
            if negs > 0:
                issues.append(f"fare_amount has {negs:,} negative values")
            extremes = (df["fare_amount"] > 5000).sum()
            if extremes > 0:
                issues.append(f"fare_amount has {extremes:,} extreme values (>$5000)")

        if "trip_distance" in df.columns:
            zeros = (df["trip_distance"] == 0).sum()
            if zeros > 0:
                issues.append(f"trip_distance has {zeros:,} zero values")
            negs = (df["trip_distance"] < 0).sum()
            if negs > 0:
                issues.append(f"trip_distance has {negs:,} negative values")

        if "passenger_count" in df.columns:
            nulls = df["passenger_count"].isnull().sum()
            if nulls > 0:
                issues.append(f"passenger_count has {nulls:,} null values")
            zeros = (df["passenger_count"] == 0).sum()
            if zeros > 0:
                issues.append(f"passenger_count has {zeros:,} zero values")

        if "PULocationID" in df.columns:
            invalid = ((df["PULocationID"] < 1) | (df["PULocationID"] > 265)).sum()
            if invalid > 0:
                issues.append(f"PULocationID has {invalid:,} values outside [1, 265]")

        if "DOLocationID" in df.columns:
            invalid = ((df["DOLocationID"] < 1) | (df["DOLocationID"] > 265)).sum()
            if invalid > 0:
                issues.append(f"DOLocationID has {invalid:,} values outside [1, 265]")

        if "total_amount" in df.columns:
            negs = (df["total_amount"] < 0).sum()
            if negs > 0:
                issues.append(f"total_amount has {negs:,} negative values")

    if name == "green_tripdata":
        if "fare_amount" in df.columns:
            negs = (df["fare_amount"] < 0).sum()
            if negs > 0:
                issues.append(f"fare_amount has {negs:,} negative values")
        if "trip_distance" in df.columns:
            zeros = (df["trip_distance"] == 0).sum()
            if zeros > 0:
                issues.append(f"trip_distance has {zeros:,} zero values")
        if "passenger_count" in df.columns:
            nulls = df["passenger_count"].isnull().sum()
            if nulls > 0:
                issues.append(f"passenger_count has {nulls:,} null values")

    if name == "taxi_zone_lookup":
        if "LocationID" in df.columns:
            dupes = df["LocationID"].duplicated().sum()
            if dupes > 0:
                issues.append(f"LocationID has {dupes:,} duplicates")
        if "Borough" in df.columns:
            boroughs = df["Borough"].unique().tolist()
            issues.append(f"Borough values: {sorted(boroughs)}")

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

    # 1. Yellow Tripdata
    yellow_frames = []
    for f in sorted(RAW_DIR.glob("yellow_tripdata_*.parquet")):
        yellow_frames.append(pd.read_parquet(f))
    if yellow_frames:
        yellow = pd.concat(yellow_frames, ignore_index=True)
        profile_dataset("yellow_tripdata", yellow,
                        key_columns=["VendorID", "PULocationID", "DOLocationID"])
        find_specific_issues("yellow_tripdata", yellow)
    else:
        print("\n  No yellow_tripdata parquet files found.")

    # 2. Green Tripdata
    green_frames = []
    for f in sorted(RAW_DIR.glob("green_tripdata_*.parquet")):
        green_frames.append(pd.read_parquet(f))
    if green_frames:
        green = pd.concat(green_frames, ignore_index=True)
        profile_dataset("green_tripdata", green,
                        key_columns=["VendorID", "PULocationID", "DOLocationID"])
        find_specific_issues("green_tripdata", green)
    else:
        print("\n  No green_tripdata parquet files found.")

    # 3. Taxi Zone Lookup
    zones = pd.read_csv(RAW_DIR / "taxi_zone_lookup.csv")
    profile_dataset("taxi_zone_lookup", zones, key_columns=["LocationID"])
    find_specific_issues("taxi_zone_lookup", zones)

    # 4. Vendors
    vendors = pd.read_csv(RAW_DIR / "vendors.csv")
    profile_dataset("vendors", vendors, key_columns=["vendor_id"])
    find_specific_issues("vendors", vendors)

    # 5. Rate Codes
    rate_codes = pd.read_csv(RAW_DIR / "rate_codes.csv")
    profile_dataset("rate_codes", rate_codes, key_columns=["rate_code_id"])
    find_specific_issues("rate_codes", rate_codes)

    # 6. Payment Types
    payment_types = pd.read_csv(RAW_DIR / "payment_types.csv")
    profile_dataset("payment_types", payment_types, key_columns=["payment_type_id"])
    find_specific_issues("payment_types", payment_types)

    # 7. FHV Bases
    fhv_bases = pd.read_csv(RAW_DIR / "fhv_bases.csv")
    profile_dataset("fhv_bases", fhv_bases, key_columns=["base_license_num"])
    find_specific_issues("fhv_bases", fhv_bases)

    # 8. NYC Weather
    weather_path = RAW_DIR / "nyc_weather_2023.csv"
    if weather_path.exists():
        weather = pd.read_csv(weather_path)
        profile_dataset("nyc_weather_2023", weather)
        find_specific_issues("nyc_weather_2023", weather)

    # Summary using DuckDB for fast aggregation on yellow trips
    if yellow_frames:
        print(f"\n{'=' * 80}")
        print("  DUCKDB QUICK STATS (yellow_tripdata)")
        print(f"{'=' * 80}")
        con = duckdb.connect()
        con.execute("CREATE TABLE yellow AS SELECT * FROM yellow")  # noqa
        result = con.execute("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT VendorID) AS unique_vendors,
                COUNT(DISTINCT PULocationID) AS unique_pickup_zones,
                COUNT(DISTINCT DOLocationID) AS unique_dropoff_zones,
                MIN(tpep_pickup_datetime) AS earliest_pickup,
                MAX(tpep_pickup_datetime) AS latest_pickup,
                AVG(trip_distance) AS avg_distance,
                AVG(fare_amount) AS avg_fare,
                AVG(total_amount) AS avg_total
            FROM yellow
        """).fetchone()
        labels = ["total_rows", "unique_vendors", "unique_pickup_zones",
                  "unique_dropoff_zones", "earliest_pickup", "latest_pickup",
                  "avg_distance", "avg_fare", "avg_total"]
        for label, val in zip(labels, result):
            if isinstance(val, float):
                print(f"    {label}: {val:.2f}")
            else:
                print(f"    {label}: {val}")
        con.close()

    print(f"\n{'=' * 80}")
    print("  PROFILING COMPLETE")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
