"""
Module 02 - Exercise 1: Data Profiling
=======================================

This script profiles every raw data source in the NYC TLC taxi dataset.
Profiling is the FIRST thing you should do before writing any pipeline. You need
to understand the shape of the data, its quality issues, and its quirks before
you can clean or transform it.

What we look for:
- Row/column counts and data types
- Null counts and percentages
- Unique value distributions for categorical columns
- Min/max ranges for numeric and date columns
- Duplicates (both full-row and key-based)
- Data quality anomalies specific to taxi data (negative fares, zero distances, etc.)

Usage:
    python solutions/profile_data.py
"""

import logging
from glob import glob
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Logging setup -- every production script should use structured logging
# instead of print() so you can control verbosity and route output.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Resolve paths relative to the project root (two levels up from this script).
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


# ===== Helper: profile a single DataFrame ===================================
def profile_dataframe(df: pd.DataFrame, name: str, key_columns: list[str] | None = None) -> None:
    """Print a comprehensive profile of a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The data to profile.
    name : str
        Human-readable label for log output.
    key_columns : list[str], optional
        If provided, check for duplicate values on this composite key.
    """
    separator = "=" * 70
    log.info(separator)
    log.info("PROFILING: %s", name)
    log.info(separator)

    # -- Shape ---------------------------------------------------------------
    log.info("Rows: %d  |  Columns: %d", len(df), len(df.columns))

    # -- Data types ----------------------------------------------------------
    log.info("Column types:")
    for col in df.columns:
        log.info("  %-30s  dtype=%-12s", col, df[col].dtype)

    # -- Null analysis -------------------------------------------------------
    null_counts = df.isnull().sum()
    null_pct = (df.isnull().sum() / len(df) * 100).round(2)
    log.info("Null counts:")
    has_nulls = False
    for col in df.columns:
        if null_counts[col] > 0:
            log.info(
                "  %-30s  nulls=%d  (%.2f%%)", col, null_counts[col], null_pct[col]
            )
            has_nulls = True
    if not has_nulls:
        log.info("  (no nulls found)")

    # -- Unique values for low-cardinality columns ---------------------------
    log.info("Unique value counts:")
    for col in df.columns:
        n_unique = df[col].nunique()
        # Only show value distribution for columns with <= 20 unique values.
        if n_unique <= 20:
            log.info("  %-30s  %d unique values: %s", col, n_unique, dict(df[col].value_counts()))
        else:
            log.info("  %-30s  %d unique values", col, n_unique)

    # -- Sample values -------------------------------------------------------
    log.info("Sample values (first 3 rows):")
    for col in df.columns:
        samples = df[col].dropna().head(3).tolist()
        log.info("  %-30s  %s", col, samples)

    # -- Numeric ranges ------------------------------------------------------
    numeric_cols = df.select_dtypes(include="number").columns
    if len(numeric_cols) > 0:
        log.info("Numeric column ranges:")
        for col in numeric_cols:
            log.info(
                "  %-30s  min=%-15s  max=%-15s  mean=%.2f",
                col,
                df[col].min(),
                df[col].max(),
                df[col].mean(),
            )

    # -- Datetime ranges -----------------------------------------------------
    datetime_cols = df.select_dtypes(include=["datetime", "datetimetz"]).columns
    if len(datetime_cols) > 0:
        log.info("Datetime column ranges:")
        for col in datetime_cols:
            log.info(
                "  %-30s  min=%s  max=%s",
                col,
                df[col].min(),
                df[col].max(),
            )

    # -- Duplicate detection -------------------------------------------------
    full_dupes = df.duplicated().sum()
    log.info("Full-row duplicates: %d", full_dupes)

    if key_columns:
        valid_keys = [k for k in key_columns if k in df.columns]
        if valid_keys:
            key_dupes = df.duplicated(subset=valid_keys).sum()
            log.info("Duplicate on key %s: %d", valid_keys, key_dupes)

    log.info("")  # blank line between profiles


# ===== Taxi-specific quality checks =========================================
def taxi_quality_checks(df: pd.DataFrame, name: str) -> None:
    """Run taxi-data-specific quality checks on a trip DataFrame."""
    log.info("--- Taxi data quality checks for %s ---", name)

    if "passenger_count" in df.columns:
        n_null_pax = df["passenger_count"].isna().sum()
        n_zero_pax = (df["passenger_count"] == 0).sum()
        log.info("  Null passenger_count: %d (%.2f%%)", n_null_pax, n_null_pax / len(df) * 100)
        log.info("  Zero passenger_count: %d (%.2f%%)", n_zero_pax, n_zero_pax / len(df) * 100)

    if "fare_amount" in df.columns:
        n_negative_fare = (df["fare_amount"] < 0).sum()
        log.info("  Negative fare_amount: %d (%.2f%%)", n_negative_fare, n_negative_fare / len(df) * 100)
        if n_negative_fare > 0:
            log.info("    Sample negative fares: %s", df.loc[df["fare_amount"] < 0, "fare_amount"].head(5).tolist())

    if "trip_distance" in df.columns:
        n_zero_dist = (df["trip_distance"] == 0).sum()
        log.info("  Zero trip_distance: %d (%.2f%%)", n_zero_dist, n_zero_dist / len(df) * 100)
        if "fare_amount" in df.columns:
            n_zero_dist_with_fare = ((df["trip_distance"] == 0) & (df["fare_amount"] > 0)).sum()
            log.info("  Zero distance BUT positive fare: %d", n_zero_dist_with_fare)

    if "rate_code_id" in df.columns:
        rate_dist = df["rate_code_id"].value_counts(dropna=False).to_dict()
        log.info("  rate_code_id distribution: %s", rate_dist)
        n_99 = (df["rate_code_id"] == 99).sum()
        if n_99 > 0:
            log.info("  rate_code_id=99 (unknown): %d records", n_99)

    # Check for trips outside expected date range
    pickup_col = None
    for candidate in ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"]:
        if candidate in df.columns:
            pickup_col = candidate
            break

    if pickup_col is not None:
        dt = pd.to_datetime(df[pickup_col], errors="coerce")
        log.info("  Pickup date range: %s to %s", dt.min(), dt.max())
        # Check for trips way outside expected range (e.g., year 2001 or 2099)
        n_old = (dt.dt.year < 2018).sum()
        n_future = (dt.dt.year > 2025).sum()
        if n_old > 0:
            log.info("  Trips with pickup before 2018: %d (likely data errors)", n_old)
        if n_future > 0:
            log.info("  Trips with pickup after 2025: %d (likely data errors)", n_future)

    dropoff_col = None
    for candidate in ["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime"]:
        if candidate in df.columns:
            dropoff_col = candidate
            break

    if pickup_col and dropoff_col:
        pickup_dt = pd.to_datetime(df[pickup_col], errors="coerce")
        dropoff_dt = pd.to_datetime(df[dropoff_col], errors="coerce")
        n_time_travel = (dropoff_dt < pickup_dt).sum()
        log.info("  Trips where dropoff < pickup (time travel): %d", n_time_travel)

    log.info("")


# ===== Main profiling logic ==================================================
def main() -> None:
    log.info("Starting data profiling against: %s", RAW_DIR)

    # ---- 1. Yellow Taxi Trips (Parquet) ------------------------------------
    yellow_files = sorted(glob(str(RAW_DIR / "yellow_tripdata_*.parquet")))
    if yellow_files:
        log.info("Found %d yellow taxi Parquet file(s)", len(yellow_files))
        for fpath in yellow_files:
            log.info("Reading %s ...", fpath)
            df = pd.read_parquet(fpath)
            profile_dataframe(
                df,
                Path(fpath).name,
                key_columns=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                             "pu_location_id", "do_location_id", "trip_distance"],
            )
            taxi_quality_checks(df, Path(fpath).name)
    else:
        log.warning("No yellow taxi Parquet files found in %s", RAW_DIR)

    # ---- 2. Green Taxi Trips (Parquet) -------------------------------------
    green_files = sorted(glob(str(RAW_DIR / "green_tripdata_*.parquet")))
    if green_files:
        log.info("Found %d green taxi Parquet file(s)", len(green_files))
        for fpath in green_files:
            log.info("Reading %s ...", fpath)
            df = pd.read_parquet(fpath)
            profile_dataframe(
                df,
                Path(fpath).name,
                key_columns=["lpep_pickup_datetime", "lpep_dropoff_datetime",
                             "pu_location_id", "do_location_id", "trip_distance"],
            )
            taxi_quality_checks(df, Path(fpath).name)
    else:
        log.info("No green taxi Parquet files found (skipping)")

    # ---- 3. FHV Trips (Parquet) --------------------------------------------
    fhv_files = sorted(glob(str(RAW_DIR / "fhvhv_tripdata_*.parquet")))
    if fhv_files:
        log.info("Found %d FHV Parquet file(s)", len(fhv_files))
        for fpath in fhv_files:
            log.info("Reading %s ...", fpath)
            df = pd.read_parquet(fpath)
            profile_dataframe(
                df,
                Path(fpath).name,
                key_columns=["pickup_datetime", "dropoff_datetime",
                             "pu_location_id", "do_location_id"],
            )
            taxi_quality_checks(df, Path(fpath).name)
    else:
        log.info("No FHV Parquet files found (skipping)")

    # ---- 4. Taxi Zone Lookup (CSV) -----------------------------------------
    zones_path = RAW_DIR / "taxi_zone_lookup.csv"
    if zones_path.exists():
        log.info("Reading %s ...", zones_path)
        zones = pd.read_csv(zones_path)
        profile_dataframe(zones, "taxi_zone_lookup.csv", key_columns=["LocationID"])

        # Borough distribution
        log.info("--- Borough distribution ---")
        log.info("  %s", dict(zones["Borough"].value_counts()))
    else:
        log.warning("taxi_zone_lookup.csv not found")

    # ---- 5. Dimension tables (CSV) -----------------------------------------
    for dim_file in ["vendors.csv", "rate_codes.csv", "payment_types.csv", "fhv_bases.csv"]:
        dim_path = RAW_DIR / dim_file
        if dim_path.exists():
            log.info("Reading %s ...", dim_path)
            dim = pd.read_csv(dim_path)
            profile_dataframe(dim, dim_file)
        else:
            log.info("%s not found (skipping)", dim_file)

    # ---- 6. Weather data (CSV) ---------------------------------------------
    weather_files = sorted(glob(str(RAW_DIR / "nyc_weather_*.csv")))
    if weather_files:
        for fpath in weather_files:
            log.info("Reading %s ...", fpath)
            weather = pd.read_csv(fpath)
            profile_dataframe(weather, Path(fpath).name)
    else:
        log.info("No weather CSV files found (skipping)")

    # ---- Summary -----------------------------------------------------------
    log.info("=" * 70)
    log.info("PROFILING COMPLETE")
    log.info("=" * 70)
    log.info("Key findings to investigate:")
    log.info("  1. Yellow taxi trips have null passenger_count values")
    log.info("  2. Some trips have negative fare_amount (refunds? errors?)")
    log.info("  3. Zero-distance trips with positive fares exist")
    log.info("  4. rate_code_id=99 appears (unknown rate code)")
    log.info("  5. Some trips have dropoff before pickup (impossible)")
    log.info("  6. Exact row duplicates exist in the TLC data")
    log.info("  7. Trips outside the expected month range may be present")


if __name__ == "__main__":
    main()
