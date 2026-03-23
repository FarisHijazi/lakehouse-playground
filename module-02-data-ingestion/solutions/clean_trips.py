"""
Module 02 - Exercise 3: Clean Messy Taxi Trip Data
====================================================

This script tackles real-world data quality issues in NYC yellow taxi trip data.

The TLC data is *genuinely* messy -- these are not synthetic problems:
- Null passenger_count (float column because of NaN in the source)
- Negative fare_amount (refunds? meter errors? we don't know)
- Zero-distance trips with positive fares (flat-rate? meter not running?)
- rate_code_id=99 (unknown -- a real data quality issue in the TLC system)
- Trips where dropoff is before pickup (clock skew or GPS errors)
- Extreme outliers: $10,000 fares, 500-mile trips within Manhattan

These problems appear in virtually every production dataset. The patterns
here -- null handling, outlier capping, derived columns -- are reusable
across projects.

Usage:
    python solutions/clean_trips.py
"""

import logging
from glob import glob
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_DIR = PROJECT_ROOT / "data" / "processed" / "silver"


def load_yellow_trips() -> pd.DataFrame:
    """Load all yellow taxi trip Parquet files from the raw directory."""
    yellow_files = sorted(glob(str(RAW_DIR / "yellow_tripdata_*.parquet")))
    if not yellow_files:
        raise FileNotFoundError(f"No yellow taxi Parquet files found in {RAW_DIR}")

    log.info("Found %d yellow taxi file(s)", len(yellow_files))
    frames = []
    for fpath in yellow_files:
        log.info("  Reading %s ...", Path(fpath).name)
        df = pd.read_parquet(fpath)
        frames.append(df)
        log.info("    %d rows loaded", len(df))

    df = pd.concat(frames, ignore_index=True)
    log.info("Total yellow taxi rows: %d", len(df))
    return df


def main() -> None:
    log.info("Cleaning yellow taxi trip data")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Read raw data -----------------------------------------------------
    df = load_yellow_trips()
    n_original = len(df)
    log.info("Original shape: %d rows, %d columns", len(df), len(df.columns))

    # ---- Pre-cleaning profile ----------------------------------------------
    log.info("Pre-cleaning data quality summary:")
    log.info("  Null passenger_count: %d (%.2f%%)",
             df["passenger_count"].isna().sum(),
             df["passenger_count"].isna().sum() / len(df) * 100)
    log.info("  Negative fare_amount: %d", (df["fare_amount"] < 0).sum())
    log.info("  Zero trip_distance: %d", (df["trip_distance"] == 0).sum())
    log.info("  rate_code_id=99: %d", (df["RatecodeID"] == 99).sum())
    log.info("  Null RatecodeID: %d", df["RatecodeID"].isna().sum())

    # ---- Step 1: Handle null values ----------------------------------------
    log.info("Step 1: Handling null values ...")

    # passenger_count: fill nulls with 1 (single-rider assumption)
    n_null_pax = df["passenger_count"].isna().sum()
    df["passenger_count"] = df["passenger_count"].fillna(1.0)
    log.info("  Filled %d null passenger_count with 1", n_null_pax)

    # RatecodeID: replace 99 and nulls with 1 (standard rate)
    n_bad_rate = (df["RatecodeID"].isna() | (df["RatecodeID"] == 99)).sum()
    df.loc[df["RatecodeID"].isna() | (df["RatecodeID"] == 99), "RatecodeID"] = 1.0
    log.info("  Fixed %d null/unknown RatecodeID values -> 1 (standard rate)", n_bad_rate)

    # store_and_fwd_flag: fill nulls with 'N'
    n_null_fwd = df["store_and_fwd_flag"].isna().sum()
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].fillna("N")
    log.info("  Filled %d null store_and_fwd_flag with 'N'", n_null_fwd)

    # congestion_surcharge: fill nulls with 0
    if "congestion_surcharge" in df.columns:
        n_null_cong = df["congestion_surcharge"].isna().sum()
        df["congestion_surcharge"] = df["congestion_surcharge"].fillna(0.0)
        log.info("  Filled %d null congestion_surcharge with 0.0", n_null_cong)

    # airport_fee: fill nulls with 0
    if "airport_fee" in df.columns:
        n_null_air = df["airport_fee"].isna().sum()
        df["airport_fee"] = df["airport_fee"].fillna(0.0)
        log.info("  Filled %d null airport_fee with 0.0", n_null_air)

    # ---- Step 2: Remove bad records ----------------------------------------
    log.info("Step 2: Removing bad records ...")
    n_before = len(df)

    # Negative fare_amount
    mask_neg_fare = df["fare_amount"] < 0
    n_neg_fare = mask_neg_fare.sum()
    log.info("  Negative fare_amount: %d rows flagged", n_neg_fare)

    # Negative trip_distance
    mask_neg_dist = df["trip_distance"] < 0
    n_neg_dist = mask_neg_dist.sum()
    log.info("  Negative trip_distance: %d rows flagged", n_neg_dist)

    # Excessive passenger_count (taxi max is ~6; be generous up to 9)
    mask_high_pax = df["passenger_count"] > 9
    n_high_pax = mask_high_pax.sum()
    log.info("  passenger_count > 9: %d rows flagged", n_high_pax)

    # Time travel: dropoff before pickup
    mask_time_travel = df["tpep_dropoff_datetime"] < df["tpep_pickup_datetime"]
    n_time_travel = mask_time_travel.sum()
    log.info("  Dropoff before pickup: %d rows flagged", n_time_travel)

    # Remove all flagged records
    bad_mask = mask_neg_fare | mask_neg_dist | mask_high_pax | mask_time_travel
    df = df[~bad_mask].copy()
    n_removed = n_before - len(df)
    log.info("  Removed %d bad records total (%d -> %d rows)", n_removed, n_before, len(df))

    # ---- Step 3: Cap outliers ----------------------------------------------
    log.info("Step 3: Capping outliers ...")

    # trip_distance > 200 miles (NYC is ~35 miles long)
    n_dist_cap = (df["trip_distance"] > 200).sum()
    df.loc[df["trip_distance"] > 200, "trip_distance"] = 200.0
    log.info("  Capped %d trips with distance > 200 miles", n_dist_cap)

    # fare_amount > 1000 (even JFK flat rate is ~$70)
    n_fare_cap = (df["fare_amount"] > 1000).sum()
    df.loc[df["fare_amount"] > 1000, "fare_amount"] = 1000.0
    log.info("  Capped %d trips with fare > $1,000", n_fare_cap)

    # tip_amount > 500
    n_tip_cap = (df["tip_amount"] > 500).sum()
    df.loc[df["tip_amount"] > 500, "tip_amount"] = 500.0
    log.info("  Capped %d trips with tip > $500", n_tip_cap)

    # ---- Step 4: Add derived columns ---------------------------------------
    log.info("Step 4: Adding derived columns ...")

    # Trip duration in minutes
    df["trip_duration_min"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60.0
    ).round(2)
    log.info("  Added trip_duration_min (mean=%.1f, max=%.1f)",
             df["trip_duration_min"].mean(), df["trip_duration_min"].max())

    # Pickup date (for partitioning and aggregation)
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

    log.info("  Added pickup_date")

    # ---- Step 5: Validate --------------------------------------------------
    log.info("Running validation checks ...")

    assert df["passenger_count"].isna().sum() == 0, "passenger_count still has nulls!"
    assert df["RatecodeID"].isna().sum() == 0, "RatecodeID still has nulls!"
    assert (df["fare_amount"] < 0).sum() == 0, "Negative fare_amount values remain!"
    assert (df["trip_distance"] < 0).sum() == 0, "Negative trip_distance values remain!"
    assert (df["tpep_dropoff_datetime"] < df["tpep_pickup_datetime"]).sum() == 0, (
        "Time-travel trips remain!"
    )

    log.info("All validations passed.")

    # ---- Step 6: Write to Parquet ------------------------------------------
    output_path = SILVER_DIR / "yellow_trips_clean.parquet"

    # Let pyarrow infer the schema from the cleaned data.
    # In production you might define an explicit silver schema.
    df.to_parquet(output_path, index=False, compression="snappy", engine="pyarrow")

    log.info("Wrote cleaned trips to: %s", output_path)
    log.info("Final shape: %d rows, %d columns", len(df), len(df.columns))
    log.info("Records removed: %d (%.2f%% of original)",
             n_original - len(df), (n_original - len(df)) / n_original * 100)
    log.info("File size: %.2f MB", output_path.stat().st_size / (1024 * 1024))


if __name__ == "__main__":
    main()
