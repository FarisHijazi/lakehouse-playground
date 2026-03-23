"""
Module 02 - Exercise 4: Deduplicate Taxi Trips
================================================

Real NYC TLC trip data contains genuine duplicates. Common causes:
- Meter system resubmissions when the payment terminal retries
- Vendor data processing bugs during batch uploads to TLC
- System-of-record reconciliation producing duplicate records
- Data pipeline replays after incident recovery

Unlike event streams with unique IDs, taxi trips have NO natural unique key.
We must deduplicate using a composite key of trip attributes.

Deduplication strategies used here:
1. **Exact row dedup**: Two rows are byte-for-byte identical across all columns.
   These are unambiguous duplicates.
2. **Composite key dedup**: Same vendor, pickup/dropoff times, locations, distance,
   and fare -- almost certainly the same trip even if other fields differ slightly.

Usage:
    python solutions/deduplicate_events.py
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

# Composite key for semantic deduplication.
# These columns together identify a unique trip with high confidence.
COMPOSITE_KEY = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
]


def load_yellow_trips() -> pd.DataFrame:
    """Load all yellow taxi trip Parquet files from the raw directory."""
    yellow_files = sorted(glob(str(RAW_DIR / "yellow_tripdata_*.parquet")))
    if not yellow_files:
        raise FileNotFoundError(f"No yellow taxi Parquet files found in {RAW_DIR}")

    log.info("Found %d yellow taxi file(s) to process", len(yellow_files))
    frames = []
    for fpath in yellow_files:
        log.info("  Reading %s ...", Path(fpath).name)
        df = pd.read_parquet(fpath)
        frames.append(df)
        log.info("    %d rows", len(df))

    df = pd.concat(frames, ignore_index=True)
    log.info("Loaded %d total rows from %d files", len(df), len(yellow_files))
    return df


def deduplicate_exact(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact row-level duplicates (all columns identical).

    This is the safest form of deduplication. If every single column value
    matches, the rows are unambiguously the same record.
    """
    log.info("Phase 1: Exact row deduplication ...")
    n_before = len(df)

    # Count exact duplicates
    n_exact_dupes = df.duplicated().sum()
    log.info("  Exact row duplicates found: %d (%.3f%%)",
             n_exact_dupes, n_exact_dupes / n_before * 100)

    if n_exact_dupes > 0:
        # Show a sample of duplicated rows
        dup_mask = df.duplicated(keep=False)
        sample_dupes = df[dup_mask].head(4)
        log.info("  Sample duplicated rows:")
        for _, row in sample_dupes.iterrows():
            log.info("    vendor=%s pickup=%s dropoff=%s dist=%.1f fare=%.2f",
                     row.get("VendorID"), row.get("tpep_pickup_datetime"),
                     row.get("tpep_dropoff_datetime"), row.get("trip_distance", 0),
                     row.get("fare_amount", 0))

    df = df.drop_duplicates()
    log.info("  After exact dedup: %d rows (removed %d)", len(df), n_before - len(df))
    return df


def deduplicate_composite_key(df: pd.DataFrame) -> pd.DataFrame:
    """Remove semantic duplicates using a composite key.

    Two records with the same (vendor, pickup_time, dropoff_time, pickup_loc,
    dropoff_loc, distance, fare) are almost certainly the same trip, even if
    other columns like tip_amount or payment_type differ slightly.

    When duplicates are found, we keep the first occurrence. In taxi data,
    there is no "latest is best" heuristic like in event streams -- both
    copies are equally valid, so we just pick one deterministically.
    """
    log.info("Phase 2: Composite key deduplication ...")
    log.info("  Key columns: %s", COMPOSITE_KEY)

    # Verify all key columns exist
    missing_keys = [k for k in COMPOSITE_KEY if k not in df.columns]
    if missing_keys:
        log.warning("  Missing key columns: %s -- skipping composite dedup", missing_keys)
        return df

    n_before = len(df)

    # Count duplicates on composite key
    n_key_dupes = df.duplicated(subset=COMPOSITE_KEY).sum()
    log.info("  Composite key duplicates found: %d (%.3f%%)",
             n_key_dupes, n_key_dupes / n_before * 100)

    if n_key_dupes > 0:
        # Analyze duplicate frequency
        dup_counts = df.groupby(COMPOSITE_KEY).size()
        multi = dup_counts[dup_counts > 1]
        log.info("  Duplicate frequency distribution:")
        log.info("    Trips appearing 2x: %d", (multi == 2).sum())
        log.info("    Trips appearing 3x: %d", (multi == 3).sum())
        log.info("    Trips appearing 4x+: %d", (multi > 3).sum())

    df = df.drop_duplicates(subset=COMPOSITE_KEY, keep="first")
    n_removed = n_before - len(df)
    log.info("  After composite key dedup: %d rows (removed %d)", len(df), n_removed)

    return df


def main() -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Load all trips ----------------------------------------------------
    df = load_yellow_trips()

    # ---- Profile before deduplication --------------------------------------
    log.info("Pre-deduplication profile:")
    log.info("  Columns: %s", list(df.columns))
    log.info("  Null counts:")
    for col in df.columns:
        n_null = df[col].isna().sum()
        if n_null > 0:
            log.info("    %s: %d nulls (%.2f%%)", col, n_null, n_null / len(df) * 100)

    n_original = len(df)

    # ---- Phase 1: Exact row dedup ------------------------------------------
    df = deduplicate_exact(df)

    # ---- Phase 2: Composite key dedup --------------------------------------
    df = deduplicate_composite_key(df)

    # ---- Summary -----------------------------------------------------------
    n_total_removed = n_original - len(df)
    log.info("")
    log.info("Deduplication summary:")
    log.info("  Original rows:  %d", n_original)
    log.info("  Final rows:     %d", len(df))
    log.info("  Total removed:  %d (%.3f%%)", n_total_removed, n_total_removed / n_original * 100)

    # ---- Write output ------------------------------------------------------
    output_path = SILVER_DIR / "yellow_trips_deduped.parquet"
    df.to_parquet(output_path, index=False, compression="snappy", engine="pyarrow")

    log.info("Wrote deduplicated trips to: %s", output_path)
    log.info("File size: %.2f MB", output_path.stat().st_size / (1024 * 1024))


if __name__ == "__main__":
    main()
