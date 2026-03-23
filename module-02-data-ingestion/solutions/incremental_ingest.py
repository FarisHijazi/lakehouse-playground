"""
Module 02 - Exercise 7: Incremental Ingestion
===============================================

In production, you never want to reprocess all historical data on every pipeline
run.  Incremental ingestion processes only NEW data since the last run.

This script implements a file-level checkpoint system for yellow taxi trip data:
1. Maintain a JSON checkpoint file listing all previously processed files.
2. On each run, scan the source directory for files not in the checkpoint.
3. Read only the new files.
4. Deduplicate the new trips (within the batch AND against previously seen IDs).
5. Append the new data to the output.
6. Update the checkpoint AFTER successful write (atomic checkpoint pattern).

Why checkpoint after write?  If the script crashes between writing data and
updating the checkpoint, the next run will re-read those files.  Since we
deduplicate, this is safe -- we get at-least-once processing with exactly-once
output.  If we checkpointed BEFORE writing, a crash would cause data loss.

In production, tools like Apache Airflow, Dagster, or Prefect manage this
automatically with DAG runs and task state.  But understanding the mechanics
matters for debugging and for situations where you need a lightweight solution.

Usage:
    python solutions/incremental_ingest.py

    Run it twice:
    - First run: processes all files.
    - Second run: "No new files to process."
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
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
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "processed" / "checkpoints"

CHECKPOINT_FILE = CHECKPOINT_DIR / "yellow_trips_checkpoint.json"
OUTPUT_DIR = SILVER_DIR / "yellow_trips_incremental"

TRIPS_SCHEMA = pa.schema([
    pa.field("VendorID", pa.int64()),
    pa.field("tpep_pickup_datetime", pa.timestamp("us")),
    pa.field("tpep_dropoff_datetime", pa.timestamp("us")),
    pa.field("passenger_count", pa.float64()),
    pa.field("trip_distance", pa.float64()),
    pa.field("RatecodeID", pa.float64()),
    pa.field("store_and_fwd_flag", pa.string()),
    pa.field("PULocationID", pa.int64()),
    pa.field("DOLocationID", pa.int64()),
    pa.field("payment_type", pa.int64()),
    pa.field("fare_amount", pa.float64()),
    pa.field("extra", pa.float64()),
    pa.field("mta_tax", pa.float64()),
    pa.field("tip_amount", pa.float64()),
    pa.field("tolls_amount", pa.float64()),
    pa.field("improvement_surcharge", pa.float64()),
    pa.field("total_amount", pa.float64()),
    pa.field("congestion_surcharge", pa.float64()),
    pa.field("airport_fee", pa.float64()),
])


# ---------------------------------------------------------------------------
# Checkpoint management
# ---------------------------------------------------------------------------

def load_checkpoint() -> dict:
    """Load the checkpoint file, or return an empty checkpoint if none exists.

    Checkpoint structure:
    {
        "processed_files": ["yellow_tripdata_2023-01.parquet", ...],
        "last_run": "2024-01-15T10:30:00Z",
        "total_trips_processed": 12345
    }
    """
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            checkpoint = json.load(f)
        log.info("Loaded checkpoint: %d files previously processed", len(checkpoint.get("processed_files", [])))
        return checkpoint

    log.info("No checkpoint found -- this is the first run")
    return {
        "processed_files": [],
        "last_run": None,
        "total_trips_processed": 0,
    }


def save_checkpoint(checkpoint: dict) -> None:
    """Save the checkpoint file atomically.

    We write to a temp file first and rename.  On most filesystems, rename()
    is atomic, so a crash during write won't corrupt the checkpoint.
    """
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = CHECKPOINT_FILE.with_suffix(".tmp")

    with open(tmp_path, "w") as f:
        json.dump(checkpoint, f, indent=2, default=str)

    # Atomic rename
    tmp_path.rename(CHECKPOINT_FILE)
    log.info("Checkpoint saved: %d files tracked", len(checkpoint["processed_files"]))


def load_seen_trip_keys() -> set:
    """Load composite keys from previously written output files.

    For cross-batch deduplication, we build a composite key from
    (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID,
    DOLocationID) since yellow taxi data has no natural unique ID.
    We read only those columns from existing output Parquet files
    (column pruning makes this fast).
    """
    if not OUTPUT_DIR.exists():
        return set()

    parquet_files = list(OUTPUT_DIR.glob("*.parquet"))
    if not parquet_files:
        return set()

    KEY_COLS = ["VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"]
    seen_keys = set()
    for pf in parquet_files:
        table = pq.read_table(str(pf), columns=KEY_COLS)
        df = table.to_pandas()
        for _, row in df.iterrows():
            seen_keys.add((row["VendorID"], str(row["tpep_pickup_datetime"]),
                           row["PULocationID"], row["DOLocationID"]))

    log.info("Loaded %d previously seen trip keys from %d output files", len(seen_keys), len(parquet_files))
    return seen_keys


# ---------------------------------------------------------------------------
# Ingestion logic
# ---------------------------------------------------------------------------

def find_new_files(processed_files: list[str]) -> list[str]:
    """Compare source directory against checkpoint to find unprocessed files."""
    all_files = sorted(glob(str(RAW_DIR / "yellow_tripdata_*.parquet")))
    # Use basenames for comparison (checkpoint stores basenames, not full paths)
    all_basenames = {Path(f).name for f in all_files}
    processed_set = set(processed_files)

    new_basenames = sorted(all_basenames - processed_set)
    log.info("Source files: %d total, %d already processed, %d new",
             len(all_basenames), len(processed_set), len(new_basenames))

    # Return full paths for the new files
    new_full_paths = [str(RAW_DIR / name) for name in new_basenames]
    return new_full_paths


def ingest_new_files(file_paths: list[str], seen_trip_keys: set) -> pd.DataFrame | None:
    """Read new files, deduplicate within batch and against seen keys.

    Returns the deduplicated DataFrame, or None if no new data.
    """
    if not file_paths:
        return None

    log.info("Reading %d new files ...", len(file_paths))
    frames = []
    for fpath in file_paths:
        try:
            chunk = pd.read_parquet(fpath)
            frames.append(chunk)
            log.info("  Read %s: %d rows", Path(fpath).name, len(chunk))
        except Exception as exc:
            log.warning("Failed to read %s: %s", fpath, exc)

    if not frames:
        return None

    df = pd.concat(frames, ignore_index=True)
    n_raw = len(df)
    log.info("Read %d raw trips from new files", n_raw)

    # Step 1: Deduplicate within the new batch using composite key
    DEDUP_COLS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                  "PULocationID", "DOLocationID", "fare_amount"]
    n_before_batch_dedup = len(df)
    df = df.drop_duplicates(subset=DEDUP_COLS, keep="first")
    n_batch_dupes = n_before_batch_dedup - len(df)
    log.info("Removed %d within-batch duplicates", n_batch_dupes)

    # Step 2: Remove trips that were already processed in previous runs
    if seen_trip_keys:
        n_before_cross_dedup = len(df)
        df["_trip_key"] = list(zip(
            df["VendorID"], df["tpep_pickup_datetime"].astype(str),
            df["PULocationID"], df["DOLocationID"]
        ))
        df = df[~df["_trip_key"].isin(seen_trip_keys)]
        df = df.drop(columns=["_trip_key"])
        n_cross_dupes = n_before_cross_dedup - len(df)
        log.info("Removed %d cross-batch duplicates (already in output)", n_cross_dupes)

    if len(df) == 0:
        log.info("No new unique trips after deduplication")
        return None

    log.info("New unique trips to write: %d (from %d raw)", len(df), n_raw)
    return df.reset_index(drop=True)


def append_to_output(df: pd.DataFrame) -> None:
    """Append new trips to the output directory.

    We write each batch as a separate Parquet file with a timestamp-based name.
    This is append-friendly: no need to read and rewrite the entire dataset.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Generate a unique filename for this batch
    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = OUTPUT_DIR / f"batch_{batch_ts}.parquet"

    table = pa.Table.from_pandas(df, schema=TRIPS_SCHEMA, preserve_index=False)
    pq.write_table(table, str(output_path), compression="snappy")

    log.info("Wrote %d trips to: %s (%.2f MB)",
             len(df), output_path.name, output_path.stat().st_size / (1024 * 1024))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=" * 60)
    log.info("INCREMENTAL INGESTION: Yellow Taxi Trips")
    log.info("=" * 60)

    # 1. Load checkpoint
    checkpoint = load_checkpoint()

    # 2. Find new files
    new_files = find_new_files(checkpoint["processed_files"])

    if not new_files:
        log.info("No new files to process. Pipeline is up to date.")
        return

    # 3. Load previously seen trip keys for cross-batch dedup
    seen_keys = load_seen_trip_keys()

    # 4. Ingest and deduplicate new files
    df = ingest_new_files(new_files, seen_keys)

    if df is None:
        log.info("No new unique trips. Updating checkpoint only.")
        # Still update checkpoint so we don't re-read these files
        checkpoint["processed_files"].extend([Path(f).name for f in new_files])
        checkpoint["last_run"] = datetime.now(timezone.utc).isoformat()
        save_checkpoint(checkpoint)
        return

    # 5. Write output (BEFORE updating checkpoint -- crash-safe ordering)
    append_to_output(df)

    # 6. Update checkpoint (AFTER successful write)
    checkpoint["processed_files"].extend([Path(f).name for f in new_files])
    checkpoint["total_trips_processed"] = checkpoint.get("total_trips_processed", 0) + len(df)
    checkpoint["last_run"] = datetime.now(timezone.utc).isoformat()
    save_checkpoint(checkpoint)

    # 7. Summary
    log.info("")
    log.info("=" * 60)
    log.info("INGESTION COMPLETE")
    log.info("  New files processed: %d", len(new_files))
    log.info("  New trips written: %d", len(df))
    log.info("  Total files tracked: %d", len(checkpoint["processed_files"]))
    log.info("  Total trips processed: %d", checkpoint["total_trips_processed"])
    log.info("=" * 60)


if __name__ == "__main__":
    main()
