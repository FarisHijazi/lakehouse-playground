"""
Module 02 - Exercise 5: Partition Listening Events by Date
===========================================================

Partitioning organizes data files into a directory tree based on column values.
When a query filters on the partition column, the query engine can skip entire
directories (partition pruning), dramatically reducing I/O.

Example partition layout:
    listening_events_partitioned/
    +-- year=2018/
    |   +-- month=01/
    |       +-- data.parquet
    +-- year=2019/
        +-- month=06/
            +-- data.parquet

Partition design tradeoffs:
- Too few partitions (e.g., just by year): each file is large, no granularity.
- Too many partitions (e.g., by year/month/day/hour): thousands of tiny files.
  Small files cause metadata overhead and slow down listing and planning.
- Sweet spot for ~200k rows: year/month gives ~80 partitions with ~2,500 rows each.

This script reads the deduplicated events from Exercise 4 and writes them
as partitioned Parquet using PyArrow's write_to_dataset().

Usage:
    python solutions/partition_events.py
"""

import logging
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
SILVER_DIR = PROJECT_ROOT / "data" / "processed" / "silver"
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def load_deduped_events() -> pd.DataFrame:
    """Load deduplicated events from Exercise 4, or fall back to raw data.

    In a real pipeline, each step should be idempotent and able to detect
    whether its upstream dependency has been run.  Here we gracefully
    fall back to raw data so the exercise can be run independently.
    """
    deduped_path = SILVER_DIR / "listening_events_deduped.parquet"
    if deduped_path.exists():
        log.info("Loading deduplicated events from: %s", deduped_path)
        df = pd.read_parquet(deduped_path)
    else:
        log.warning("Deduplicated file not found at %s", deduped_path)
        log.warning("Falling back to raw JSONL files (run deduplicate_events.py first for best results)")
        from glob import glob

        events_dir = RAW_DIR / "listening_events"
        jsonl_files = sorted(glob(str(events_dir / "events_*.jsonl")))
        frames = [pd.read_json(f, lines=True) for f in jsonl_files]
        df = pd.concat(frames, ignore_index=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    log.info("Loaded %d events", len(df))
    return df


def main() -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = SILVER_DIR / "listening_events_partitioned"

    # Clean up previous output to ensure idempotency
    if output_dir.exists():
        import shutil
        shutil.rmtree(output_dir)
        log.info("Removed previous output at %s", output_dir)

    # ---- Load data ---------------------------------------------------------
    df = load_deduped_events()

    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Drop rows with null timestamps (can't partition on null)
    n_null_ts = df["timestamp"].isna().sum()
    if n_null_ts > 0:
        log.warning("Dropping %d rows with null timestamp", n_null_ts)
        df = df.dropna(subset=["timestamp"])

    # ---- Create partition columns ------------------------------------------
    # We extract year and month as string columns.  Using strings ensures the
    # directory names are human-readable (year=2018/month=01 not month=1).
    df["year"] = df["timestamp"].dt.year.astype(str)
    df["month"] = df["timestamp"].dt.month.astype(str).str.zfill(2)

    log.info("Partition distribution:")
    partition_counts = df.groupby(["year", "month"]).size().reset_index(name="count")
    for _, row in partition_counts.iterrows():
        log.info("  year=%s / month=%s : %d records", row["year"], row["month"], row["count"])
    log.info("Total partitions: %d", len(partition_counts))

    # ---- Write partitioned Parquet -----------------------------------------
    # PyArrow's write_to_dataset handles directory creation and file naming.
    # We exclude the partition columns from the Parquet data itself (they are
    # encoded in the directory path and reconstructed on read).

    schema = pa.schema([
        pa.field("event_id", pa.string()),
        pa.field("user_id", pa.string()),
        pa.field("episode_id", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("timestamp", pa.timestamp("us")),
        pa.field("listened_seconds", pa.int64()),
        pa.field("platform", pa.string()),
        pa.field("country", pa.string()),
        pa.field("app_version", pa.string()),
        pa.field("year", pa.string()),
        pa.field("month", pa.string()),
    ])

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=["year", "month"],
        compression="snappy",
    )

    # ---- Report results ----------------------------------------------------
    # Count the output files and their sizes
    parquet_files = list(output_dir.rglob("*.parquet"))
    total_size_mb = sum(f.stat().st_size for f in parquet_files) / (1024 * 1024)

    log.info("Wrote %d Parquet files to: %s", len(parquet_files), output_dir)
    log.info("Total size: %.2f MB", total_size_mb)

    # Verify we can read the partitioned dataset back
    log.info("Verification: reading partitioned dataset back ...")
    readback = pq.read_table(str(output_dir))
    log.info("  Read back %d rows, %d columns", readback.num_rows, readback.num_columns)

    # Demonstrate partition pruning: read only one month
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(output_dir), format="parquet", partitioning="hive")
    filtered = dataset.to_table(filter=(ds.field("year") == 2020) & (ds.field("month") == 6))
    log.info("  Filtered read (year=2020, month=06): %d rows", filtered.num_rows)
    log.info("  (Only files in year=2020/month=06/ were read -- partition pruning in action)")


if __name__ == "__main__":
    main()
