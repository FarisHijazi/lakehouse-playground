"""
Module 02 - Exercise 4: Deduplicate Listening Events
=====================================================

Event streams almost always contain duplicates.  Common causes:
- At-least-once delivery guarantees (Kafka, Pub/Sub, SQS)
- Client retries on network errors
- Buggy client-side event tracking
- Replayed data during incident recovery

Deduplication strategies:
1. **Exact dedup on event_id**: If two records have the same event_id, they are
   the same event.  Keep the one with the latest timestamp (most recent data
   is usually the most correct -- e.g., the server may have enriched it).
2. **Semantic dedup on composite key**: Same user + same episode + same action
   within a time window.  Useful when event_id is unreliable or missing.
3. **Windowed dedup**: Only deduplicate within a time window (e.g., last 7 days)
   for performance at scale.

This script implements strategy #1 (exact dedup on event_id).

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


def load_all_events(events_dir: Path) -> pd.DataFrame:
    """Read all JSONL files from the listening events directory.

    We read each file individually and concatenate rather than using a single
    glob pattern because:
    - It gives us visibility into per-file errors
    - We can log progress for large directories
    - In production, this pattern extends naturally to parallel reading
    """
    jsonl_files = sorted(glob(str(events_dir / "events_*.jsonl")))
    log.info("Found %d JSONL files to process", len(jsonl_files))

    if not jsonl_files:
        raise FileNotFoundError(f"No JSONL files found in {events_dir}")

    frames = []
    total_rows = 0
    for i, fpath in enumerate(jsonl_files):
        try:
            chunk = pd.read_json(fpath, lines=True)
            frames.append(chunk)
            total_rows += len(chunk)
        except Exception as exc:
            log.warning("Failed to read %s: %s", fpath, exc)

        # Log progress every 500 files
        if (i + 1) % 500 == 0:
            log.info("  Read %d / %d files (%d rows so far)", i + 1, len(jsonl_files), total_rows)

    df = pd.concat(frames, ignore_index=True)
    log.info("Loaded %d total rows from %d files", len(df), len(jsonl_files))
    return df


def deduplicate_by_event_id(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate events, keeping the record with the latest timestamp.

    Algorithm:
    1. Parse timestamp column to datetime for proper comparison.
    2. Sort by event_id and timestamp (descending) so the latest record comes first.
    3. Drop duplicates on event_id, keeping the first occurrence (= latest timestamp).

    Why keep the latest?  In most event pipelines, a re-sent event may have been
    enriched with additional server-side data (e.g., geo-IP lookup completed on
    retry).  The latest version is therefore the most complete.
    """
    log.info("Deduplicating by event_id ...")

    # Parse timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Count duplicates before removal
    n_total = len(df)
    n_unique_ids = df["event_id"].nunique()
    n_duplicates = n_total - n_unique_ids
    log.info("  Total rows: %d", n_total)
    log.info("  Unique event_ids: %d", n_unique_ids)
    log.info("  Duplicate rows to remove: %d (%.1f%%)", n_duplicates, n_duplicates / n_total * 100)

    # Show duplicate distribution
    dup_counts = df.groupby("event_id").size()
    multi_dupes = dup_counts[dup_counts > 1]
    if len(multi_dupes) > 0:
        log.info("  Duplicate frequency distribution:")
        log.info("    Events appearing 2x: %d", (multi_dupes == 2).sum())
        log.info("    Events appearing 3x: %d", (multi_dupes == 3).sum())
        log.info("    Events appearing 4x+: %d", (multi_dupes > 3).sum())

    # Sort by event_id and timestamp descending, then keep first occurrence
    df = df.sort_values(["event_id", "timestamp"], ascending=[True, False])
    df = df.drop_duplicates(subset=["event_id"], keep="first")

    log.info("  After deduplication: %d rows", len(df))

    # Also remove rows with null event_id (these cannot be deduplicated meaningfully)
    n_null_ids = df["event_id"].isna().sum()
    if n_null_ids > 0:
        log.warning("  Dropping %d rows with null event_id", n_null_ids)
        df = df.dropna(subset=["event_id"])

    return df.reset_index(drop=True)


def main() -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Load all events ---------------------------------------------------
    events_dir = RAW_DIR / "listening_events"
    df = load_all_events(events_dir)

    # ---- Profile before deduplication --------------------------------------
    log.info("Pre-deduplication profile:")
    log.info("  Columns: %s", list(df.columns))
    log.info("  Null counts:")
    for col in df.columns:
        n_null = df[col].isna().sum()
        if n_null > 0:
            log.info("    %s: %d nulls", col, n_null)

    # ---- Deduplicate -------------------------------------------------------
    df = deduplicate_by_event_id(df)

    # ---- Write output ------------------------------------------------------
    output_path = SILVER_DIR / "listening_events_deduped.parquet"

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
    ])

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, str(output_path), compression="snappy")

    log.info("Wrote deduplicated events to: %s", output_path)
    log.info("File size: %.2f MB", output_path.stat().st_size / (1024 * 1024))


if __name__ == "__main__":
    main()
