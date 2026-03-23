"""
Module 02 - Exercise 2: Convert CSV/JSON to Parquet
====================================================

This script converts every raw data source from its original format (CSV, JSON,
JSONL) into Parquet with explicit PyArrow schemas and Snappy compression.

Why Parquet?
- Columnar storage: only reads the columns your query needs (column pruning).
- Built-in compression: Snappy gives 5-10x size reduction over CSV.
- Embedded schema: data types are stored in the file, no guessing.
- Predicate pushdown: query engines can skip row groups that don't match filters.

Key design decisions:
- We define explicit schemas instead of relying on pandas type inference.
  Inference is fragile -- a column of "1, 2, 3, NA" might be inferred as float
  when you intended int.  Explicit schemas catch type mismatches early.
- We write to data/processed/bronze/ because this is a raw-to-bronze conversion
  (no cleaning yet, just format change).

Usage:
    python solutions/convert_formats.py
"""

import json
import logging
import os
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
BRONZE_DIR = PROJECT_ROOT / "data" / "processed" / "bronze"


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------
# Defining schemas explicitly is a best practice.  It documents the expected
# shape of your data and causes a loud failure if the source data changes in
# an unexpected way (e.g., a column is renamed or a new type appears).

PODCASTS_SCHEMA = pa.schema([
    pa.field("podcast_id", pa.string()),
    pa.field("name", pa.string()),
    pa.field("name_en", pa.string()),
    pa.field("category", pa.string()),
    pa.field("language", pa.string()),
    pa.field("host", pa.string()),
    pa.field("created_at", pa.string()),  # Keep as string in bronze; parse in silver
])

EPISODES_SCHEMA = pa.schema([
    pa.field("episode_id", pa.string()),
    pa.field("podcast_id", pa.string()),
    pa.field("title", pa.string()),
    pa.field("published_at", pa.string()),
    pa.field("duration_seconds", pa.int32()),
    pa.field("season", pa.int32()),
    pa.field("episode_number", pa.int32()),
])

USERS_SCHEMA = pa.schema([
    pa.field("user_id", pa.string()),
    pa.field("name", pa.string()),
    pa.field("email", pa.string()),
    pa.field("country", pa.string()),
    pa.field("city", pa.string()),
    pa.field("platform", pa.string()),
    pa.field("signup_date", pa.string()),  # Mixed formats -- keep as string in bronze
    pa.field("subscription_type", pa.string()),
    pa.field("age", pa.float64()),  # float because of nulls (pandas int limitation)
    pa.field("gender", pa.string()),
])

LISTENING_EVENTS_SCHEMA = pa.schema([
    pa.field("event_id", pa.string()),
    pa.field("user_id", pa.string()),
    pa.field("episode_id", pa.string()),
    pa.field("event_type", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("listened_seconds", pa.int64()),
    pa.field("platform", pa.string()),
    pa.field("country", pa.string()),
    pa.field("app_version", pa.string()),
])

CDN_LOGS_SCHEMA = pa.schema([
    pa.field("log_id", pa.string()),
    pa.field("event_id", pa.string()),
    pa.field("user_id", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("isp", pa.string()),
    pa.field("bitrate", pa.string()),
    pa.field("buffer_events", pa.int32()),
    pa.field("rebuffer_ratio", pa.float64()),
    pa.field("startup_time_ms", pa.int32()),
    pa.field("error_type", pa.string()),
    pa.field("cdn_node", pa.string()),
    pa.field("bytes_transferred", pa.int64()),
])

AD_EVENTS_SCHEMA = pa.schema([
    pa.field("ad_event_id", pa.string()),
    pa.field("event_id", pa.string()),
    pa.field("user_id", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("ad_type", pa.string()),
    pa.field("action", pa.string()),
    pa.field("advertiser", pa.string()),
    pa.field("campaign_id", pa.string()),
    pa.field("revenue_sar", pa.float64()),
    pa.field("duration_seconds", pa.float64()),
])


def get_file_size_mb(path: Path) -> float:
    """Return file size in MB, or sum of sizes if path is a directory."""
    if path.is_dir():
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / (1024 * 1024)
    return path.stat().st_size / (1024 * 1024)


def write_parquet(df: pd.DataFrame, schema: pa.Schema, output_path: Path, name: str) -> None:
    """Convert a DataFrame to a PyArrow Table with an explicit schema and write Parquet.

    The explicit schema cast is the important part.  Without it, pandas will
    silently infer types that may not match what downstream consumers expect.
    """
    try:
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
        log.error("Schema mismatch for %s: %s", name, exc)
        log.error("Falling back to inferred schema (not recommended for production)")
        table = pa.Table.from_pandas(df, preserve_index=False)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(output_path), compression="snappy")
    log.info("  Wrote %s (%.2f MB)", output_path.name, get_file_size_mb(output_path))


def main() -> None:
    log.info("Converting raw data to Parquet (bronze layer)")
    log.info("Output directory: %s", BRONZE_DIR)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    results = []  # (name, original_mb, parquet_mb)

    # ---- Podcasts ----------------------------------------------------------
    log.info("Converting podcasts.json ...")
    src = RAW_DIR / "podcasts.json"
    with open(src) as f:
        podcasts = pd.DataFrame(json.load(f))
    out = BRONZE_DIR / "podcasts.parquet"
    write_parquet(podcasts, PODCASTS_SCHEMA, out, "podcasts")
    results.append(("podcasts", get_file_size_mb(src), get_file_size_mb(out)))

    # ---- Episodes ----------------------------------------------------------
    log.info("Converting episodes.json ...")
    src = RAW_DIR / "episodes.json"
    with open(src) as f:
        episodes = pd.DataFrame(json.load(f))
    out = BRONZE_DIR / "episodes.parquet"
    write_parquet(episodes, EPISODES_SCHEMA, out, "episodes")
    results.append(("episodes", get_file_size_mb(src), get_file_size_mb(out)))

    # ---- Users -------------------------------------------------------------
    log.info("Converting users.csv ...")
    src = RAW_DIR / "users.csv"
    users = pd.read_csv(src, dtype=str)  # Read everything as string for bronze
    out = BRONZE_DIR / "users.parquet"
    write_parquet(users, USERS_SCHEMA, out, "users")
    results.append(("users", get_file_size_mb(src), get_file_size_mb(out)))

    # ---- Listening Events --------------------------------------------------
    log.info("Converting listening_events (JSONL files) ...")
    events_dir = RAW_DIR / "listening_events"
    jsonl_files = sorted(glob(str(events_dir / "events_*.jsonl")))
    frames = []
    for fpath in jsonl_files:
        try:
            frames.append(pd.read_json(fpath, lines=True))
        except Exception as exc:
            log.warning("Skipping %s: %s", fpath, exc)
    events = pd.concat(frames, ignore_index=True)
    # Convert listened_seconds to int (fill nulls with 0 first)
    events["listened_seconds"] = pd.to_numeric(events["listened_seconds"], errors="coerce").fillna(0).astype(int)
    # Convert timestamp to string for bronze layer (keep raw, parse in silver)
    events["timestamp"] = events["timestamp"].astype(str)
    out = BRONZE_DIR / "listening_events.parquet"
    write_parquet(events, LISTENING_EVENTS_SCHEMA, out, "listening_events")
    results.append(("listening_events", get_file_size_mb(events_dir), get_file_size_mb(out)))

    # ---- CDN Logs ----------------------------------------------------------
    log.info("Converting cdn_logs.csv ...")
    src = RAW_DIR / "cdn_logs.csv"
    cdn = pd.read_csv(src)
    out = BRONZE_DIR / "cdn_logs.parquet"
    write_parquet(cdn, CDN_LOGS_SCHEMA, out, "cdn_logs")
    results.append(("cdn_logs", get_file_size_mb(src), get_file_size_mb(out)))

    # ---- Ad Events ---------------------------------------------------------
    log.info("Converting ad_events.json ...")
    src = RAW_DIR / "ad_events.json"
    with open(src) as f:
        ads = pd.DataFrame(json.load(f))
    out = BRONZE_DIR / "ad_events.parquet"
    write_parquet(ads, AD_EVENTS_SCHEMA, out, "ad_events")
    results.append(("ad_events", get_file_size_mb(src), get_file_size_mb(out)))

    # ---- Summary table -----------------------------------------------------
    log.info("")
    log.info("=" * 60)
    log.info("SIZE COMPARISON: Raw vs Parquet (Snappy)")
    log.info("=" * 60)
    log.info("%-20s  %10s  %10s  %10s", "Dataset", "Raw (MB)", "Parquet", "Ratio")
    log.info("-" * 60)
    for name, raw_mb, pq_mb in results:
        ratio = raw_mb / pq_mb if pq_mb > 0 else float("inf")
        log.info("%-20s  %10.2f  %10.2f  %9.1fx", name, raw_mb, pq_mb, ratio)
    log.info("")
    log.info("Done. Bronze Parquet files written to: %s", BRONZE_DIR)


if __name__ == "__main__":
    main()
