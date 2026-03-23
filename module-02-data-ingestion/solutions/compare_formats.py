"""
Module 02 - Exercise 6: Compare File Sizes and Read Performance
================================================================

This script writes the same data in multiple formats and measures:
- File size on disk
- Write time
- Full-scan read time
- Filtered read time (single column + predicate)

The goal is to build intuition for why Parquet is the default choice for
analytics and when you might choose a different format or compression.

Expected insights:
- Parquet is 5-10x smaller than CSV due to columnar compression.
- Parquet reads are faster for analytical queries because of column pruning
  (only the columns you need are read from disk).
- Snappy compression is the best default: fast to decompress with good compression.
- Zstd gives better compression than Snappy with only slightly slower decompression.
- Gzip gives the best compression but is noticeably slower.

Usage:
    python solutions/compare_formats.py
"""

import logging
import os
import shutil
import time
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
BENCHMARK_DIR = PROJECT_ROOT / "data" / "processed" / "benchmarks"
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def load_events() -> pd.DataFrame:
    """Load listening events for benchmarking."""
    deduped_path = SILVER_DIR / "listening_events_deduped.parquet"
    if deduped_path.exists():
        log.info("Loading from deduplicated Parquet ...")
        return pd.read_parquet(deduped_path)

    log.info("Deduplicated file not found, loading from raw JSONL ...")
    from glob import glob
    events_dir = RAW_DIR / "listening_events"
    jsonl_files = sorted(glob(str(events_dir / "events_*.jsonl")))
    frames = [pd.read_json(f, lines=True) for f in jsonl_files]
    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


def measure_write(df: pd.DataFrame, path: Path, write_fn) -> float:
    """Measure wall-clock write time.  Returns seconds elapsed."""
    start = time.perf_counter()
    write_fn(df, path)
    elapsed = time.perf_counter() - start
    return elapsed


def measure_read(path: Path, read_fn) -> tuple[float, int]:
    """Measure wall-clock read time.  Returns (seconds, row_count)."""
    start = time.perf_counter()
    result = read_fn(path)
    elapsed = time.perf_counter() - start
    return elapsed, len(result)


def file_size_mb(path: Path) -> float:
    """Return file size in MB."""
    return path.stat().st_size / (1024 * 1024)


def main() -> None:
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)

    df = load_events()
    log.info("Loaded %d rows for benchmarking", len(df))

    # Ensure timestamp is string for CSV/JSON (they don't handle datetime natively)
    df_for_text = df.copy()
    if pd.api.types.is_datetime64_any_dtype(df_for_text["timestamp"]):
        df_for_text["timestamp"] = df_for_text["timestamp"].astype(str)

    # ---- Define formats to test --------------------------------------------
    # Each entry: (label, file_extension, write_function, read_function, filtered_read_function)
    formats = [
        (
            "CSV (uncompressed)",
            "csv",
            lambda d, p: d.to_csv(p, index=False),
            lambda p: pd.read_csv(p),
            lambda p: pd.read_csv(p, usecols=["event_type"]).query("event_type == 'complete'"),
        ),
        (
            "JSON",
            "json",
            lambda d, p: d.to_json(p, orient="records", lines=True),
            lambda p: pd.read_json(p, lines=True),
            lambda p: pd.read_json(p, lines=True)[["event_type"]].query("event_type == 'complete'"),
        ),
        (
            "Parquet (no compression)",
            "none.parquet",
            lambda d, p: d.to_parquet(p, compression=None, index=False),
            lambda p: pd.read_parquet(p),
            lambda p: pd.read_parquet(p, columns=["event_type"]).query("event_type == 'complete'"),
        ),
        (
            "Parquet (Snappy)",
            "snappy.parquet",
            lambda d, p: d.to_parquet(p, compression="snappy", index=False),
            lambda p: pd.read_parquet(p),
            lambda p: pd.read_parquet(p, columns=["event_type"]).query("event_type == 'complete'"),
        ),
        (
            "Parquet (Gzip)",
            "gzip.parquet",
            lambda d, p: d.to_parquet(p, compression="gzip", index=False),
            lambda p: pd.read_parquet(p),
            lambda p: pd.read_parquet(p, columns=["event_type"]).query("event_type == 'complete'"),
        ),
        (
            "Parquet (Zstd)",
            "zstd.parquet",
            lambda d, p: d.to_parquet(p, compression="zstd", index=False),
            lambda p: pd.read_parquet(p),
            lambda p: pd.read_parquet(p, columns=["event_type"]).query("event_type == 'complete'"),
        ),
    ]

    # ---- Run benchmarks ----------------------------------------------------
    results = []

    for label, ext, write_fn, read_fn, filtered_read_fn in formats:
        path = BENCHMARK_DIR / f"events_benchmark.{ext}"
        log.info("Benchmarking: %s", label)

        # Use text-safe DataFrame for CSV and JSON
        data = df_for_text if ext in ("csv", "json") else df

        # Write
        write_time = measure_write(data, path, write_fn)
        size_mb = file_size_mb(path)
        log.info("  Write: %.3fs, Size: %.2f MB", write_time, size_mb)

        # Full read
        read_time, row_count = measure_read(path, read_fn)
        log.info("  Full read: %.3fs (%d rows)", read_time, row_count)

        # Filtered read (column pruning + predicate)
        filtered_time, filtered_count = measure_read(path, filtered_read_fn)
        log.info("  Filtered read: %.3fs (%d rows)", filtered_time, filtered_count)

        results.append({
            "format": label,
            "size_mb": round(size_mb, 2),
            "write_sec": round(write_time, 3),
            "full_read_sec": round(read_time, 3),
            "filtered_read_sec": round(filtered_time, 3),
        })

    # ---- Print comparison table --------------------------------------------
    log.info("")
    log.info("=" * 90)
    log.info("BENCHMARK RESULTS")
    log.info("=" * 90)
    log.info(
        "%-25s  %10s  %10s  %12s  %14s",
        "Format", "Size (MB)", "Write (s)", "Full Read (s)", "Filtered (s)",
    )
    log.info("-" * 90)

    for r in results:
        log.info(
            "%-25s  %10.2f  %10.3f  %12.3f  %14.3f",
            r["format"],
            r["size_mb"],
            r["write_sec"],
            r["full_read_sec"],
            r["filtered_read_sec"],
        )

    # ---- Relative comparisons -----------------------------------------------
    csv_result = results[0]
    log.info("")
    log.info("Relative to CSV:")
    for r in results[1:]:
        size_ratio = csv_result["size_mb"] / r["size_mb"] if r["size_mb"] > 0 else float("inf")
        read_ratio = csv_result["full_read_sec"] / r["full_read_sec"] if r["full_read_sec"] > 0 else float("inf")
        log.info(
            "  %-25s  %.1fx smaller, %.1fx faster to read",
            r["format"], size_ratio, read_ratio,
        )

    # ---- Cleanup benchmark files -------------------------------------------
    log.info("")
    log.info("Benchmark files saved in: %s", BENCHMARK_DIR)
    log.info("(You can delete them to reclaim disk space)")


if __name__ == "__main__":
    main()
