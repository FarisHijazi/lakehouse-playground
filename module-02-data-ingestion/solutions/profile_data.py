"""
Module 02 - Exercise 1: Data Profiling
=======================================

This script profiles every raw data source in the podcast platform dataset.
Profiling is the FIRST thing you should do before writing any pipeline. You need
to understand the shape of the data, its quality issues, and its quirks before
you can clean or transform it.

What we look for:
- Row/column counts and data types
- Null counts and percentages
- Unique value distributions for categorical columns
- Min/max ranges for numeric and date columns
- Duplicates (both full-row and key-based)

Usage:
    python solutions/profile_data.py
"""

import json
import logging
import sys
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
def profile_dataframe(df: pd.DataFrame, name: str, key_column: str | None = None) -> None:
    """Print a comprehensive profile of a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The data to profile.
    name : str
        Human-readable label for log output.
    key_column : str, optional
        If provided, check for duplicate values in this column.
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
        log.info("  %-25s  dtype=%-12s", col, df[col].dtype)

    # -- Null analysis -------------------------------------------------------
    null_counts = df.isnull().sum()
    null_pct = (df.isnull().sum() / len(df) * 100).round(2)
    log.info("Null counts:")
    for col in df.columns:
        if null_counts[col] > 0:
            log.info(
                "  %-25s  nulls=%d  (%.2f%%)", col, null_counts[col], null_pct[col]
            )
    if null_counts.sum() == 0:
        log.info("  (no nulls found)")

    # -- Unique values for low-cardinality columns ---------------------------
    log.info("Unique value counts:")
    for col in df.columns:
        n_unique = df[col].nunique()
        # Only show value distribution for columns with <= 20 unique values.
        # This avoids dumping thousands of user IDs to the console.
        if n_unique <= 20:
            log.info("  %-25s  %d unique values: %s", col, n_unique, dict(df[col].value_counts()))
        else:
            log.info("  %-25s  %d unique values", col, n_unique)

    # -- Sample values -------------------------------------------------------
    log.info("Sample values (first 3 rows):")
    for col in df.columns:
        samples = df[col].dropna().head(3).tolist()
        log.info("  %-25s  %s", col, samples)

    # -- Numeric ranges ------------------------------------------------------
    numeric_cols = df.select_dtypes(include="number").columns
    if len(numeric_cols) > 0:
        log.info("Numeric column ranges:")
        for col in numeric_cols:
            log.info(
                "  %-25s  min=%-12s  max=%-12s  mean=%.2f",
                col,
                df[col].min(),
                df[col].max(),
                df[col].mean(),
            )

    # -- Duplicate detection -------------------------------------------------
    full_dupes = df.duplicated().sum()
    log.info("Full-row duplicates: %d", full_dupes)

    if key_column and key_column in df.columns:
        key_dupes = df.duplicated(subset=[key_column]).sum()
        log.info("Duplicate '%s' values: %d", key_column, key_dupes)

    log.info("")  # blank line between profiles


# ===== Main profiling logic ==================================================
def main() -> None:
    log.info("Starting data profiling against: %s", RAW_DIR)

    # ---- 1. Users (CSV) ----------------------------------------------------
    users_path = RAW_DIR / "users.csv"
    log.info("Reading %s ...", users_path)
    users = pd.read_csv(users_path)
    profile_dataframe(users, "users.csv", key_column="user_id")

    # Specific investigation: date formats in signup_date
    log.info("--- signup_date format analysis ---")
    # Examine a sample of raw signup_date values to identify formats.
    sample_dates = users["signup_date"].dropna().sample(min(20, len(users)), random_state=42)
    for d in sample_dates:
        log.info("  %s", d)

    # Specific investigation: gender value distribution
    log.info("--- gender value distribution ---")
    log.info("  %s", dict(users["gender"].fillna("(null)").value_counts()))

    # ---- 2. Podcasts (JSON) ------------------------------------------------
    podcasts_path = RAW_DIR / "podcasts.json"
    log.info("Reading %s ...", podcasts_path)
    with open(podcasts_path) as f:
        podcasts = pd.DataFrame(json.load(f))
    profile_dataframe(podcasts, "podcasts.json", key_column="podcast_id")

    # ---- 3. Episodes (JSON) ------------------------------------------------
    episodes_path = RAW_DIR / "episodes.json"
    log.info("Reading %s ...", episodes_path)
    with open(episodes_path) as f:
        episodes = pd.DataFrame(json.load(f))
    profile_dataframe(episodes, "episodes.json", key_column="episode_id")

    # ---- 4. Listening Events (JSONL files) ---------------------------------
    events_dir = RAW_DIR / "listening_events"
    jsonl_files = sorted(glob(str(events_dir / "events_*.jsonl")))
    log.info("Reading %d JSONL files from %s ...", len(jsonl_files), events_dir)

    # Read all JSONL files and concatenate into one DataFrame.
    # In production you might use chunked reading for very large datasets,
    # but ~200k rows fits comfortably in memory.
    frames = []
    for fpath in jsonl_files:
        try:
            chunk = pd.read_json(fpath, lines=True)
            frames.append(chunk)
        except Exception as exc:
            log.warning("Failed to read %s: %s", fpath, exc)

    events = pd.concat(frames, ignore_index=True)
    profile_dataframe(events, "listening_events (all files)", key_column="event_id")

    # Date range of listening events
    events["timestamp"] = pd.to_datetime(events["timestamp"], errors="coerce")
    log.info(
        "Listening events date range: %s  to  %s",
        events["timestamp"].min(),
        events["timestamp"].max(),
    )

    # ---- 5. CDN Logs (CSV) -------------------------------------------------
    cdn_path = RAW_DIR / "cdn_logs.csv"
    log.info("Reading %s ...", cdn_path)
    cdn = pd.read_csv(cdn_path)
    profile_dataframe(cdn, "cdn_logs.csv", key_column="log_id")

    # ---- 6. Ad Events (JSON) -----------------------------------------------
    ads_path = RAW_DIR / "ad_events.json"
    log.info("Reading %s ...", ads_path)
    with open(ads_path) as f:
        ads = pd.DataFrame(json.load(f))
    profile_dataframe(ads, "ad_events.json", key_column="ad_event_id")

    # ---- Summary -----------------------------------------------------------
    log.info("=" * 70)
    log.info("PROFILING COMPLETE")
    log.info("=" * 70)
    log.info("Key findings to investigate:")
    log.info("  1. users.csv has mixed date formats in signup_date")
    log.info("  2. users.csv has inconsistent gender values")
    log.info("  3. users.csv has null city/age values")
    log.info("  4. listening_events have duplicate event_id values")
    log.info("  5. All data sources should be converted to Parquet for efficiency")


if __name__ == "__main__":
    main()
