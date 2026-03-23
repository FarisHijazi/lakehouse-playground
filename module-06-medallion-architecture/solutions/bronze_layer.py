"""
Module 06 - Exercise 1: Bronze Layer
=====================================
Land all raw data sources into the Bronze layer as Parquet files.
No cleaning, no transformations -- just faithful copies with ingestion metadata.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

BATCH_ID = str(uuid.uuid4())
INGESTED_AT = datetime.now().isoformat()


def add_metadata(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Add standard ingestion metadata columns."""
    df = df.copy()
    df["_ingested_at"] = INGESTED_AT
    df["_source_file"] = source_file
    df["_batch_id"] = BATCH_ID
    return df


def print_summary(name: str, df: pd.DataFrame) -> None:
    """Print summary statistics for a dataset."""
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")
    print(f"  Rows:    {len(df):,}")
    print(f"  Columns: {list(df.columns)}")
    null_counts = df.isnull().sum()
    nulls = null_counts[null_counts > 0]
    if len(nulls) > 0:
        print(f"  Nulls:")
        for col, count in nulls.items():
            if not col.startswith("_"):
                print(f"    {col}: {count:,}")
    print(f"  Sample (first 3 rows):")
    print(df.head(3).to_string(index=False, max_colwidth=40))


# ---------------------------------------------------------------------------
# 1. Podcasts (JSON array)
# ---------------------------------------------------------------------------
def ingest_podcasts() -> None:
    print("\n>>> Ingesting podcasts.json ...")
    df = pd.read_json(RAW_DIR / "podcasts.json", dtype=False)
    df = add_metadata(df, "podcasts.json")
    out_dir = BRONZE_DIR / "podcasts"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "part-00000.parquet", engine="pyarrow", index=False)
    print_summary("Bronze: Podcasts", df)


# ---------------------------------------------------------------------------
# 2. Episodes (JSON array)
# ---------------------------------------------------------------------------
def ingest_episodes() -> None:
    print("\n>>> Ingesting episodes.json ...")
    df = pd.read_json(RAW_DIR / "episodes.json", dtype=False)
    df = add_metadata(df, "episodes.json")
    out_dir = BRONZE_DIR / "episodes"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "part-00000.parquet", engine="pyarrow", index=False)
    print_summary("Bronze: Episodes", df)


# ---------------------------------------------------------------------------
# 3. Users (CSV -- read as strings to avoid type coercion)
# ---------------------------------------------------------------------------
def ingest_users() -> None:
    print("\n>>> Ingesting users.csv ...")
    df = pd.read_csv(RAW_DIR / "users.csv", dtype=str)
    df = add_metadata(df, "users.csv")
    out_dir = BRONZE_DIR / "users"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "part-00000.parquet", engine="pyarrow", index=False)
    print_summary("Bronze: Users", df)


# ---------------------------------------------------------------------------
# 4. Listening Events (daily JSONL files, partitioned by date)
# ---------------------------------------------------------------------------
def ingest_listening_events() -> None:
    print("\n>>> Ingesting listening_events/ (daily JSONL files) ...")
    events_dir = RAW_DIR / "listening_events"
    jsonl_files = sorted(events_dir.glob("events_*.jsonl"))
    print(f"  Found {len(jsonl_files)} daily event files")

    all_dfs = []
    for fpath in jsonl_files:
        # Extract date from filename: events_2024-01-01.jsonl -> 2024-01-01
        date_str = fpath.stem.replace("events_", "")
        df = pd.read_json(fpath, lines=True, dtype=False)
        df = add_metadata(df, fpath.name)
        df["_ingested_date"] = date_str
        all_dfs.append(df)

    combined = pd.concat(all_dfs, ignore_index=True)

    out_dir = BRONZE_DIR / "listening_events"
    out_dir.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(
        out_dir,
        engine="pyarrow",
        index=False,
        partition_cols=["_ingested_date"],
    )
    print_summary("Bronze: Listening Events", combined)
    print(f"  Date range: {combined['_ingested_date'].min()} to {combined['_ingested_date'].max()}")


# ---------------------------------------------------------------------------
# 5. CDN Logs (CSV)
# ---------------------------------------------------------------------------
def ingest_cdn_logs() -> None:
    print("\n>>> Ingesting cdn_logs.csv ...")
    df = pd.read_csv(RAW_DIR / "cdn_logs.csv", dtype=str)
    df = add_metadata(df, "cdn_logs.csv")
    out_dir = BRONZE_DIR / "cdn_logs"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "part-00000.parquet", engine="pyarrow", index=False)
    print_summary("Bronze: CDN Logs", df)


# ---------------------------------------------------------------------------
# 6. Ad Events (JSON array)
# ---------------------------------------------------------------------------
def ingest_ad_events() -> None:
    print("\n>>> Ingesting ad_events.json ...")
    df = pd.read_json(RAW_DIR / "ad_events.json", dtype=False)
    df = add_metadata(df, "ad_events.json")
    out_dir = BRONZE_DIR / "ad_events"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "part-00000.parquet", engine="pyarrow", index=False)
    print_summary("Bronze: Ad Events", df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    print("=" * 60)
    print("  BRONZE LAYER INGESTION")
    print(f"  Batch ID: {BATCH_ID}")
    print(f"  Ingested at: {INGESTED_AT}")
    print("=" * 60)

    ingest_podcasts()
    ingest_episodes()
    ingest_users()
    ingest_listening_events()
    ingest_cdn_logs()
    ingest_ad_events()

    print("\n" + "=" * 60)
    print("  BRONZE LAYER COMPLETE")
    print(f"  Output directory: {BRONZE_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
