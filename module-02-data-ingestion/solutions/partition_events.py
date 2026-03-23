"""
Module 02 - Exercise 5: Partition Taxi Trips by Date and Borough
=================================================================

Partitioning organizes data files into a directory tree based on column values.
When a query filters on the partition column, the query engine can skip entire
directories (partition pruning), dramatically reducing I/O.

For NYC taxi data, the natural partition scheme is:
  - pickup_borough: Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR, Unknown
  - pickup_month: 2023-01, 2023-02, etc.

This gives a manageable number of partitions (~7 boroughs x N months) with
meaningful data in each partition (Manhattan alone accounts for ~70% of trips).

This script also demonstrates a common ingestion pattern: enriching trip data
by joining with a dimension table (taxi zones) at ingestion time, so downstream
consumers get borough names without having to repeat the join.

Usage:
    python solutions/partition_events.py
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


def load_taxi_zones() -> pd.DataFrame:
    """Load the taxi zone lookup table for borough enrichment."""
    zones_path = RAW_DIR / "taxi_zone_lookup.csv"
    if not zones_path.exists():
        log.warning("taxi_zone_lookup.csv not found at %s", zones_path)
        log.warning("Borough enrichment will be skipped (all trips -> 'Unknown')")
        return None

    zones = pd.read_csv(zones_path)
    log.info("Loaded %d taxi zones", len(zones))
    log.info("  Borough distribution: %s", dict(zones["Borough"].value_counts()))
    return zones


def load_trips() -> pd.DataFrame:
    """Load yellow taxi trip data.

    Tries the cleaned/deduped version first, then falls back to raw data.
    Each exercise should be runnable independently.
    """
    # Try cleaned data first
    clean_path = SILVER_DIR / "yellow_trips_clean.parquet"
    if clean_path.exists():
        log.info("Loading cleaned trips from: %s", clean_path)
        return pd.read_parquet(clean_path)

    # Try deduplicated data
    deduped_path = SILVER_DIR / "yellow_trips_deduped.parquet"
    if deduped_path.exists():
        log.info("Loading deduplicated trips from: %s", deduped_path)
        return pd.read_parquet(deduped_path)

    # Fall back to raw data
    log.warning("No cleaned/deduped data found. Loading raw Parquet files.")
    yellow_files = sorted(glob(str(RAW_DIR / "yellow_tripdata_*.parquet")))
    if not yellow_files:
        raise FileNotFoundError(f"No yellow taxi Parquet files found in {RAW_DIR}")

    frames = [pd.read_parquet(f) for f in yellow_files]
    df = pd.concat(frames, ignore_index=True)
    log.info("Loaded %d raw trips from %d files", len(df), len(yellow_files))
    return df


def main() -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = SILVER_DIR / "yellow_trips_partitioned"

    # Clean up previous output to ensure idempotency
    if output_dir.exists():
        import shutil
        shutil.rmtree(output_dir)
        log.info("Removed previous output at %s", output_dir)

    # ---- Load data ---------------------------------------------------------
    df = load_trips()
    log.info("Loaded %d trips", len(df))

    # ---- Load and join taxi zones ------------------------------------------
    zones = load_taxi_zones()
    if zones is not None:
        # Join on pickup location ID to get borough name
        # The zone table uses "LocationID"; the trip data uses "PULocationID"
        zone_lookup = zones[["LocationID", "Borough"]].rename(
            columns={"LocationID": "PULocationID", "Borough": "pickup_borough"}
        )
        df = df.merge(zone_lookup, on="PULocationID", how="left")
        df["pickup_borough"] = df["pickup_borough"].fillna("Unknown")
        log.info("Enriched trips with pickup borough from zone lookup")
    else:
        df["pickup_borough"] = "Unknown"

    # ---- Create partition columns ------------------------------------------
    # Ensure pickup datetime column exists and is datetime type
    if "tpep_pickup_datetime" not in df.columns:
        raise ValueError("Expected column 'tpep_pickup_datetime' not found")

    if not pd.api.types.is_datetime64_any_dtype(df["tpep_pickup_datetime"]):
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")

    # Drop rows with null pickup datetime (cannot partition on null)
    n_null_dt = df["tpep_pickup_datetime"].isna().sum()
    if n_null_dt > 0:
        log.warning("Dropping %d rows with null tpep_pickup_datetime", n_null_dt)
        df = df.dropna(subset=["tpep_pickup_datetime"])

    # Create pickup_month as YYYY-MM string for readable partition names
    df["pickup_month"] = df["tpep_pickup_datetime"].dt.to_period("M").astype(str)

    # ---- Report partition distribution -------------------------------------
    log.info("Partition distribution:")
    partition_counts = (
        df.groupby(["pickup_borough", "pickup_month"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    for _, row in partition_counts.head(20).iterrows():
        log.info("  borough=%-15s / month=%s : %d records",
                 row["pickup_borough"], row["pickup_month"], row["count"])
    if len(partition_counts) > 20:
        log.info("  ... and %d more partitions", len(partition_counts) - 20)
    log.info("Total partitions: %d", len(partition_counts))

    # ---- Write partitioned Parquet -----------------------------------------
    # PyArrow's write_to_dataset handles directory creation and file naming.
    table = pa.Table.from_pandas(df, preserve_index=False)

    pq.write_to_dataset(
        table,
        root_path=str(output_dir),
        partition_cols=["pickup_borough", "pickup_month"],
        compression="snappy",
    )

    # ---- Report results ----------------------------------------------------
    parquet_files = list(output_dir.rglob("*.parquet"))
    total_size_mb = sum(f.stat().st_size for f in parquet_files) / (1024 * 1024)

    log.info("Wrote %d Parquet files to: %s", len(parquet_files), output_dir)
    log.info("Total size: %.2f MB", total_size_mb)

    # Verify we can read the partitioned dataset back
    log.info("Verification: reading partitioned dataset back ...")
    readback = pq.read_table(str(output_dir))
    log.info("  Read back %d rows, %d columns", readback.num_rows, readback.num_columns)

    # Demonstrate partition pruning: read only Manhattan trips
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(output_dir), format="parquet", partitioning="hive")
    filtered = dataset.to_table(filter=ds.field("pickup_borough") == "Manhattan")
    log.info("  Filtered read (pickup_borough=Manhattan): %d rows", filtered.num_rows)
    log.info("  (Only files in pickup_borough=Manhattan/ were read -- partition pruning)")

    # Borough summary
    borough_counts = df["pickup_borough"].value_counts()
    log.info("")
    log.info("Borough distribution:")
    for borough, count in borough_counts.items():
        pct = count / len(df) * 100
        log.info("  %-20s  %d trips (%.1f%%)", borough, count, pct)


if __name__ == "__main__":
    main()
