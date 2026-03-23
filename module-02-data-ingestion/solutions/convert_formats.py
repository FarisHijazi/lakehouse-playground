"""
Module 02 - Exercise 2: Convert Taxi Data Formats
===================================================

This script demonstrates format conversion and tradeoffs using real NYC taxi data.

The TLC already distributes trip data as Parquet (they switched from CSV in 2022).
This is a great case study: we convert Parquet to CSV and JSON to see WHY the TLC
chose Parquet, and we also convert the small CSV dimension tables to Parquet for
consistency in our lakehouse.

Key design decisions:
- We define explicit PyArrow schemas instead of relying on pandas type inference.
  Inference is fragile -- a column of "1, 2, 3, NA" might be inferred as float
  when you intended int.  Explicit schemas catch type mismatches early.
- We write to data/processed/bronze/ because this is a raw-to-bronze conversion
  (no cleaning yet, just format change and schema enforcement).

Usage:
    python solutions/convert_formats.py
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
BRONZE_DIR = PROJECT_ROOT / "data" / "processed" / "bronze"


# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------
# Defining schemas explicitly is a best practice.  It documents the expected
# shape of your data and causes a loud failure if the source data changes in
# an unexpected way (e.g., a column is renamed or a new type appears).

YELLOW_TAXI_SCHEMA = pa.schema([
    pa.field("VendorID", pa.int64()),
    pa.field("tpep_pickup_datetime", pa.timestamp("us")),
    pa.field("tpep_dropoff_datetime", pa.timestamp("us")),
    pa.field("passenger_count", pa.float64()),     # float because source has NaN
    pa.field("trip_distance", pa.float64()),
    pa.field("RatecodeID", pa.float64()),           # float because source has NaN
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

TAXI_ZONES_SCHEMA = pa.schema([
    pa.field("LocationID", pa.int32()),
    pa.field("Borough", pa.string()),
    pa.field("Zone", pa.string()),
    pa.field("service_zone", pa.string()),
])

VENDORS_SCHEMA = pa.schema([
    pa.field("vendor_id", pa.int32()),
    pa.field("vendor_name", pa.string()),
])

RATE_CODES_SCHEMA = pa.schema([
    pa.field("rate_code_id", pa.int32()),
    pa.field("rate_code_name", pa.string()),
])

PAYMENT_TYPES_SCHEMA = pa.schema([
    pa.field("payment_type_id", pa.int32()),
    pa.field("payment_type_name", pa.string()),
])

WEATHER_SCHEMA = pa.schema([
    pa.field("date", pa.string()),             # Keep as string in bronze; parse in silver
    pa.field("temp_max_f", pa.float64()),
    pa.field("temp_min_f", pa.float64()),
    pa.field("temp_avg_f", pa.float64()),
    pa.field("precipitation_in", pa.float64()),
    pa.field("snowfall_in", pa.float64()),
    pa.field("snow_depth_in", pa.float64()),
    pa.field("wind_speed_mph", pa.float64()),
])


def get_file_size_mb(path: Path) -> float:
    """Return file size in MB, or sum of sizes if path is a directory."""
    if path.is_dir():
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / (1024 * 1024)
    return path.stat().st_size / (1024 * 1024)


def write_parquet(df: pd.DataFrame, schema: pa.Schema, output_path: Path, name: str) -> None:
    """Convert a DataFrame to a PyArrow Table with an explicit schema and write Parquet."""
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
    log.info("Converting raw NYC taxi data to bronze layer")
    log.info("Output directory: %s", BRONZE_DIR)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    results = []  # (name, original_mb, parquet_mb)

    # ---- Yellow Taxi Trips -------------------------------------------------
    # The TLC distributes trip data as Parquet already. We re-write with an
    # explicit schema to catch any schema drift, and also convert to CSV/JSON
    # to demonstrate the size difference.
    yellow_files = sorted(glob(str(RAW_DIR / "yellow_tripdata_*.parquet")))
    if yellow_files:
        # Process the first file for format comparison demo
        src_path = Path(yellow_files[0])
        log.info("Converting %s ...", src_path.name)
        df = pd.read_parquet(src_path)
        log.info("  Loaded %d rows, %d columns", len(df), len(df.columns))

        # Limit to a sample for CSV/JSON to avoid massive files
        sample_size = min(100_000, len(df))
        df_sample = df.head(sample_size).copy()
        log.info("  Using %d rows for format comparison", sample_size)

        # Write as Parquet (with explicit schema)
        out_pq = BRONZE_DIR / src_path.name
        write_parquet(df, YELLOW_TAXI_SCHEMA, out_pq, "yellow_taxi (parquet)")
        results.append(("yellow_taxi.parquet", get_file_size_mb(src_path), get_file_size_mb(out_pq)))

        # Write sample as CSV to show size difference
        out_csv = BRONZE_DIR / src_path.name.replace(".parquet", ".csv")
        # Convert timestamps to string for CSV
        df_csv = df_sample.copy()
        for col in df_csv.select_dtypes(include=["datetime", "datetimetz"]).columns:
            df_csv[col] = df_csv[col].astype(str)
        df_csv.to_csv(out_csv, index=False)
        log.info("  Wrote %s (%.2f MB) [%d rows]", out_csv.name, get_file_size_mb(out_csv), sample_size)
        results.append((f"yellow_taxi.csv ({sample_size:,} rows)", get_file_size_mb(out_csv), None))

        # Write sample as JSON (line-delimited) to show size difference
        out_json = BRONZE_DIR / src_path.name.replace(".parquet", ".jsonl")
        df_csv.to_json(out_json, orient="records", lines=True)
        log.info("  Wrote %s (%.2f MB) [%d rows]", out_json.name, get_file_size_mb(out_json), sample_size)
        results.append((f"yellow_taxi.jsonl ({sample_size:,} rows)", get_file_size_mb(out_json), None))

        # Process remaining yellow files (Parquet only)
        for fpath in yellow_files[1:]:
            src = Path(fpath)
            log.info("Converting %s ...", src.name)
            df2 = pd.read_parquet(src)
            out2 = BRONZE_DIR / src.name
            write_parquet(df2, YELLOW_TAXI_SCHEMA, out2, src.name)
            results.append((src.name, get_file_size_mb(src), get_file_size_mb(out2)))
    else:
        log.warning("No yellow taxi Parquet files found in %s", RAW_DIR)

    # ---- Green Taxi Trips --------------------------------------------------
    green_files = sorted(glob(str(RAW_DIR / "green_tripdata_*.parquet")))
    for fpath in green_files:
        src = Path(fpath)
        log.info("Converting %s ...", src.name)
        df = pd.read_parquet(src)
        out = BRONZE_DIR / src.name
        # Green taxi has a slightly different schema; use inferred for bronze
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, str(out), compression="snappy")
        log.info("  Wrote %s (%.2f MB)", out.name, get_file_size_mb(out))
        results.append((src.name, get_file_size_mb(src), get_file_size_mb(out)))

    # ---- FHV Trips ---------------------------------------------------------
    fhv_files = sorted(glob(str(RAW_DIR / "fhvhv_tripdata_*.parquet")))
    for fpath in fhv_files:
        src = Path(fpath)
        log.info("Converting %s ...", src.name)
        df = pd.read_parquet(src)
        out = BRONZE_DIR / src.name
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, str(out), compression="snappy")
        log.info("  Wrote %s (%.2f MB)", out.name, get_file_size_mb(out))
        results.append((src.name, get_file_size_mb(src), get_file_size_mb(out)))

    # ---- Taxi Zones (CSV -> Parquet) ---------------------------------------
    zones_path = RAW_DIR / "taxi_zone_lookup.csv"
    if zones_path.exists():
        log.info("Converting taxi_zone_lookup.csv ...")
        zones = pd.read_csv(zones_path)
        out = BRONZE_DIR / "taxi_zones.parquet"
        write_parquet(zones, TAXI_ZONES_SCHEMA, out, "taxi_zones")
        results.append(("taxi_zones", get_file_size_mb(zones_path), get_file_size_mb(out)))
    else:
        log.warning("taxi_zone_lookup.csv not found")

    # ---- Vendors (CSV -> Parquet) ------------------------------------------
    vendors_path = RAW_DIR / "vendors.csv"
    if vendors_path.exists():
        log.info("Converting vendors.csv ...")
        vendors = pd.read_csv(vendors_path)
        out = BRONZE_DIR / "vendors.parquet"
        write_parquet(vendors, VENDORS_SCHEMA, out, "vendors")
        results.append(("vendors", get_file_size_mb(vendors_path), get_file_size_mb(out)))

    # ---- Rate Codes (CSV -> Parquet) ---------------------------------------
    rates_path = RAW_DIR / "rate_codes.csv"
    if rates_path.exists():
        log.info("Converting rate_codes.csv ...")
        rates = pd.read_csv(rates_path)
        out = BRONZE_DIR / "rate_codes.parquet"
        write_parquet(rates, RATE_CODES_SCHEMA, out, "rate_codes")
        results.append(("rate_codes", get_file_size_mb(rates_path), get_file_size_mb(out)))

    # ---- Payment Types (CSV -> Parquet) ------------------------------------
    pay_path = RAW_DIR / "payment_types.csv"
    if pay_path.exists():
        log.info("Converting payment_types.csv ...")
        pay = pd.read_csv(pay_path)
        out = BRONZE_DIR / "payment_types.parquet"
        write_parquet(pay, PAYMENT_TYPES_SCHEMA, out, "payment_types")
        results.append(("payment_types", get_file_size_mb(pay_path), get_file_size_mb(out)))

    # ---- Weather (CSV -> Parquet) ------------------------------------------
    weather_files = sorted(glob(str(RAW_DIR / "nyc_weather_*.csv")))
    for fpath in weather_files:
        src = Path(fpath)
        log.info("Converting %s ...", src.name)
        weather = pd.read_csv(src)
        out = BRONZE_DIR / "nyc_weather.parquet"
        write_parquet(weather, WEATHER_SCHEMA, out, "weather")
        results.append(("nyc_weather", get_file_size_mb(src), get_file_size_mb(out)))

    # ---- Summary table -----------------------------------------------------
    log.info("")
    log.info("=" * 65)
    log.info("SIZE COMPARISON: Raw vs Bronze")
    log.info("=" * 65)
    log.info("%-40s  %10s  %10s  %8s", "Dataset", "Raw (MB)", "Bronze", "Ratio")
    log.info("-" * 65)
    for name, raw_mb, bronze_mb in results:
        if bronze_mb is not None and bronze_mb > 0:
            ratio = raw_mb / bronze_mb
            log.info("%-40s  %10.2f  %10.2f  %7.1fx", name, raw_mb, bronze_mb, ratio)
        else:
            log.info("%-40s  %10.2f  %10s  %8s", name, raw_mb, "---", "---")
    log.info("")
    log.info("Done. Bronze files written to: %s", BRONZE_DIR)
    log.info("")
    log.info("Key takeaway: The TLC distributes data as Parquet for good reason.")
    log.info("CSV and JSON versions of the same data are 3-10x larger on disk.")


if __name__ == "__main__":
    main()
