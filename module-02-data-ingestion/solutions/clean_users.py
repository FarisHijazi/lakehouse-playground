"""
Module 02 - Exercise 3: Clean Messy User Data
===============================================

This script tackles the most common real-world data quality issues:
- Mixed date formats in a single column
- Inconsistent categorical values (gender: 'm', 'M', 'male', 'Male', 'f', ...)
- Null/missing values that need different fill strategies per column
- Duplicate records

These problems appear in virtually every production dataset.  The patterns
here -- normalization maps, fallback date parsing, median imputation -- are
reusable across projects.

Usage:
    python solutions/clean_users.py
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
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SILVER_DIR = PROJECT_ROOT / "data" / "processed" / "silver"


# ---------------------------------------------------------------------------
# Gender normalization map
# ---------------------------------------------------------------------------
# In production you would build this map after profiling the data (Exercise 1).
# The key insight is to lowercase first, then map.  This handles 'M', 'Male',
# 'MALE', etc. in one step.
GENDER_MAP = {
    "m": "male",
    "male": "male",
    "f": "female",
    "female": "female",
    # Anything else (including empty string, NaN) maps to "unknown"
}


def parse_mixed_dates(series: pd.Series) -> pd.Series:
    """Parse a Series containing multiple date formats into datetime.

    Strategy:
    1. Try pandas' built-in mixed format parser first -- it handles ISO 8601
       and many common formats automatically.
    2. For anything that fails, try common non-ISO formats one by one.
    3. Anything still unparsed becomes NaT (Not a Time).

    Why not just use format='mixed'?  It works for most cases, but being
    explicit about fallback formats makes the code self-documenting and
    easier to debug when a new format appears.
    """
    # First pass: let pandas try to infer.  dayfirst=False because we have
    # MM-DD-YYYY formats like '03-09-2019' where 03 is month, not day.
    parsed = pd.to_datetime(series, format="mixed", dayfirst=False, errors="coerce")

    # Identify values that failed to parse
    mask_failed = parsed.isna() & series.notna()
    n_failed = mask_failed.sum()

    if n_failed > 0:
        log.warning("%d dates failed initial parsing, trying fallback formats...", n_failed)
        # Fallback: try specific formats for the failures
        fallback_formats = [
            "%d/%m/%Y",       # 23/09/2022 (day-first slash format)
            "%m-%d-%Y",       # 03-09-2019 (US format with dashes)
            "%Y-%m-%dT%H:%M:%S",  # ISO with time
            "%d-%m-%Y",       # 23-09-2022 (day-first dash format)
        ]
        remaining = series[mask_failed]
        for fmt in fallback_formats:
            still_na = parsed[mask_failed].isna()
            if not still_na.any():
                break
            newly_parsed = pd.to_datetime(
                remaining[still_na], format=fmt, errors="coerce"
            )
            parsed.update(newly_parsed)

    final_failures = parsed.isna() & series.notna()
    if final_failures.sum() > 0:
        log.warning(
            "%d dates could not be parsed at all. Sample: %s",
            final_failures.sum(),
            series[final_failures].head(5).tolist(),
        )

    return parsed


def normalize_gender(series: pd.Series) -> pd.Series:
    """Normalize gender values to {male, female, unknown}.

    Steps:
    1. Fill NaN with empty string so .str.lower() doesn't produce NaN.
    2. Lowercase and strip whitespace.
    3. Map through the normalization dictionary.
    4. Anything unmapped becomes 'unknown'.
    """
    cleaned = series.fillna("").str.strip().str.lower()
    normalized = cleaned.map(GENDER_MAP).fillna("unknown")

    # Log the mapping results for auditability
    original_dist = series.fillna("(null)").value_counts()
    log.info("Gender normalization mapping:")
    for orig_val, count in original_dist.items():
        mapped_val = GENDER_MAP.get(str(orig_val).strip().lower(), "unknown")
        log.info("  '%s' (%d records) -> '%s'", orig_val, count, mapped_val)

    return normalized


def main() -> None:
    log.info("Cleaning user data from: %s", RAW_DIR / "users.csv")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # ---- Read raw data -----------------------------------------------------
    df = pd.read_csv(RAW_DIR / "users.csv")
    log.info("Loaded %d rows, %d columns", len(df), len(df.columns))

    # ---- Step 1: Remove full-row duplicates --------------------------------
    n_before = len(df)
    df = df.drop_duplicates()
    n_dupes_full = n_before - len(df)
    log.info("Removed %d full-row duplicates (%d -> %d rows)", n_dupes_full, n_before, len(df))

    # Also check for duplicate user_id values (different data, same key)
    n_before = len(df)
    df = df.drop_duplicates(subset=["user_id"], keep="last")
    n_dupes_key = n_before - len(df)
    log.info("Removed %d duplicate user_id values (%d -> %d rows)", n_dupes_key, n_before, len(df))

    # ---- Step 2: Parse mixed date formats ----------------------------------
    log.info("Parsing mixed date formats in signup_date ...")
    log.info("  Sample raw values: %s", df["signup_date"].head(10).tolist())
    df["signup_date"] = parse_mixed_dates(df["signup_date"])
    log.info(
        "  Date range after parsing: %s to %s",
        df["signup_date"].min(),
        df["signup_date"].max(),
    )
    log.info("  Unparsable dates (NaT): %d", df["signup_date"].isna().sum())

    # ---- Step 3: Normalize gender values -----------------------------------
    log.info("Normalizing gender values ...")
    log.info("  Raw distribution: %s", dict(df["gender"].fillna("(null)").value_counts()))
    df["gender"] = normalize_gender(df["gender"])
    log.info("  Normalized distribution: %s", dict(df["gender"].value_counts()))

    # ---- Step 4: Handle null values ----------------------------------------
    log.info("Handling null values ...")

    # city: fill with "Unknown"
    n_null_city = df["city"].isna().sum()
    df["city"] = df["city"].fillna("Unknown")
    log.info("  Filled %d null city values with 'Unknown'", n_null_city)

    # age: fill with median (a common strategy for numeric columns that avoids
    # skewing the distribution the way mean-filling can with outliers)
    n_null_age = df["age"].isna().sum()
    median_age = df["age"].median()
    df["age"] = df["age"].fillna(median_age).astype(int)
    log.info("  Filled %d null age values with median (%.0f)", n_null_age, median_age)

    # email: fill with a placeholder
    n_null_email = df["email"].isna().sum()
    df["email"] = df["email"].fillna("unknown@placeholder.com")
    log.info("  Filled %d null email values with placeholder", n_null_email)

    # signup_date: if still NaT after parsing, fill with a sentinel date
    n_null_date = df["signup_date"].isna().sum()
    if n_null_date > 0:
        df["signup_date"] = df["signup_date"].fillna(pd.Timestamp("1970-01-01"))
        log.info("  Filled %d unparsable signup_date values with 1970-01-01", n_null_date)

    # ---- Step 5: Validate --------------------------------------------------
    log.info("Running validation checks ...")

    assert df["signup_date"].isna().sum() == 0, "signup_date still has nulls!"
    assert df["gender"].isna().sum() == 0, "gender still has nulls!"
    assert df["city"].isna().sum() == 0, "city still has nulls!"
    assert set(df["gender"].unique()).issubset({"male", "female", "unknown"}), (
        f"Unexpected gender values: {set(df['gender'].unique())}"
    )
    assert df["user_id"].duplicated().sum() == 0, "Duplicate user_id values remain!"

    log.info("All validations passed.")

    # ---- Step 6: Write to Parquet ------------------------------------------
    output_path = SILVER_DIR / "users_clean.parquet"

    # Define a clean schema for the silver layer
    schema = pa.schema([
        pa.field("user_id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("country", pa.string()),
        pa.field("city", pa.string()),
        pa.field("platform", pa.string()),
        pa.field("signup_date", pa.timestamp("us")),
        pa.field("subscription_type", pa.string()),
        pa.field("age", pa.int32()),
        pa.field("gender", pa.string()),
    ])

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, str(output_path), compression="snappy")

    log.info("Wrote cleaned users to: %s", output_path)
    log.info("Final shape: %d rows, %d columns", len(df), len(df.columns))
    log.info("File size: %.2f MB", output_path.stat().st_size / (1024 * 1024))


if __name__ == "__main__":
    main()
