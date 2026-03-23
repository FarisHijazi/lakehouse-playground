"""
Module 06 - Exercise 2: Silver Layer -- Users
===============================================
Clean, deduplicate, normalize, and validate the Bronze users table.
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

# ---------------------------------------------------------------------------
# Reference mappings
# ---------------------------------------------------------------------------
GENDER_MAP = {
    "m": "male",
    "male": "male",
    "f": "female",
    "female": "female",
}

AGE_BINS = [12, 17, 24, 34, 44, 54, 64, 120]
AGE_LABELS = ["13-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"]


def main() -> None:
    print("=" * 60)
    print("  SILVER LAYER: USERS")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Read Bronze
    # -----------------------------------------------------------------------
    df = pd.read_parquet(BRONZE_DIR / "users")
    bronze_count = len(df)
    print(f"\n[1] Bronze records loaded: {bronze_count:,}")
    print(f"    Columns: {list(df.columns)}")

    # -----------------------------------------------------------------------
    # 2. Parse signup_date (mixed formats)
    # -----------------------------------------------------------------------
    df["signup_date"] = pd.to_datetime(
        df["signup_date"], format="mixed", dayfirst=False, errors="coerce"
    )
    bad_dates = df["signup_date"].isna().sum()
    print(f"\n[2] Date parsing: {bad_dates} unparseable dates (set to NaT)")

    # -----------------------------------------------------------------------
    # 3. Normalize gender
    # -----------------------------------------------------------------------
    df["gender"] = (
        df["gender"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .map(GENDER_MAP)
        .fillna("unknown")
    )
    print(f"\n[3] Gender distribution after normalization:")
    for g, cnt in df["gender"].value_counts().items():
        print(f"    {g}: {cnt:,}")

    # -----------------------------------------------------------------------
    # 4. Deduplicate on user_id (keep latest signup_date)
    # -----------------------------------------------------------------------
    before_dedup = len(df)
    df = df.sort_values("signup_date", na_position="first")
    df = df.drop_duplicates(subset=["user_id"], keep="last")
    dupes_removed = before_dedup - len(df)
    print(f"\n[4] Deduplication: {dupes_removed:,} duplicate user_id records removed")

    # -----------------------------------------------------------------------
    # 5. Handle nulls
    # -----------------------------------------------------------------------
    # Age: convert to numeric first, then fill with median
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    median_age = df["age"].median()
    age_nulls = df["age"].isna().sum()
    df["age"] = df["age"].fillna(median_age).astype(int)
    print(f"\n[5] Null handling:")
    print(f"    age: {age_nulls} nulls filled with median ({median_age:.0f})")

    city_nulls = df["city"].isna().sum()
    df["city"] = df["city"].fillna("Unknown")
    print(f"    city: {city_nulls} nulls filled with 'Unknown'")

    email_nulls = df["email"].isna().sum()
    print(f"    email: {email_nulls} nulls left as-is")

    # -----------------------------------------------------------------------
    # 6. Add derived columns
    # -----------------------------------------------------------------------
    df["signup_year"] = df["signup_date"].dt.year.astype("Int64")
    df["age_group"] = pd.cut(
        df["age"], bins=AGE_BINS, labels=AGE_LABELS, right=True
    )
    print(f"\n[6] Derived columns added: signup_year, age_group")
    print(f"    Age group distribution:")
    for grp, cnt in df["age_group"].value_counts().sort_index().items():
        print(f"      {grp}: {cnt:,}")

    # -----------------------------------------------------------------------
    # 7. Validate & quarantine
    # -----------------------------------------------------------------------
    invalid_mask = pd.Series(False, index=df.index)

    # Null user_id
    null_uid = df["user_id"].isna()
    invalid_mask |= null_uid
    print(f"\n[7] Validation:")
    print(f"    Null user_id: {null_uid.sum()}")

    # Age out of range
    bad_age = (df["age"] < 13) | (df["age"] > 120)
    invalid_mask |= bad_age
    print(f"    Age out of range (< 13 or > 120): {bad_age.sum()}")

    quarantine = df[invalid_mask].copy()
    df = df[~invalid_mask].copy()

    print(f"    Quarantined: {len(quarantine):,}")
    print(f"    Valid Silver records: {len(df):,}")

    # -----------------------------------------------------------------------
    # 8. Select final columns & write
    # -----------------------------------------------------------------------
    keep_cols = [
        "user_id", "name", "email", "country", "city", "platform",
        "signup_date", "subscription_type", "age", "gender",
        "signup_year", "age_group",
    ]
    df = df[keep_cols].reset_index(drop=True)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_DIR / "users.parquet", engine="pyarrow", index=False)
    print(f"\n[8] Written to: {SILVER_DIR / 'users.parquet'}")

    if len(quarantine) > 0:
        quarantine.to_parquet(
            SILVER_DIR / "users_quarantine.parquet", engine="pyarrow", index=False
        )
        print(f"    Quarantine written to: {SILVER_DIR / 'users_quarantine.parquet'}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  SILVER USERS SUMMARY")
    print(f"{'='*60}")
    print(f"  Bronze input:      {bronze_count:,}")
    print(f"  Duplicates removed:{dupes_removed:,}")
    print(f"  Quarantined:       {len(quarantine):,}")
    print(f"  Silver output:     {len(df):,}")
    print(f"\n  Sample output (first 5 rows):")
    print(df.head().to_string(index=False, max_colwidth=30))


if __name__ == "__main__":
    main()
