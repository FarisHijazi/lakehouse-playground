"""
Module 06 - Exercise 9: Schema Evolution
==========================================
Demonstrate how the medallion architecture handles a new column appearing
in source data, propagating the change through Bronze -> Silver -> Gold.
"""

import uuid
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"

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
    print("  SCHEMA EVOLUTION DEMONSTRATION")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Show current Bronze schema
    # -----------------------------------------------------------------------
    bronze_path = BRONZE_DIR / "users" / "part-00000.parquet"
    old_schema = pq.read_schema(bronze_path)
    print(f"\n[1] Current Bronze users schema:")
    for i, name in enumerate(old_schema.names):
        print(f"    {name}: {old_schema.field(name).type}")

    old_df = pd.read_parquet(BRONZE_DIR / "users")
    print(f"    Total records: {len(old_df):,}")

    # -----------------------------------------------------------------------
    # 2. Simulate a new batch with an additional column: preferred_language
    # -----------------------------------------------------------------------
    print(f"\n[2] Simulating new user batch with 'preferred_language' column...")

    languages = ["ar", "en", "fr", "ur", "hi", "de", "es"]
    new_batch_size = 50

    new_users = pd.DataFrame({
        "user_id": [f"usr_new_{i:04d}" for i in range(new_batch_size)],
        "name": [f"NewUser_{i}" for i in range(new_batch_size)],
        "email": [f"newuser_{i}@example.com" for i in range(new_batch_size)],
        "country": np.random.choice(["SA", "AE", "EG", "KW"], new_batch_size),
        "city": np.random.choice(["Riyadh", "Dubai", "Cairo", "Kuwait City"], new_batch_size),
        "platform": np.random.choice(["ios", "android", "web"], new_batch_size),
        "signup_date": "2025-06-15",
        "subscription_type": np.random.choice(["free", "premium"], new_batch_size),
        "age": np.random.randint(18, 60, new_batch_size).astype(str),
        "gender": np.random.choice(["male", "female"], new_batch_size),
        "preferred_language": np.random.choice(languages, new_batch_size),
        "_ingested_at": datetime.now().isoformat(),
        "_source_file": "users_v2.csv",
        "_batch_id": str(uuid.uuid4()),
    })

    print(f"    New batch size: {new_batch_size}")
    print(f"    New column: preferred_language")
    print(f"    Sample values: {list(new_users['preferred_language'].head(5))}")

    # -----------------------------------------------------------------------
    # 3. Append to Bronze (schema union)
    # -----------------------------------------------------------------------
    combined = pd.concat([old_df, new_users], ignore_index=True)
    print(f"\n[3] Bronze after schema evolution:")
    print(f"    Total records: {len(combined):,}")
    print(f"    preferred_language nulls: {combined['preferred_language'].isna().sum():,} "
          f"(old records without the column)")

    # Write the evolved Bronze
    evolved_path = BRONZE_DIR / "users" / "part-00000.parquet"
    combined.to_parquet(evolved_path, engine="pyarrow", index=False)

    # Show new schema
    new_schema = pq.read_schema(evolved_path)
    print(f"\n    New Bronze schema:")
    for name in new_schema.names:
        print(f"      {name}: {new_schema.field(name).type}")

    # Schema diff
    old_cols = set(old_schema.names)
    new_cols = set(new_schema.names)
    added = new_cols - old_cols
    removed = old_cols - new_cols
    print(f"\n    Schema diff:")
    print(f"      Added columns:   {added if added else 'none'}")
    print(f"      Removed columns: {removed if removed else 'none'}")

    # -----------------------------------------------------------------------
    # 4. Update Silver with evolved schema
    # -----------------------------------------------------------------------
    print(f"\n[4] Rebuilding Silver with evolved Bronze data...")

    df = combined.copy()

    # Same cleaning as silver_users.py
    df["signup_date"] = pd.to_datetime(df["signup_date"], format="mixed", dayfirst=False, errors="coerce")
    df["gender"] = (
        df["gender"].fillna("").astype(str).str.strip().str.lower()
        .map(GENDER_MAP).fillna("unknown")
    )
    df = df.sort_values("signup_date", na_position="first")
    df = df.drop_duplicates(subset=["user_id"], keep="last")

    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    median_age = df["age"].median()
    df["age"] = df["age"].fillna(median_age).astype(int)
    df["city"] = df["city"].fillna("Unknown")
    df["signup_year"] = df["signup_date"].dt.year.astype("Int64")
    df["age_group"] = pd.cut(df["age"], bins=AGE_BINS, labels=AGE_LABELS, right=True)

    # Handle the new column: fill nulls for old records
    df["preferred_language"] = df["preferred_language"].fillna("unknown")

    # Validate
    valid_mask = df["user_id"].notna() & (df["age"] >= 13) & (df["age"] <= 120)
    df = df[valid_mask].copy()

    keep_cols = [
        "user_id", "name", "email", "country", "city", "platform",
        "signup_date", "subscription_type", "age", "gender",
        "signup_year", "age_group", "preferred_language",
    ]
    df = df[keep_cols].reset_index(drop=True)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_DIR / "users.parquet", engine="pyarrow", index=False)

    print(f"    Silver records: {len(df):,}")
    print(f"    preferred_language distribution:")
    for lang, cnt in df["preferred_language"].value_counts().head(10).items():
        print(f"      {lang}: {cnt:,}")

    # -----------------------------------------------------------------------
    # 5. Show Gold backward compatibility
    # -----------------------------------------------------------------------
    print(f"\n[5] Gold layer backward compatibility check...")
    print(f"    Existing Gold queries that don't use preferred_language")
    print(f"    continue to work without modification.")

    # Example: existing Gold query still works
    age_dist = df.groupby("age_group").agg(
        count=("user_id", "count"),
        pct_premium=("subscription_type", lambda x: (x == "premium").mean()),
    ).round(3)
    print(f"\n    Sample Gold query (age group analysis) -- still works:")
    print(f"    {age_dist.to_string()}")

    # New Gold query using the new column
    lang_prefs = df.groupby("preferred_language").agg(
        users=("user_id", "count"),
        pct=("user_id", lambda x: len(x) / len(df)),
    ).sort_values("users", ascending=False).round(3)
    print(f"\n    New Gold query using preferred_language:")
    print(f"    {lang_prefs.head(10).to_string()}")

    print(f"\n{'='*60}")
    print(f"  SCHEMA EVOLUTION COMPLETE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
