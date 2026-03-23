"""
Module 06 - Exercise 10: MERGE / Upsert Pattern
=================================================
Implement idempotent writes using DuckDB's MERGE, simulating Delta Lake's
MERGE INTO behavior.
"""

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def main() -> None:
    print("=" * 60)
    print("  MERGE / UPSERT PATTERN WITH DUCKDB")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Load initial Silver users as the "target" table
    # -----------------------------------------------------------------------
    users = pd.read_parquet(SILVER_DIR / "users.parquet")
    print(f"\n[1] Initial Silver users loaded: {len(users):,}")

    conn = duckdb.connect()

    # Create target table from Silver users
    conn.execute("CREATE TABLE silver_users AS SELECT * FROM users")
    initial_count = conn.execute("SELECT COUNT(*) FROM silver_users").fetchone()[0]
    print(f"    Target table created: {initial_count:,} rows")

    # -----------------------------------------------------------------------
    # 2. Generate an "incoming batch" with updates + new records
    # -----------------------------------------------------------------------
    print(f"\n[2] Generating incoming batch...")

    # Pick some existing users to "update"
    existing_sample = users.sample(n=min(20, len(users)), random_state=42).copy()
    existing_sample["subscription_type"] = np.where(
        existing_sample["subscription_type"] == "free", "premium", "premium_annual"
    )
    existing_sample["_update_reason"] = "subscription_upgrade"

    # Create some "new" users
    new_users = pd.DataFrame({
        "user_id": [f"usr_merge_{i:04d}" for i in range(10)],
        "name": [f"MergeUser_{i}" for i in range(10)],
        "email": [f"merge_{i}@example.com" for i in range(10)],
        "country": np.random.choice(["SA", "AE", "EG"], 10),
        "city": np.random.choice(["Riyadh", "Dubai", "Cairo"], 10),
        "platform": np.random.choice(["ios", "android", "web"], 10),
        "signup_date": pd.Timestamp("2025-07-01"),
        "subscription_type": "free",
        "age": np.random.randint(18, 50, 10),
        "gender": np.random.choice(["male", "female"], 10),
        "signup_year": 2025,
        "age_group": "25-34",
    })

    # Add preferred_language if it exists in the target
    if "preferred_language" in users.columns:
        existing_sample["preferred_language"] = existing_sample.get("preferred_language", "unknown")
        new_users["preferred_language"] = np.random.choice(["ar", "en"], 10)

    # Combine into incoming batch
    incoming = pd.concat(
        [existing_sample.drop(columns=["_update_reason"], errors="ignore"), new_users],
        ignore_index=True,
    )
    print(f"    Incoming batch: {len(incoming):,} records")
    print(f"      - Updates to existing users: {len(existing_sample)}")
    print(f"      - New users: {len(new_users)}")

    # Register incoming batch
    conn.execute("CREATE TABLE incoming AS SELECT * FROM incoming")

    # -----------------------------------------------------------------------
    # 3. Perform MERGE (upsert)
    # -----------------------------------------------------------------------
    print(f"\n[3] Performing MERGE (upsert)...")

    # Build column list for the UPDATE SET clause dynamically
    columns = [c for c in incoming.columns if c != "user_id"]
    update_set = ", ".join([f"{c} = source.{c}" for c in columns])
    insert_cols = ", ".join(["user_id"] + columns)
    insert_vals = ", ".join([f"source.{c}" for c in ["user_id"] + columns])

    merge_sql = f"""
    MERGE INTO silver_users AS target
    USING incoming AS source
    ON target.user_id = source.user_id
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals})
    """

    conn.execute(merge_sql)

    after_merge = conn.execute("SELECT COUNT(*) FROM silver_users").fetchone()[0]
    print(f"    Before MERGE: {initial_count:,}")
    print(f"    After MERGE:  {after_merge:,}")
    print(f"    New rows inserted: {after_merge - initial_count}")

    # Verify updates happened
    updated_ids = list(existing_sample["user_id"].head(3))
    if updated_ids:
        placeholders = ", ".join([f"'{uid}'" for uid in updated_ids])
        check = conn.execute(
            f"SELECT user_id, subscription_type FROM silver_users WHERE user_id IN ({placeholders})"
        ).fetchdf()
        print(f"\n    Verification (updated records):")
        print(f"    {check.to_string(index=False)}")

    # -----------------------------------------------------------------------
    # 4. Idempotency check: run the same MERGE again
    # -----------------------------------------------------------------------
    print(f"\n[4] Idempotency check: running the same MERGE again...")

    conn.execute(merge_sql)
    after_second = conn.execute("SELECT COUNT(*) FROM silver_users").fetchone()[0]

    print(f"    After 1st MERGE: {after_merge:,}")
    print(f"    After 2nd MERGE: {after_second:,}")

    if after_merge == after_second:
        print(f"    PASS: Row count unchanged -- upsert is idempotent")
    else:
        print(f"    FAIL: Row count changed -- upsert is NOT idempotent")

    # -----------------------------------------------------------------------
    # 5. Audit trail
    # -----------------------------------------------------------------------
    print(f"\n[5] Audit trail:")
    print(f"    Records in target before: {initial_count:,}")
    print(f"    Incoming batch size:      {len(incoming):,}")
    print(f"    Records updated:          {len(existing_sample):,}")
    print(f"    Records inserted:         {len(new_users):,}")
    print(f"    Final record count:       {after_second:,}")

    # -----------------------------------------------------------------------
    # 6. Write final result to parquet
    # -----------------------------------------------------------------------
    result = conn.execute("SELECT * FROM silver_users").fetchdf()
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    result.to_parquet(
        GOLD_DIR / "merge_upsert_demo.parquet", engine="pyarrow", index=False
    )
    print(f"\n[6] Final result written to: {GOLD_DIR / 'merge_upsert_demo.parquet'}")

    conn.close()

    print(f"\n{'='*60}")
    print(f"  MERGE / UPSERT PATTERN COMPLETE")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
