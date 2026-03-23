"""
Module 06 - Exercise 4: Silver Layer -- CDN Logs
==================================================
Clean CDN streaming logs: fix negatives, validate, enrich with ISP tiers.
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
# ISP tier mapping
# ---------------------------------------------------------------------------
ISP_TIERS = {
    "STC": "tier_1",
    "Mobily": "tier_1",
    "Zain": "tier_1",
    "Etisalat": "tier_2",
    "Ooredoo": "tier_2",
    "du": "tier_2",
    "Batelco": "tier_2",
}


def main() -> None:
    print("=" * 60)
    print("  SILVER LAYER: CDN LOGS")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Read Bronze
    # -----------------------------------------------------------------------
    df = pd.read_parquet(BRONZE_DIR / "cdn_logs")
    bronze_count = len(df)
    print(f"\n[1] Bronze records loaded: {bronze_count:,}")

    # -----------------------------------------------------------------------
    # 2. Type casting
    # -----------------------------------------------------------------------
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    for col in ["startup_time_ms", "buffer_events", "rebuffer_ratio", "bytes_transferred"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # -----------------------------------------------------------------------
    # 3. Fix negative startup_time_ms
    # -----------------------------------------------------------------------
    neg_mask = df["startup_time_ms"] < 0
    neg_count = neg_mask.sum()
    median_startup = df.loc[~neg_mask, "startup_time_ms"].median()
    df.loc[neg_mask, "startup_time_ms"] = median_startup
    print(f"\n[3] Negative startup_time_ms: {neg_count:,} fixed (replaced with median {median_startup:.0f})")

    # -----------------------------------------------------------------------
    # 4. Validate & quarantine
    # -----------------------------------------------------------------------
    invalid_mask = pd.Series(False, index=df.index)

    bad_bytes = df["bytes_transferred"] <= 0
    invalid_mask |= bad_bytes

    bad_rebuffer = (df["rebuffer_ratio"] < 0) | (df["rebuffer_ratio"] > 1)
    invalid_mask |= bad_rebuffer

    bad_ts = df["timestamp"].isna()
    invalid_mask |= bad_ts

    print(f"\n[4] Validation:")
    print(f"    bytes_transferred <= 0: {bad_bytes.sum():,}")
    print(f"    rebuffer_ratio out of [0,1]: {bad_rebuffer.sum():,}")
    print(f"    Invalid timestamp: {bad_ts.sum():,}")

    quarantine = df[invalid_mask].copy()
    df = df[~invalid_mask].copy()
    print(f"    Quarantined: {len(quarantine):,}")

    # -----------------------------------------------------------------------
    # 5. Enrich with ISP tier
    # -----------------------------------------------------------------------
    df["isp_tier"] = df["isp"].map(ISP_TIERS).fillna("tier_3")
    print(f"\n[5] ISP tier distribution:")
    for tier, cnt in df["isp_tier"].value_counts().sort_index().items():
        print(f"    {tier}: {cnt:,}")

    # -----------------------------------------------------------------------
    # 6. Add derived columns
    # -----------------------------------------------------------------------
    df["event_date"] = df["timestamp"].dt.date.astype(str)
    df["cdn_region"] = df["cdn_node"].astype(str).str.split("-").str[1]
    df["has_error"] = df["error_type"].notna() & (df["error_type"].astype(str).str.strip() != "")
    df["quality_score"] = ((1 - df["rebuffer_ratio"]) * 100).round(1)

    print(f"\n[6] Derived columns added:")
    print(f"    CDN region distribution:")
    for region, cnt in df["cdn_region"].value_counts().sort_values(ascending=False).items():
        print(f"      {region}: {cnt:,}")
    print(f"    Average quality score: {df['quality_score'].mean():.1f}")
    print(f"    Records with errors: {df['has_error'].sum():,} ({df['has_error'].mean()*100:.1f}%)")

    # -----------------------------------------------------------------------
    # 7. Write output
    # -----------------------------------------------------------------------
    keep_cols = [
        "log_id", "event_id", "user_id", "timestamp", "event_date",
        "isp", "isp_tier", "bitrate", "buffer_events", "rebuffer_ratio",
        "startup_time_ms", "error_type", "has_error", "cdn_node", "cdn_region",
        "bytes_transferred", "quality_score",
    ]
    df = df[keep_cols].reset_index(drop=True)

    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_DIR / "cdn_logs.parquet", engine="pyarrow", index=False)
    print(f"\n[7] Written to: {SILVER_DIR / 'cdn_logs.parquet'}")

    if len(quarantine) > 0:
        quarantine.to_parquet(
            SILVER_DIR / "cdn_logs_quarantine.parquet", engine="pyarrow", index=False
        )
        print(f"    Quarantine: {SILVER_DIR / 'cdn_logs_quarantine.parquet'}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  SILVER CDN LOGS SUMMARY")
    print(f"{'='*60}")
    print(f"  Bronze input:       {bronze_count:,}")
    print(f"  Negatives fixed:    {neg_count:,}")
    print(f"  Quarantined:        {len(quarantine):,}")
    print(f"  Silver output:      {len(df):,}")


if __name__ == "__main__":
    main()
