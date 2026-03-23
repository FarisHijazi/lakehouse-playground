"""
Module 06 - Exercise 8: Gold Layer -- Ad Revenue Analytics
===========================================================
Revenue analytics for the monetization team.
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def main() -> None:
    print("=" * 60)
    print("  GOLD LAYER: AD REVENUE ANALYTICS")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Read & clean ad events from Bronze
    # -----------------------------------------------------------------------
    df = pd.read_parquet(BRONZE_DIR / "ad_events")
    print(f"\n[1] Bronze ad events loaded: {len(df):,}")

    # Parse types
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["revenue_sar"] = pd.to_numeric(df["revenue_sar"], errors="coerce")

    # Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["ad_event_id"], keep="first")
    print(f"    Duplicates removed: {before - len(df):,}")

    # Validate revenue
    neg_rev = (df["revenue_sar"] < 0).sum()
    df = df[df["revenue_sar"] >= 0].copy()
    print(f"    Negative revenue records dropped: {neg_rev:,}")

    # Drop invalid timestamps
    df = df.dropna(subset=["timestamp"])

    # Derived
    df["event_date"] = df["timestamp"].dt.date.astype(str)
    df["event_month"] = df["timestamp"].dt.to_period("M").astype(str)

    print(f"    Clean records: {len(df):,}")

    # -----------------------------------------------------------------------
    # 2. Daily revenue metrics by ad_type and advertiser
    # -----------------------------------------------------------------------
    def compute_action_counts(group):
        return pd.Series({
            "total_impressions": (group["action"] == "impression").sum(),
            "total_clicks": (group["action"] == "click").sum(),
            "total_completes": (group["action"] == "complete").sum(),
            "total_skips": (group["action"] == "skip").sum(),
            "total_revenue_sar": group["revenue_sar"].sum(),
            "avg_revenue_per_event": group["revenue_sar"].mean(),
            "total_events": len(group),
        })

    daily = (
        df.groupby(["event_date", "ad_type", "advertiser"])
        .apply(compute_action_counts, include_groups=False)
        .reset_index()
    )

    # CTR and completion rate
    daily["ctr"] = (
        daily["total_clicks"] / daily["total_impressions"].replace(0, float("nan"))
    ).round(4)

    total_actions = (
        daily["total_impressions"] + daily["total_clicks"]
        + daily["total_completes"] + daily["total_skips"]
    )
    daily["completion_rate"] = (
        daily["total_completes"] / total_actions.replace(0, float("nan"))
    ).round(4)

    daily["total_revenue_sar"] = daily["total_revenue_sar"].round(2)
    daily["avg_revenue_per_event"] = daily["avg_revenue_per_event"].round(4)

    daily = daily.sort_values("event_date").reset_index(drop=True)

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    daily.to_parquet(GOLD_DIR / "ad_revenue.parquet", engine="pyarrow", index=False)
    print(f"\n[2] Daily ad revenue written to: {GOLD_DIR / 'ad_revenue.parquet'}")

    # -----------------------------------------------------------------------
    # 3. Monthly summary by advertiser
    # -----------------------------------------------------------------------
    monthly = (
        df.groupby(["event_month", "advertiser"])
        .apply(compute_action_counts, include_groups=False)
        .reset_index()
    )
    monthly["total_revenue_sar"] = monthly["total_revenue_sar"].round(2)
    monthly["avg_revenue_per_event"] = monthly["avg_revenue_per_event"].round(4)
    monthly = monthly.sort_values(["event_month", "advertiser"]).reset_index(drop=True)

    monthly.to_parquet(
        GOLD_DIR / "ad_revenue_monthly.parquet", engine="pyarrow", index=False
    )
    print(f"[3] Monthly ad revenue written to: {GOLD_DIR / 'ad_revenue_monthly.parquet'}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  AD REVENUE ANALYTICS SUMMARY")
    print(f"{'='*60}")
    total_rev = df["revenue_sar"].sum()
    print(f"  Total revenue: {total_rev:,.2f} SAR")

    print(f"\n  Top advertisers by revenue:")
    top_adv = (
        df.groupby("advertiser")["revenue_sar"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    for adv, rev in top_adv.items():
        print(f"    {adv}: {rev:,.2f} SAR")

    print(f"\n  Revenue by ad type:")
    by_type = df.groupby("ad_type")["revenue_sar"].sum().sort_values(ascending=False)
    for at, rev in by_type.items():
        print(f"    {at}: {rev:,.2f} SAR")

    print(f"\n  Monthly revenue trend (first 3 and last 3 months):")
    monthly_trend = (
        df.groupby("event_month")["revenue_sar"]
        .sum()
        .sort_index()
        .round(2)
    )
    for m, rev in list(monthly_trend.items())[:3]:
        print(f"    {m}: {rev:,.2f} SAR")
    print(f"    ...")
    for m, rev in list(monthly_trend.items())[-3:]:
        print(f"    {m}: {rev:,.2f} SAR")


if __name__ == "__main__":
    main()
