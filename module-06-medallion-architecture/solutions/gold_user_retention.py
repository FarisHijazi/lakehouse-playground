"""
Module 06 - Exercise 7: Gold Layer -- User Retention Cohorts
=============================================================
Cohort retention analysis: how well does the platform retain users over time?
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"


def main() -> None:
    print("=" * 60)
    print("  GOLD LAYER: USER RETENTION COHORTS")
    print("=" * 60)

    # -----------------------------------------------------------------------
    # 1. Read Silver data
    # -----------------------------------------------------------------------
    users = pd.read_parquet(SILVER_DIR / "users.parquet")
    events = pd.read_parquet(SILVER_DIR / "listening_events.parquet")

    print(f"\n[1] Data loaded:")
    print(f"    Silver users:  {len(users):,}")
    print(f"    Silver events: {len(events):,}")

    # -----------------------------------------------------------------------
    # 2. Define cohorts by signup month
    # -----------------------------------------------------------------------
    users["signup_date"] = pd.to_datetime(users["signup_date"], errors="coerce")
    users = users.dropna(subset=["signup_date"])
    users["cohort"] = users["signup_date"].dt.to_period("M")

    # -----------------------------------------------------------------------
    # 3. Compute activity months for each event
    # -----------------------------------------------------------------------
    events["event_date"] = pd.to_datetime(events["event_date"], errors="coerce")
    events = events.dropna(subset=["event_date"])
    events["activity_month"] = events["event_date"].dt.to_period("M")

    # Join events with user cohort
    user_cohort = users[["user_id", "cohort"]].drop_duplicates()
    activity = events[["user_id", "activity_month"]].drop_duplicates()
    activity = activity.merge(user_cohort, on="user_id", how="inner")

    # -----------------------------------------------------------------------
    # 4. Compute month_offset
    # -----------------------------------------------------------------------
    # Period subtraction gives the number of months between two periods
    activity["month_offset"] = (
        activity["activity_month"].astype("int64") - activity["cohort"].astype("int64")
    )

    # Filter to offsets 0 through 12
    activity = activity[
        (activity["month_offset"] >= 0) & (activity["month_offset"] <= 12)
    ]

    # -----------------------------------------------------------------------
    # 5. Build cohort retention table
    # -----------------------------------------------------------------------
    # Cohort sizes
    cohort_sizes = users.groupby("cohort")["user_id"].nunique().reset_index()
    cohort_sizes.columns = ["cohort", "cohort_size"]

    # Active users per cohort per month_offset
    retention = (
        activity.groupby(["cohort", "month_offset"])["user_id"]
        .nunique()
        .reset_index()
        .rename(columns={"user_id": "active_users"})
    )

    retention = retention.merge(cohort_sizes, on="cohort", how="left")
    retention["retention_rate"] = (
        retention["active_users"] / retention["cohort_size"]
    ).round(4)

    # Convert cohort to string for Parquet compatibility
    retention["cohort"] = retention["cohort"].astype(str)

    # Sort
    retention = retention.sort_values(["cohort", "month_offset"]).reset_index(drop=True)

    # -----------------------------------------------------------------------
    # 6. Write output
    # -----------------------------------------------------------------------
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    retention.to_parquet(
        GOLD_DIR / "user_retention_cohorts.parquet", engine="pyarrow", index=False
    )
    print(f"\n[6] Written to: {GOLD_DIR / 'user_retention_cohorts.parquet'}")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"  USER RETENTION COHORTS SUMMARY")
    print(f"{'='*60}")
    print(f"  Total cohorts: {retention['cohort'].nunique()}")
    print(f"  Total rows: {len(retention):,}")

    # Cohort sizes
    cs = cohort_sizes.copy()
    cs["cohort"] = cs["cohort"].astype(str)
    print(f"\n  Cohort sizes (top 10 largest):")
    print(cs.sort_values("cohort_size", ascending=False).head(10).to_string(index=False))

    # Month-0 activation rates
    m0 = retention[retention["month_offset"] == 0][["cohort", "retention_rate"]]
    print(f"\n  Month-0 activation rates (sample):")
    print(m0.head(10).to_string(index=False))

    # Month-6 retention rates
    m6 = retention[retention["month_offset"] == 6][["cohort", "retention_rate", "active_users"]]
    if len(m6) > 0:
        print(f"\n  Month-6 retention rates (sample):")
        print(m6.head(10).to_string(index=False))
        best = m6.loc[m6["retention_rate"].idxmax()]
        worst = m6.loc[m6["retention_rate"].idxmin()]
        print(f"\n  Best month-6 retention:  {best['cohort']} ({best['retention_rate']:.2%})")
        print(f"  Worst month-6 retention: {worst['cohort']} ({worst['retention_rate']:.2%})")
    else:
        print("\n  No cohorts have reached month 6 yet.")


if __name__ == "__main__":
    main()
