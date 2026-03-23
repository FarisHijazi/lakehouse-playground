#!/usr/bin/env python3
"""
Module 10 -- Exercise 4: User Engagement Analysis
===================================================
Cohort analysis, listening frequency, activity heatmap, platform usage
trends, subscription breakdown, and power-user identification.

Outputs: ../output/user_engagement.html
"""

import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = Path(__file__).parent.parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE events AS
    SELECT * FROM read_json_auto('{DATA_DIR}/listening_events/*.jsonl')
""")
con.execute(f"""
    CREATE TABLE users AS
    SELECT * FROM read_csv_auto('{DATA_DIR}/users.csv')
""")

# ---------------------------------------------------------------------------
# 1. Signup cohorts by month
# ---------------------------------------------------------------------------
cohort_df = con.execute("""
    SELECT DATE_TRUNC('month', TRY_CAST(signup_date AS DATE)) AS cohort_month,
           COUNT(*) AS signups
    FROM users
    WHERE TRY_CAST(signup_date AS DATE) IS NOT NULL
    GROUP BY 1
    ORDER BY 1
""").df()

# ---------------------------------------------------------------------------
# 2. Listening frequency distribution (events per user)
# ---------------------------------------------------------------------------
freq_df = con.execute("""
    SELECT events_count, COUNT(*) AS users
    FROM (
        SELECT user_id, COUNT(*) AS events_count
        FROM events
        GROUP BY 1
    )
    GROUP BY 1
    ORDER BY 1
""").df()
# Bin into buckets for readability
freq_df["bucket"] = pd.cut(
    freq_df["events_count"],
    bins=[0, 1, 5, 10, 25, 50, 100, 500, float("inf")],
    labels=["1", "2-5", "6-10", "11-25", "26-50", "51-100", "101-500", "500+"],
)
freq_bucketed = freq_df.groupby("bucket", observed=True)["users"].sum().reset_index()

# ---------------------------------------------------------------------------
# 3. Activity heatmap (day-of-week x hour-of-day)
# ---------------------------------------------------------------------------
heatmap_df = con.execute("""
    SELECT
        DAYOFWEEK(CAST(timestamp AS TIMESTAMP)) AS dow,
        HOUR(CAST(timestamp AS TIMESTAMP)) AS hour,
        COUNT(*) AS events
    FROM events
    GROUP BY 1, 2
    ORDER BY 1, 2
""").df()

dow_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
# Pivot for heatmap
heatmap_pivot = heatmap_df.pivot(index="dow", columns="hour", values="events").fillna(0)

# ---------------------------------------------------------------------------
# 4. Platform usage over time
# ---------------------------------------------------------------------------
platform_monthly = con.execute("""
    SELECT DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) AS month,
           platform,
           COUNT(*) AS events
    FROM events
    GROUP BY 1, 2
    ORDER BY 1
""").df()

# ---------------------------------------------------------------------------
# 5. Subscription type breakdown
# ---------------------------------------------------------------------------
sub_df = con.execute("""
    SELECT subscription_type, COUNT(*) AS users
    FROM users
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ---------------------------------------------------------------------------
# 6. Power users -- top 20 by total listened minutes
# ---------------------------------------------------------------------------
power_users = con.execute("""
    SELECT e.user_id,
           COALESCE(u.name, e.user_id) AS user_name,
           u.country,
           u.subscription_type,
           COUNT(*) AS total_events,
           ROUND(SUM(e.listened_seconds) / 60.0, 1) AS total_minutes
    FROM events e
    LEFT JOIN users u ON e.user_id = u.user_id
    GROUP BY 1, 2, 3, 4
    ORDER BY 6 DESC
    LIMIT 20
""").df()

# ---------------------------------------------------------------------------
# Build dashboard
# ---------------------------------------------------------------------------
fig = make_subplots(
    rows=3, cols=2,
    subplot_titles=(
        "Monthly Signup Cohorts",
        "Listening Frequency Distribution",
        "Activity Heatmap (DoW x Hour)",
        "Platform Usage Over Time",
        "Subscription Type Breakdown",
        "Top 20 Power Users (listened minutes)",
    ),
    specs=[
        [{"type": "xy"}, {"type": "xy"}],
        [{"type": "heatmap"}, {"type": "xy"}],
        [{"type": "domain"}, {"type": "xy"}],
    ],
    row_heights=[0.30, 0.40, 0.30],
    vertical_spacing=0.10,
    horizontal_spacing=0.12,
)

# 1 -- Signup cohorts
fig.add_trace(go.Bar(
    x=cohort_df["cohort_month"],
    y=cohort_df["signups"],
    marker_color="#636EFA",
    showlegend=False,
), row=1, col=1)

# 2 -- Frequency distribution
fig.add_trace(go.Bar(
    x=freq_bucketed["bucket"].astype(str),
    y=freq_bucketed["users"],
    marker_color="#EF553B",
    showlegend=False,
), row=1, col=2)

# 3 -- Heatmap
fig.add_trace(go.Heatmap(
    z=heatmap_pivot.values.tolist(),
    x=[str(h) for h in range(24)],
    y=dow_labels[: len(heatmap_pivot)],
    colorscale="YlOrRd",
    showscale=True,
    name="Events",
), row=2, col=1)

# 4 -- Platform usage stacked area
platforms = platform_monthly["platform"].unique()
palette = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
           "#19D3F3", "#FF6692", "#B6E880"]
for i, plat in enumerate(platforms):
    subset = platform_monthly[platform_monthly["platform"] == plat]
    fig.add_trace(go.Scatter(
        x=subset["month"], y=subset["events"],
        mode="lines", stackgroup="one",
        name=plat,
        line={"color": palette[i % len(palette)]},
    ), row=2, col=2)

# 5 -- Subscription breakdown pie
fig.add_trace(go.Pie(
    labels=sub_df["subscription_type"],
    values=sub_df["users"],
    hole=0.4,
    name="Subscription",
), row=3, col=1)

# 6 -- Power users
fig.add_trace(go.Bar(
    y=power_users["user_name"],
    x=power_users["total_minutes"],
    orientation="h",
    marker_color="#00CC96",
    showlegend=False,
), row=3, col=2)
fig.update_yaxes(autorange="reversed", row=3, col=2)

fig.update_layout(
    title={"text": "User Engagement Analysis", "font": {"size": 24}, "x": 0.5},
    height=1200,
    template="plotly_white",
    margin={"t": 80, "b": 40},
)

output_path = OUTPUT_DIR / "user_engagement.html"
fig.write_html(str(output_path), include_plotlyjs="cdn")

# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------
total_users = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
total_events = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
unique_listeners = con.execute("SELECT COUNT(DISTINCT user_id) FROM events").fetchone()[0]

print("=" * 65)
print("  USER ENGAGEMENT -- Summary Statistics")
print("=" * 65)
print(f"  Total registered users   : {total_users:,}")
print(f"  Total listening events   : {total_events:,}")
print(f"  Unique listeners         : {unique_listeners:,}")
print(f"  Avg events per listener  : {total_events / max(unique_listeners, 1):.1f}")
print()
print("  Subscription Breakdown:")
for _, row in sub_df.iterrows():
    print(f"    {row['subscription_type']:15s} {row['users']:>6,} users")
print()
print("  Top 10 Power Users:")
print(f"  {'User':20s} {'Country':>8s} {'Events':>7s} {'Minutes':>9s}")
print("  " + "-" * 48)
for _, row in power_users.head(10).iterrows():
    print(f"  {str(row['user_name']):20s} {str(row['country']):>8s}"
          f" {row['total_events']:>7,} {row['total_minutes']:>9.1f}")
print("=" * 65)
print(f"\n  Dashboard saved to: {output_path}")
