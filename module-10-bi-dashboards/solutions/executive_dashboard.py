#!/usr/bin/env python3
"""
Module 10 -- Exercise 2: Executive Summary Dashboard
=====================================================
A single-page dashboard giving leadership a quick overview of podcast
platform health: KPI cards, DAU trend, platform breakdown, and category mix.

Outputs: ../output/executive_dashboard.html
"""

import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = Path(__file__).parent.parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Connect & load data
# ---------------------------------------------------------------------------
con = duckdb.connect()

events = con.execute(f"""
    SELECT *
    FROM read_json_auto('{DATA_DIR}/listening_events/*.jsonl')
""").df()

episodes = con.execute(f"""
    SELECT * FROM read_json_auto('{DATA_DIR}/episodes.json')
""").df()

podcasts = con.execute(f"""
    SELECT * FROM read_json_auto('{DATA_DIR}/podcasts.json')
""").df()

ad_events = con.execute(f"""
    SELECT * FROM read_json_auto('{DATA_DIR}/ad_events.json')
""").df()

# Register as tables for convenient SQL
con.register("events", events)
con.register("episodes", episodes)
con.register("podcasts", podcasts)
con.register("ad_events", ad_events)

# ---------------------------------------------------------------------------
# KPI computations
# ---------------------------------------------------------------------------
total_listeners = con.execute(
    "SELECT COUNT(DISTINCT user_id) AS n FROM events"
).fetchone()[0]

avg_listen_min = con.execute(
    "SELECT ROUND(AVG(listened_seconds) / 60.0, 1) FROM events"
).fetchone()[0]

completion_rate = con.execute("""
    SELECT ROUND(
        COUNT(*) FILTER (WHERE event_type = 'complete') * 100.0 /
        NULLIF(COUNT(*) FILTER (WHERE event_type IN ('play','resume','complete')), 0),
    1) FROM events
""").fetchone()[0]

total_ad_revenue = con.execute(
    "SELECT ROUND(SUM(revenue_sar), 2) FROM ad_events"
).fetchone()[0]

# ---------------------------------------------------------------------------
# DAU over time
# ---------------------------------------------------------------------------
dau_df = con.execute("""
    SELECT
        CAST(timestamp AS DATE) AS event_date,
        COUNT(DISTINCT user_id) AS dau
    FROM events
    GROUP BY 1
    ORDER BY 1
""").df()

# ---------------------------------------------------------------------------
# Listens by platform
# ---------------------------------------------------------------------------
platform_df = con.execute("""
    SELECT platform, COUNT(*) AS listens
    FROM events
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ---------------------------------------------------------------------------
# Listens by podcast category
# ---------------------------------------------------------------------------
category_df = con.execute("""
    SELECT p.category, COUNT(*) AS listens
    FROM events e
    JOIN episodes ep ON e.episode_id = ep.episode_id
    JOIN podcasts p  ON ep.podcast_id = p.podcast_id
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ---------------------------------------------------------------------------
# Build dashboard
# ---------------------------------------------------------------------------
fig = make_subplots(
    rows=3, cols=2,
    row_heights=[0.15, 0.45, 0.40],
    specs=[
        [{"type": "indicator"}, {"type": "indicator"}],
        [{"type": "xy", "colspan": 2}, None],
        [{"type": "xy"}, {"type": "domain"}],
    ],
    subplot_titles=("", "", "Daily Active Users", "", "Listens by Platform", "Listens by Category"),
    vertical_spacing=0.10,
    horizontal_spacing=0.10,
)

# KPI cards (row 1)
fig.add_trace(go.Indicator(
    mode="number",
    value=total_listeners,
    title={"text": "Total Unique Listeners"},
    number={"font": {"size": 40}},
), row=1, col=1)

fig.add_trace(go.Indicator(
    mode="number",
    value=avg_listen_min,
    title={"text": "Avg Listen (min)"},
    number={"font": {"size": 40}, "suffix": " min"},
), row=1, col=2)

# DAU trend (row 2)
fig.add_trace(go.Scatter(
    x=dau_df["event_date"],
    y=dau_df["dau"],
    mode="lines",
    fill="tozeroy",
    line={"color": "#636EFA", "width": 2},
    name="DAU",
), row=2, col=1)

# Platform bar chart (row 3, col 1)
fig.add_trace(go.Bar(
    x=platform_df["platform"],
    y=platform_df["listens"],
    marker_color="#EF553B",
    name="Platform",
    showlegend=False,
), row=3, col=1)

# Category pie chart (row 3, col 2)
fig.add_trace(go.Pie(
    labels=category_df["category"],
    values=category_df["listens"],
    hole=0.4,
    name="Category",
), row=3, col=2)

fig.update_layout(
    title={
        "text": "Podcast Platform -- Executive Dashboard",
        "font": {"size": 24},
        "x": 0.5,
    },
    height=900,
    template="plotly_white",
    showlegend=False,
    margin={"t": 80, "b": 40},
)

# Add extra KPI annotations
fig.add_annotation(
    text=f"<b>Completion Rate</b><br>{completion_rate}%",
    xref="paper", yref="paper",
    x=0.62, y=0.95,
    showarrow=False,
    font={"size": 16},
    align="center",
)
fig.add_annotation(
    text=f"<b>Ad Revenue</b><br>{total_ad_revenue:,.0f} SAR",
    xref="paper", yref="paper",
    x=0.88, y=0.95,
    showarrow=False,
    font={"size": 16},
    align="center",
)

output_path = OUTPUT_DIR / "executive_dashboard.html"
fig.write_html(str(output_path), include_plotlyjs="cdn")

# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------
print("=" * 60)
print("  EXECUTIVE DASHBOARD -- Summary Statistics")
print("=" * 60)
print(f"  Total Unique Listeners : {total_listeners:,}")
print(f"  Avg Listen Duration    : {avg_listen_min} min")
print(f"  Completion Rate        : {completion_rate}%")
print(f"  Total Ad Revenue       : {total_ad_revenue:,.2f} SAR")
print(f"  DAU Range              : {dau_df['dau'].min()} - {dau_df['dau'].max()}")
print(f"  Days of Data           : {len(dau_df)}")
print("-" * 60)
print("  Top Platforms:")
for _, row in platform_df.iterrows():
    print(f"    {str(row['platform']):20s} {int(row['listens']):>8,} listens")
print("-" * 60)
print("  Category Mix:")
for _, row in category_df.iterrows():
    print(f"    {str(row['category']):20s} {int(row['listens']):>8,} listens")
print("=" * 60)
print(f"\n  Dashboard saved to: {output_path}")
