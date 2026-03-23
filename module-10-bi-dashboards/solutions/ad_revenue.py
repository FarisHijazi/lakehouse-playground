#!/usr/bin/env python3
"""
Module 10 -- Exercise 5: Ad Revenue Dashboard
===============================================
Advertising performance analysis: revenue trends, ad-type breakdown,
top advertisers, conversion funnel, and campaign performance table.

Outputs: ../output/ad_revenue.html
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

con = duckdb.connect()

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE ads AS
    SELECT * FROM read_json_auto('{DATA_DIR}/ad_events.json')
""")

# ---------------------------------------------------------------------------
# KPI computations
# ---------------------------------------------------------------------------
kpis = con.execute("""
    SELECT
        ROUND(SUM(revenue_sar), 2) AS total_revenue,
        COUNT(*) FILTER (WHERE action = 'impression') AS total_impressions,
        COUNT(*) FILTER (WHERE action = 'click') AS total_clicks,
        COUNT(*) FILTER (WHERE action = 'complete') AS total_completes,
        ROUND(
            SUM(revenue_sar) / NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0) * 1000,
            2
        ) AS ecpm,
        ROUND(
            COUNT(*) FILTER (WHERE action = 'click') * 100.0 /
            NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0),
            2
        ) AS ctr_pct
    FROM ads
""").df().iloc[0]

# ---------------------------------------------------------------------------
# Monthly revenue trend
# ---------------------------------------------------------------------------
monthly_rev = con.execute("""
    SELECT DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) AS month,
           ROUND(SUM(revenue_sar), 2) AS revenue
    FROM ads
    GROUP BY 1
    ORDER BY 1
""").df()

# ---------------------------------------------------------------------------
# Revenue by ad type over time
# ---------------------------------------------------------------------------
adtype_monthly = con.execute("""
    SELECT DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) AS month,
           ad_type,
           ROUND(SUM(revenue_sar), 2) AS revenue
    FROM ads
    GROUP BY 1, 2
    ORDER BY 1
""").df()

# ---------------------------------------------------------------------------
# Top advertisers
# ---------------------------------------------------------------------------
top_adv = con.execute("""
    SELECT advertiser,
           ROUND(SUM(revenue_sar), 2) AS revenue,
           COUNT(*) FILTER (WHERE action = 'impression') AS impressions
    FROM ads
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
""").df()

# ---------------------------------------------------------------------------
# Ad funnel
# ---------------------------------------------------------------------------
funnel_df = con.execute("""
    SELECT action, COUNT(*) AS cnt
    FROM ads
    WHERE action IN ('impression', 'click', 'complete')
    GROUP BY 1
""").df()
funnel_order = {"impression": 0, "click": 1, "complete": 2}
funnel_df["order"] = funnel_df["action"].map(funnel_order)
funnel_df = funnel_df.sort_values("order")

# ---------------------------------------------------------------------------
# Campaign performance table
# ---------------------------------------------------------------------------
campaign_df = con.execute("""
    SELECT campaign_id,
           advertiser,
           ROUND(SUM(revenue_sar), 2) AS revenue,
           COUNT(*) FILTER (WHERE action = 'impression') AS impressions,
           COUNT(*) FILTER (WHERE action = 'click') AS clicks,
           COUNT(*) FILTER (WHERE action = 'complete') AS completes,
           ROUND(
               COUNT(*) FILTER (WHERE action = 'click') * 100.0 /
               NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0), 2
           ) AS ctr_pct,
           ROUND(
               COUNT(*) FILTER (WHERE action = 'complete') * 100.0 /
               NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0), 2
           ) AS completion_pct
    FROM ads
    GROUP BY 1, 2
    ORDER BY 3 DESC
""").df()

# ---------------------------------------------------------------------------
# Build dashboard
# ---------------------------------------------------------------------------
fig = make_subplots(
    rows=3, cols=2,
    subplot_titles=(
        "", "",
        "Monthly Revenue Trend (SAR)",
        "Revenue by Ad Type (Monthly)",
        "Top 10 Advertisers by Revenue",
        "Ad Conversion Funnel",
    ),
    specs=[
        [{"type": "indicator"}, {"type": "indicator"}],
        [{"type": "xy"}, {"type": "xy"}],
        [{"type": "xy"}, {"type": "xy"}],
    ],
    row_heights=[0.15, 0.40, 0.45],
    vertical_spacing=0.10,
    horizontal_spacing=0.12,
)

# KPI indicators
fig.add_trace(go.Indicator(
    mode="number",
    value=kpis["total_revenue"],
    title={"text": "Total Revenue (SAR)"},
    number={"font": {"size": 38}, "prefix": "SAR "},
), row=1, col=1)

fig.add_trace(go.Indicator(
    mode="number",
    value=kpis["ecpm"],
    title={"text": "eCPM (SAR)"},
    number={"font": {"size": 38}},
), row=1, col=2)

# Extra KPI annotations
fig.add_annotation(
    text=f"<b>Impressions</b><br>{int(kpis['total_impressions']):,}",
    xref="paper", yref="paper", x=0.62, y=0.97,
    showarrow=False, font={"size": 15},
)
fig.add_annotation(
    text=f"<b>CTR</b><br>{kpis['ctr_pct']}%",
    xref="paper", yref="paper", x=0.88, y=0.97,
    showarrow=False, font={"size": 15},
)

# Monthly revenue
fig.add_trace(go.Bar(
    x=monthly_rev["month"], y=monthly_rev["revenue"],
    marker_color="#636EFA", showlegend=False,
), row=2, col=1)

# Revenue by ad type
ad_types = adtype_monthly["ad_type"].unique()
colors = {"pre_roll": "#636EFA", "mid_roll": "#EF553B", "post_roll": "#00CC96"}
for at in ad_types:
    subset = adtype_monthly[adtype_monthly["ad_type"] == at]
    fig.add_trace(go.Bar(
        x=subset["month"], y=subset["revenue"],
        name=at, marker_color=colors.get(at, "#AB63FA"),
    ), row=2, col=2)
fig.update_layout(barmode="stack")

# Top advertisers
fig.add_trace(go.Bar(
    y=top_adv["advertiser"], x=top_adv["revenue"],
    orientation="h", marker_color="#FFA15A", showlegend=False,
), row=3, col=1)
fig.update_yaxes(autorange="reversed", row=3, col=1)

# Funnel chart
fig.add_trace(go.Funnel(
    y=funnel_df["action"],
    x=funnel_df["cnt"],
    textinfo="value+percent initial",
    marker_color=["#636EFA", "#EF553B", "#00CC96"],
), row=3, col=2)

fig.update_layout(
    title={"text": "Ad Revenue Dashboard", "font": {"size": 24}, "x": 0.5},
    height=1100,
    template="plotly_white",
    margin={"t": 80, "b": 40},
)

output_path = OUTPUT_DIR / "ad_revenue.html"
fig.write_html(str(output_path), include_plotlyjs="cdn")

# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------
print("=" * 65)
print("  AD REVENUE DASHBOARD -- Summary Statistics")
print("=" * 65)
print(f"  Total Revenue        : {kpis['total_revenue']:,.2f} SAR")
print(f"  Total Impressions    : {int(kpis['total_impressions']):,}")
print(f"  Total Clicks         : {int(kpis['total_clicks']):,}")
print(f"  Total Completes      : {int(kpis['total_completes']):,}")
print(f"  eCPM                 : {kpis['ecpm']:.2f} SAR")
print(f"  CTR                  : {kpis['ctr_pct']:.2f}%")
print()
print("  Top 10 Advertisers:")
for _, row in top_adv.iterrows():
    print(f"    {row['advertiser']:25s} {row['revenue']:>12,.2f} SAR")
print()
print("  Campaign Performance (top 10):")
print(f"  {'Campaign':15s} {'Advertiser':20s} {'Revenue':>10s} {'CTR%':>6s} {'Compl%':>7s}")
print("  " + "-" * 62)
for _, row in campaign_df.head(10).iterrows():
    print(f"  {row['campaign_id']:15s} {row['advertiser']:20s}"
          f" {row['revenue']:>10,.2f} {row['ctr_pct']:>5.2f}% {row['completion_pct']:>6.2f}%")
print("=" * 65)
print(f"\n  Dashboard saved to: {output_path}")
