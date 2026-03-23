#!/usr/bin/env python3
"""
Module 10 -- Exercise 7: Geographic Distribution Analysis
===========================================================
Choropleth map, top countries, platform preference by country,
subscription mix, listening volume, and summary table.

Outputs: ../output/geographic_analysis.html
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
    CREATE TABLE events AS
    SELECT * FROM read_json_auto('{DATA_DIR}/listening_events/*.jsonl')
""")
con.execute(f"""
    CREATE TABLE users AS
    SELECT * FROM read_csv_auto('{DATA_DIR}/users.csv')
""")

# ---------------------------------------------------------------------------
# ISO-2 to ISO-3 mapping for choropleth
# ---------------------------------------------------------------------------
iso_map = {
    "SA": "SAU", "AE": "ARE", "KW": "KWT", "BH": "BHR", "QA": "QAT",
    "OM": "OMN", "EG": "EGY", "JO": "JOR", "LB": "LBN", "IQ": "IRQ",
    "MA": "MAR", "TN": "TUN", "DZ": "DZA", "US": "USA", "GB": "GBR",
    "FR": "FRA", "DE": "DEU", "CA": "CAN", "AU": "AUS", "IN": "IND",
    "PK": "PAK", "TR": "TUR", "MY": "MYS", "ID": "IDN", "SY": "SYR",
    "YE": "YEM", "LY": "LBY", "SD": "SDN", "PS": "PSE", "NL": "NLD",
    "SE": "SWE", "IT": "ITA", "ES": "ESP", "BR": "BRA", "MX": "MEX",
    "NG": "NGA", "ZA": "ZAF", "KE": "KEN", "JP": "JPN", "KR": "KOR",
    "CN": "CHN", "SG": "SGP", "PH": "PHL", "TH": "THA", "RU": "RUS",
}

# ---------------------------------------------------------------------------
# Listeners by country
# ---------------------------------------------------------------------------
country_listeners = con.execute("""
    SELECT country,
           COUNT(DISTINCT user_id) AS listeners,
           ROUND(SUM(listened_seconds) / 3600.0, 1) AS total_hours,
           ROUND(AVG(listened_seconds) / 60.0, 1) AS avg_session_min,
           COUNT(*) AS total_events
    FROM events
    WHERE country IS NOT NULL AND country != ''
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

country_listeners["iso3"] = country_listeners["country"].map(iso_map)

# ---------------------------------------------------------------------------
# Top 10 countries
# ---------------------------------------------------------------------------
top10 = country_listeners.head(10)

# ---------------------------------------------------------------------------
# Platform preference by top country
# ---------------------------------------------------------------------------
platform_by_country = con.execute("""
    SELECT e.country, e.platform, COUNT(*) AS events
    FROM events e
    WHERE e.country IN (
        SELECT country FROM (
            SELECT country, COUNT(DISTINCT user_id) AS n
            FROM events WHERE country IS NOT NULL AND country != ''
            GROUP BY 1 ORDER BY 2 DESC LIMIT 8
        )
    )
    GROUP BY 1, 2
    ORDER BY 1, 3 DESC
""").df()

# ---------------------------------------------------------------------------
# Subscription type by country (from users table)
# ---------------------------------------------------------------------------
sub_by_country = con.execute("""
    SELECT country, subscription_type, COUNT(*) AS users
    FROM users
    WHERE country IS NOT NULL AND country != ''
    GROUP BY 1, 2
    ORDER BY 1
""").df()

# Compute premium percentage per country
premium_pct = con.execute("""
    SELECT country,
           COUNT(*) AS total,
           COUNT(*) FILTER (WHERE subscription_type = 'premium') AS premium,
           ROUND(COUNT(*) FILTER (WHERE subscription_type = 'premium') * 100.0
                 / COUNT(*), 1) AS pct_premium
    FROM users
    WHERE country IS NOT NULL AND country != ''
    GROUP BY 1
    ORDER BY 4 DESC
""").df()

# ---------------------------------------------------------------------------
# Build dashboard
# ---------------------------------------------------------------------------
fig = make_subplots(
    rows=3, cols=2,
    subplot_titles=(
        "Listeners by Country (World Map)",
        "Top 10 Countries by Listener Count",
        "Platform Mix by Country (Top 8)",
        "Premium Subscription % by Country",
        "Total Listening Hours by Country (Top 10)",
        "",
    ),
    specs=[
        [{"type": "choropleth"}, {"type": "xy"}],
        [{"type": "xy"}, {"type": "xy"}],
        [{"type": "xy"}, {"type": "table"}],
    ],
    row_heights=[0.40, 0.30, 0.30],
    vertical_spacing=0.08,
    horizontal_spacing=0.10,
)

# 1 -- Choropleth
fig.add_trace(go.Choropleth(
    locations=country_listeners["iso3"],
    z=country_listeners["listeners"],
    text=country_listeners["country"],
    colorscale="YlOrRd",
    marker_line_color="white",
    marker_line_width=0.5,
    colorbar_title="Listeners",
    showscale=True,
), row=1, col=1)
fig.update_geos(
    showframe=False, showcoastlines=True,
    projection_type="natural earth",
)

# 2 -- Top 10 countries bar
fig.add_trace(go.Bar(
    y=top10["country"],
    x=top10["listeners"],
    orientation="h",
    marker_color="#636EFA",
    showlegend=False,
), row=1, col=2)
fig.update_yaxes(autorange="reversed", row=1, col=2)

# 3 -- Platform mix stacked bar
platforms = platform_by_country["platform"].unique()
palette = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
           "#19D3F3", "#FF6692", "#B6E880"]
for i, plat in enumerate(platforms):
    sub = platform_by_country[platform_by_country["platform"] == plat]
    fig.add_trace(go.Bar(
        x=sub["country"], y=sub["events"],
        name=plat,
        marker_color=palette[i % len(palette)],
    ), row=2, col=1)
fig.update_layout(barmode="stack")

# 4 -- Premium % bar
top_premium = premium_pct.head(15)
fig.add_trace(go.Bar(
    x=top_premium["country"],
    y=top_premium["pct_premium"],
    marker_color="#00CC96",
    showlegend=False,
), row=2, col=2)

# 5 -- Listening hours bar
fig.add_trace(go.Bar(
    x=top10["country"],
    y=top10["total_hours"],
    marker_color="#FFA15A",
    showlegend=False,
), row=3, col=1)

# 6 -- Summary table
summary = country_listeners.head(15)
fig.add_trace(go.Table(
    header=dict(
        values=["Country", "Listeners", "Hours", "Avg Min", "Events"],
        fill_color="#636EFA",
        font=dict(color="white", size=12),
        align="left",
    ),
    cells=dict(
        values=[
            summary["country"],
            summary["listeners"],
            summary["total_hours"],
            summary["avg_session_min"],
            summary["total_events"],
        ],
        fill_color="lavender",
        align="left",
    ),
), row=3, col=2)

fig.update_layout(
    title={"text": "Geographic Distribution of Listeners", "font": {"size": 24}, "x": 0.5},
    height=1200,
    template="plotly_white",
    margin={"t": 80, "b": 40},
)

output_path = OUTPUT_DIR / "geographic_analysis.html"
fig.write_html(str(output_path), include_plotlyjs="cdn")

# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------
print("=" * 65)
print("  GEOGRAPHIC ANALYSIS -- Summary Statistics")
print("=" * 65)
print(f"  Countries with listeners : {len(country_listeners)}")
print(f"  Total listeners          : {country_listeners['listeners'].sum():,}")
print()
print("  Top 10 Countries:")
print(f"  {'Country':>8s} {'Listeners':>10s} {'Hours':>10s} {'AvgMin':>8s}")
print("  " + "-" * 40)
for _, row in top10.iterrows():
    print(f"  {row['country']:>8s} {row['listeners']:>10,} {row['total_hours']:>10.1f}"
          f" {row['avg_session_min']:>8.1f}")
print()
print("  Premium Subscription % (top 10):")
for _, row in premium_pct.head(10).iterrows():
    print(f"    {row['country']:>8s} {row['pct_premium']:>6.1f}%"
          f"  ({row['premium']:,}/{row['total']:,})")
print("=" * 65)
print(f"\n  Dashboard saved to: {output_path}")
