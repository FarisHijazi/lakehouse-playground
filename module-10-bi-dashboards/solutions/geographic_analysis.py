#!/usr/bin/env python3
"""
Module 10 -- Exercise 3: Geographic Analysis Dashboard
========================================================
Analyzes NYC taxi trip patterns across boroughs and zones: top pickup/dropoff
zones, borough-to-borough flow heatmap, popular routes, and summary statistics.

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
    CREATE TABLE trips AS
    SELECT * FROM read_parquet('{DATA_DIR}/yellow_tripdata_*.parquet')
""")
con.execute(f"""
    CREATE TABLE zones AS
    SELECT * FROM read_csv_auto('{DATA_DIR}/taxi_zone_lookup.csv')
""")

# ---------------------------------------------------------------------------
# Top 15 pickup zones
# ---------------------------------------------------------------------------
top_pickup = con.execute("""
    SELECT z.Zone AS zone_name, z.Borough AS borough, COUNT(*) AS num_trips
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    WHERE z.Zone IS NOT NULL AND z.Borough != 'Unknown'
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 15
""").df()

# ---------------------------------------------------------------------------
# Top 15 dropoff zones
# ---------------------------------------------------------------------------
top_dropoff = con.execute("""
    SELECT z.Zone AS zone_name, z.Borough AS borough, COUNT(*) AS num_trips
    FROM trips t
    JOIN zones z ON t.DOLocationID = z.LocationID
    WHERE z.Zone IS NOT NULL AND z.Borough != 'Unknown'
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 15
""").df()

# ---------------------------------------------------------------------------
# Borough-to-borough flow (heatmap)
# ---------------------------------------------------------------------------
borough_flow = con.execute("""
    SELECT
        pz.Borough AS pickup_borough,
        dz.Borough AS dropoff_borough,
        COUNT(*) AS num_trips
    FROM trips t
    JOIN zones pz ON t.PULocationID = pz.LocationID
    JOIN zones dz ON t.DOLocationID = dz.LocationID
    WHERE pz.Borough IS NOT NULL AND dz.Borough IS NOT NULL
      AND pz.Borough != 'Unknown' AND dz.Borough != 'Unknown'
    GROUP BY 1, 2
    ORDER BY 3 DESC
""").df()

# Pivot for heatmap
boroughs = sorted(borough_flow["pickup_borough"].unique())
heatmap_data = []
for pu_b in boroughs:
    row = []
    for do_b in boroughs:
        match = borough_flow[
            (borough_flow["pickup_borough"] == pu_b) &
            (borough_flow["dropoff_borough"] == do_b)
        ]
        row.append(int(match["num_trips"].sum()) if len(match) > 0 else 0)
    heatmap_data.append(row)

# ---------------------------------------------------------------------------
# Top 10 routes (zone pairs)
# ---------------------------------------------------------------------------
top_routes = con.execute("""
    SELECT
        pz.Zone AS pickup_zone,
        dz.Zone AS dropoff_zone,
        pz.Borough AS pu_borough,
        dz.Borough AS do_borough,
        COUNT(*) AS num_trips,
        ROUND(AVG(t.fare_amount), 2) AS avg_fare,
        ROUND(AVG(t.trip_distance), 2) AS avg_distance
    FROM trips t
    JOIN zones pz ON t.PULocationID = pz.LocationID
    JOIN zones dz ON t.DOLocationID = dz.LocationID
    WHERE pz.Zone IS NOT NULL AND dz.Zone IS NOT NULL
      AND pz.Borough != 'Unknown' AND dz.Borough != 'Unknown'
    GROUP BY 1, 2, 3, 4
    ORDER BY 5 DESC
    LIMIT 10
""").df()

# ---------------------------------------------------------------------------
# Borough summary statistics
# ---------------------------------------------------------------------------
borough_summary = con.execute("""
    SELECT
        pz.Borough AS borough,
        COUNT(*) AS total_trips,
        ROUND(AVG(t.fare_amount), 2) AS avg_fare,
        ROUND(AVG(t.trip_distance), 2) AS avg_distance,
        ROUND(AVG(t.tip_amount / NULLIF(t.fare_amount, 0)) * 100, 1) AS avg_tip_pct
    FROM trips t
    JOIN zones pz ON t.PULocationID = pz.LocationID
    WHERE pz.Borough IS NOT NULL AND pz.Borough != 'Unknown'
      AND t.fare_amount > 0
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ---------------------------------------------------------------------------
# Build dashboard
# ---------------------------------------------------------------------------
fig = make_subplots(
    rows=3, cols=2,
    subplot_titles=(
        "Top 15 Pickup Zones",
        "Top 15 Dropoff Zones",
        "Borough-to-Borough Trip Flow",
        "Top 10 Routes (Zone Pairs)",
        "Borough Summary Statistics",
        "",
    ),
    specs=[
        [{"type": "xy"}, {"type": "xy"}],
        [{"type": "xy"}, {"type": "table"}],
        [{"type": "table"}, {"type": "xy"}],
    ],
    row_heights=[0.35, 0.35, 0.30],
    vertical_spacing=0.08,
    horizontal_spacing=0.10,
)

# 1 -- Top pickup zones (horizontal bar)
fig.add_trace(go.Bar(
    y=top_pickup["zone_name"],
    x=top_pickup["num_trips"],
    orientation="h",
    marker_color="#636EFA",
    showlegend=False,
    text=top_pickup["borough"],
    textposition="inside",
), row=1, col=1)
fig.update_yaxes(autorange="reversed", row=1, col=1)

# 2 -- Top dropoff zones (horizontal bar)
fig.add_trace(go.Bar(
    y=top_dropoff["zone_name"],
    x=top_dropoff["num_trips"],
    orientation="h",
    marker_color="#EF553B",
    showlegend=False,
    text=top_dropoff["borough"],
    textposition="inside",
), row=1, col=2)
fig.update_yaxes(autorange="reversed", row=1, col=2)

# 3 -- Borough-to-borough heatmap
fig.add_trace(go.Heatmap(
    z=heatmap_data,
    x=boroughs,
    y=boroughs,
    colorscale="YlOrRd",
    showscale=True,
    colorbar={"title": "Trips", "len": 0.3, "y": 0.5},
    text=[[f"{v:,}" for v in row] for row in heatmap_data],
    texttemplate="%{text}",
    textfont={"size": 9},
), row=2, col=1)
fig.update_xaxes(title_text="Dropoff Borough", row=2, col=1)
fig.update_yaxes(title_text="Pickup Borough", row=2, col=1)

# 4 -- Top routes table
route_labels = [
    f"{r['pickup_zone']} -> {r['dropoff_zone']}" for _, r in top_routes.iterrows()
]
fig.add_trace(go.Table(
    header=dict(
        values=["Route", "Trips", "Avg Fare", "Avg Dist"],
        fill_color="#EF553B",
        font=dict(color="white", size=11),
        align="left",
    ),
    cells=dict(
        values=[
            route_labels,
            [f"{v:,}" for v in top_routes["num_trips"]],
            [f"${v:.2f}" for v in top_routes["avg_fare"]],
            [f"{v:.1f} mi" for v in top_routes["avg_distance"]],
        ],
        fill_color="lavender",
        align="left",
    ),
), row=2, col=2)

# 5 -- Borough summary table
fig.add_trace(go.Table(
    header=dict(
        values=["Borough", "Total Trips", "Avg Fare", "Avg Distance", "Avg Tip %"],
        fill_color="#636EFA",
        font=dict(color="white", size=12),
        align="left",
    ),
    cells=dict(
        values=[
            borough_summary["borough"],
            [f"{v:,}" for v in borough_summary["total_trips"]],
            [f"${v:.2f}" for v in borough_summary["avg_fare"]],
            [f"{v:.2f} mi" for v in borough_summary["avg_distance"]],
            [f"{v:.1f}%" for v in borough_summary["avg_tip_pct"]],
        ],
        fill_color="lavender",
        align="left",
    ),
), row=3, col=1)

# 6 -- Trips by borough bar chart (simple overview)
fig.add_trace(go.Bar(
    x=borough_summary["borough"],
    y=borough_summary["total_trips"],
    marker_color="#00CC96",
    showlegend=False,
), row=3, col=2)

fig.update_layout(
    title={"text": "NYC Taxi -- Geographic Analysis", "font": {"size": 24}, "x": 0.5},
    height=1400,
    template="plotly_white",
    margin={"t": 80, "b": 40},
)

output_path = OUTPUT_DIR / "geographic_analysis.html"
fig.write_html(str(output_path), include_plotlyjs="cdn")

# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------
print("=" * 70)
print("  GEOGRAPHIC ANALYSIS -- Summary Statistics")
print("=" * 70)
print()
print("  Borough Summary:")
print(f"  {'Borough':>15s} {'Trips':>12s} {'AvgFare':>10s} {'AvgDist':>10s} {'TipPct':>8s}")
print("  " + "-" * 58)
for _, row in borough_summary.iterrows():
    print(f"  {row['borough']:>15s} {row['total_trips']:>12,} ${row['avg_fare']:>8.2f}"
          f" {row['avg_distance']:>9.2f} {row['avg_tip_pct']:>7.1f}%")
print()
print("  Top 10 Routes:")
print(f"  {'Route':>50s} {'Trips':>10s} {'AvgFare':>10s}")
print("  " + "-" * 72)
for _, row in top_routes.iterrows():
    route = f"{row['pickup_zone']} -> {row['dropoff_zone']}"
    print(f"  {route:>50s} {row['num_trips']:>10,} ${row['avg_fare']:>8.2f}")
print()
print("  Top 5 Pickup Zones:")
for _, row in top_pickup.head(5).iterrows():
    print(f"    {row['zone_name']:30s} ({row['borough']}) {row['num_trips']:>10,} trips")
print()
print("  Top 5 Dropoff Zones:")
for _, row in top_dropoff.head(5).iterrows():
    print(f"    {row['zone_name']:30s} ({row['borough']}) {row['num_trips']:>10,} trips")
print("=" * 70)
print(f"\n  Dashboard saved to: {output_path}")
