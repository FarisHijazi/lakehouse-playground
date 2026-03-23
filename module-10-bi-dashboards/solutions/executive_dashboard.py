#!/usr/bin/env python3
"""
Module 10 -- Exercise 2: Executive Summary Dashboard
=====================================================
A single-page dashboard giving leadership a quick overview of NYC taxi
fleet performance: KPI cards, daily trip trend, borough breakdown, and
payment type mix.

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

trips = con.execute(f"""
    SELECT *
    FROM read_parquet('{DATA_DIR}/yellow_tripdata_*.parquet')
""").df()

zones = con.execute(f"""
    SELECT * FROM read_csv_auto('{DATA_DIR}/taxi_zone_lookup.csv')
""").df()

payment_types = con.execute(f"""
    SELECT * FROM read_csv_auto('{DATA_DIR}/payment_types.csv')
""").df()

# Register as tables for convenient SQL
con.register("trips", trips)
con.register("zones", zones)
con.register("payment_types", payment_types)

# ---------------------------------------------------------------------------
# KPI computations
# ---------------------------------------------------------------------------
total_trips = con.execute(
    "SELECT COUNT(*) AS n FROM trips"
).fetchone()[0]

avg_fare = con.execute(
    "SELECT ROUND(AVG(fare_amount), 2) FROM trips WHERE fare_amount > 0"
).fetchone()[0]

avg_distance = con.execute(
    "SELECT ROUND(AVG(trip_distance), 2) FROM trips WHERE trip_distance > 0"
).fetchone()[0]

total_revenue = con.execute(
    "SELECT ROUND(SUM(total_amount), 2) FROM trips"
).fetchone()[0]

# ---------------------------------------------------------------------------
# Daily trips over time
# ---------------------------------------------------------------------------
daily_trips_df = con.execute("""
    SELECT
        CAST(tpep_pickup_datetime AS DATE) AS trip_date,
        COUNT(*) AS num_trips
    FROM trips
    GROUP BY 1
    ORDER BY 1
""").df()

# ---------------------------------------------------------------------------
# Trips by borough (pickup)
# ---------------------------------------------------------------------------
borough_df = con.execute("""
    SELECT z.Borough AS borough, COUNT(*) AS num_trips
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    WHERE z.Borough IS NOT NULL AND z.Borough != 'Unknown'
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ---------------------------------------------------------------------------
# Trips by payment type
# ---------------------------------------------------------------------------
payment_df = con.execute("""
    SELECT
        COALESCE(pt.description, 'Unknown') AS payment_method,
        COUNT(*) AS num_trips
    FROM trips t
    LEFT JOIN payment_types pt ON t.payment_type = pt.payment_type
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
    subplot_titles=("", "", "Daily Trip Count", "", "Trips by Borough", "Trips by Payment Type"),
    vertical_spacing=0.10,
    horizontal_spacing=0.10,
)

# KPI cards (row 1)
fig.add_trace(go.Indicator(
    mode="number",
    value=total_trips,
    title={"text": "Total Trips"},
    number={"font": {"size": 40}},
), row=1, col=1)

fig.add_trace(go.Indicator(
    mode="number",
    value=avg_fare,
    title={"text": "Avg Fare ($)"},
    number={"font": {"size": 40}, "prefix": "$"},
), row=1, col=2)

# Daily trip trend (row 2)
fig.add_trace(go.Scatter(
    x=daily_trips_df["trip_date"],
    y=daily_trips_df["num_trips"],
    mode="lines",
    fill="tozeroy",
    line={"color": "#636EFA", "width": 2},
    name="Daily Trips",
), row=2, col=1)

# Borough bar chart (row 3, col 1)
fig.add_trace(go.Bar(
    x=borough_df["borough"],
    y=borough_df["num_trips"],
    marker_color="#EF553B",
    name="Borough",
    showlegend=False,
), row=3, col=1)

# Payment type pie chart (row 3, col 2)
fig.add_trace(go.Pie(
    labels=payment_df["payment_method"],
    values=payment_df["num_trips"],
    hole=0.4,
    name="Payment Type",
), row=3, col=2)

fig.update_layout(
    title={
        "text": "NYC Taxi -- Executive Dashboard",
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
    text=f"<b>Avg Distance</b><br>{avg_distance} mi",
    xref="paper", yref="paper",
    x=0.62, y=0.95,
    showarrow=False,
    font={"size": 16},
    align="center",
)
fig.add_annotation(
    text=f"<b>Total Revenue</b><br>${total_revenue:,.0f}",
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
print(f"  Total Trips            : {total_trips:,}")
print(f"  Avg Fare               : ${avg_fare}")
print(f"  Avg Trip Distance      : {avg_distance} mi")
print(f"  Total Revenue          : ${total_revenue:,.2f}")
print(f"  Daily Trip Range       : {daily_trips_df['num_trips'].min():,} - {daily_trips_df['num_trips'].max():,}")
print(f"  Days of Data           : {len(daily_trips_df)}")
print("-" * 60)
print("  Trips by Borough:")
for _, row in borough_df.iterrows():
    print(f"    {str(row['borough']):20s} {int(row['num_trips']):>10,} trips")
print("-" * 60)
print("  Payment Type Mix:")
for _, row in payment_df.iterrows():
    print(f"    {str(row['payment_method']):20s} {int(row['num_trips']):>10,} trips")
print("=" * 60)
print(f"\n  Dashboard saved to: {output_path}")
