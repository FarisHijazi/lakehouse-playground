#!/usr/bin/env python3
"""
Module 10 -- Exercise 4: Programmatic Metrics Layer
=====================================================
Reads metric definitions from metrics/metrics.yml, computes current values
from the NYC taxi raw data, evaluates health status, and outputs both a
console report and an HTML metrics card page.

Outputs: ../output/metrics_report.html
"""

import duckdb
import yaml
import plotly.graph_objects as go
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
MODULE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = MODULE_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
METRICS_FILE = MODULE_DIR / "metrics" / "metrics.yml"

# ---------------------------------------------------------------------------
# Load metric definitions
# ---------------------------------------------------------------------------
with open(METRICS_FILE) as f:
    metrics_config = yaml.safe_load(f)

metric_defs = metrics_config["metrics"]

# ---------------------------------------------------------------------------
# Connect and load data
# ---------------------------------------------------------------------------
con = duckdb.connect()

con.execute(f"""
    CREATE TABLE trips AS
    SELECT * FROM read_parquet('{DATA_DIR}/yellow_tripdata_*.parquet')
""")
con.execute(f"""
    CREATE TABLE zones AS
    SELECT * FROM read_csv_auto('{DATA_DIR}/taxi_zone_lookup.csv')
""")
con.execute(f"""
    CREATE TABLE payment_types AS
    SELECT * FROM read_csv_auto('{DATA_DIR}/payment_types.csv')
""")

# ---------------------------------------------------------------------------
# Compute metrics
# ---------------------------------------------------------------------------
# We map each metric name to a SQL query that returns a single scalar.
METRIC_QUERIES = {
    "daily_trips": """
        SELECT ROUND(AVG(daily_count), 0) FROM (
            SELECT CAST(tpep_pickup_datetime AS DATE) AS d, COUNT(*) AS daily_count
            FROM trips GROUP BY 1
        )
    """,
    "trips_per_hour": """
        SELECT ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT
            CAST(tpep_pickup_datetime AS DATE) || '-' ||
            EXTRACT(HOUR FROM tpep_pickup_datetime)
        ), 0), 1)
        FROM trips
    """,
    "avg_passenger_count": """
        SELECT ROUND(AVG(passenger_count), 2) FROM trips
        WHERE passenger_count > 0
    """,
    "total_revenue": """
        SELECT ROUND(SUM(total_amount), 2) FROM trips
    """,
    "avg_fare_amount": """
        SELECT ROUND(AVG(fare_amount), 2) FROM trips
        WHERE fare_amount > 0
    """,
    "avg_tip_percentage": """
        SELECT ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)), 4) FROM trips
        WHERE fare_amount > 0 AND tip_amount >= 0
    """,
    "revenue_per_mile": """
        SELECT ROUND(SUM(total_amount) / NULLIF(SUM(trip_distance), 0), 2)
        FROM trips WHERE trip_distance > 0
    """,
    "avg_trip_distance": """
        SELECT ROUND(AVG(trip_distance), 2) FROM trips
        WHERE trip_distance > 0
    """,
    "avg_trip_duration_minutes": """
        SELECT ROUND(AVG(
            DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime)
        ), 1)
        FROM trips
        WHERE tpep_dropoff_datetime > tpep_pickup_datetime
    """,
    "p95_trip_duration_minutes": """
        SELECT ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
            DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime)
        ), 1)
        FROM trips
        WHERE tpep_dropoff_datetime > tpep_pickup_datetime
          AND DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) > 0
    """,
    "short_trip_rate": """
        SELECT ROUND(
            COUNT(*) FILTER (WHERE trip_distance < 1) * 1.0 / NULLIF(COUNT(*), 0),
        4) FROM trips WHERE trip_distance >= 0
    """,
    "trips_by_borough": """
        SELECT COUNT(*) FROM trips
    """,
    "top_pickup_zones": """
        SELECT COUNT(DISTINCT PULocationID) FROM trips
    """,
    "cross_borough_rate": """
        SELECT ROUND(
            COUNT(*) FILTER (WHERE pz.Borough != dz.Borough) * 1.0 /
            NULLIF(COUNT(*), 0),
        4)
        FROM trips t
        JOIN zones pz ON t.PULocationID = pz.LocationID
        JOIN zones dz ON t.DOLocationID = dz.LocationID
        WHERE pz.Borough IS NOT NULL AND dz.Borough IS NOT NULL
          AND pz.Borough != 'Unknown' AND dz.Borough != 'Unknown'
    """,
    "credit_card_rate": """
        SELECT ROUND(
            COUNT(*) FILTER (WHERE payment_type = 1) * 1.0 / NULLIF(COUNT(*), 0),
        4) FROM trips
    """,
    "avg_tip_credit_card": """
        SELECT ROUND(AVG(tip_amount), 2) FROM trips
        WHERE payment_type = 1
    """,
}


def evaluate_status(metric_def, value):
    """Return 'green', 'yellow', or 'red' based on thresholds."""
    thresholds = metric_def.get("thresholds") or {}
    if not thresholds or value is None:
        return "green"

    # Check critical first, then warning
    if "critical_below" in thresholds and value < thresholds["critical_below"]:
        return "red"
    if "critical_above" in thresholds and value > thresholds["critical_above"]:
        return "red"
    if "warning_below" in thresholds and value < thresholds["warning_below"]:
        return "yellow"
    if "warning_above" in thresholds and value > thresholds["warning_above"]:
        return "yellow"
    return "green"


# Compute each metric
results = []
for mdef in metric_defs:
    name = mdef["name"]
    query = METRIC_QUERIES.get(name)
    value = None
    if query:
        try:
            value = con.execute(query).fetchone()[0]
            if value is not None:
                value = float(value)
        except Exception as e:
            value = None
            print(f"  [WARN] Could not compute {name}: {e}")

    status = evaluate_status(mdef, value)
    results.append({
        "name": name,
        "display_name": mdef["display_name"],
        "domain": mdef["domain"],
        "value": value,
        "status": status,
        "description": mdef.get("description", "").strip(),
        "owner": mdef.get("owner", ""),
    })

# ---------------------------------------------------------------------------
# Console report
# ---------------------------------------------------------------------------
print("=" * 80)
print("  METRICS LAYER -- Current Values Report")
print("=" * 80)
print(f"  {'Metric':35s} {'Value':>15s} {'Status':>8s}  {'Domain':>18s}")
print("  " + "-" * 76)

STATUS_ICON = {"green": "[OK]", "yellow": "[WARN]", "red": "[CRIT]"}

for r in results:
    val_str = f"{r['value']:,.4f}" if r["value"] is not None else "N/A"
    # Clean up trailing zeros for readability
    if r["value"] is not None and r["value"] == int(r["value"]):
        val_str = f"{int(r['value']):,}"
    icon = STATUS_ICON[r["status"]]
    print(f"  {r['display_name']:35s} {val_str:>15s} {icon:>8s}  {r['domain']:>18s}")

print("=" * 80)

# ---------------------------------------------------------------------------
# Build HTML report
# ---------------------------------------------------------------------------
STATUS_COLOR = {"green": "#28a745", "yellow": "#ffc107", "red": "#dc3545"}

cards_html = []
for r in results:
    val_str = f"{r['value']:,.4f}" if r["value"] is not None else "N/A"
    if r["value"] is not None and r["value"] == int(r["value"]):
        val_str = f"{int(r['value']):,}"
    color = STATUS_COLOR[r["status"]]
    cards_html.append(f"""
    <div style="border:1px solid #ddd; border-left:5px solid {color};
                border-radius:8px; padding:16px; margin:10px;
                width:320px; display:inline-block; vertical-align:top;
                box-shadow:2px 2px 6px rgba(0,0,0,0.08);">
        <div style="font-size:12px; color:#888; text-transform:uppercase;">
            {r['domain']}
        </div>
        <div style="font-size:16px; font-weight:bold; margin:4px 0;">
            {r['display_name']}
        </div>
        <div style="font-size:28px; font-weight:bold; color:{color};">
            {val_str}
        </div>
        <div style="font-size:12px; color:#666; margin-top:8px;">
            {r['description'][:120]}
        </div>
        <div style="font-size:11px; color:#aaa; margin-top:6px;">
            Owner: {r['owner']}
        </div>
    </div>
    """)

html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Metrics Layer Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI',
               Roboto, sans-serif; background: #f5f5f5; padding: 20px; }}
        h1 {{ text-align: center; color: #333; }}
        .cards {{ text-align: center; max-width: 1200px; margin: 0 auto; }}
    </style>
</head>
<body>
    <h1>NYC Taxi Analytics -- Metrics Layer Report</h1>
    <p style="text-align:center; color:#666;">
        Computed from raw taxi trip data. Status:
        <span style="color:#28a745;">&#9679; OK</span> &nbsp;
        <span style="color:#ffc107;">&#9679; Warning</span> &nbsp;
        <span style="color:#dc3545;">&#9679; Critical</span>
    </p>
    <div class="cards">
        {''.join(cards_html)}
    </div>
</body>
</html>
"""

output_path = OUTPUT_DIR / "metrics_report.html"
with open(output_path, "w") as f:
    f.write(html_content)

print(f"\n  Metrics report saved to: {output_path}")
