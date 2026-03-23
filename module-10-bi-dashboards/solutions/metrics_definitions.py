#!/usr/bin/env python3
"""
Module 10 -- Exercise 8: Programmatic Metrics Layer
=====================================================
Reads metric definitions from metrics/metrics.yml, computes current values
from the raw data, evaluates health status, and outputs both a console
report and an HTML metrics card page.

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
    CREATE TABLE events AS
    SELECT * FROM read_json_auto('{DATA_DIR}/listening_events/*.jsonl')
""")
con.execute(f"""
    CREATE TABLE users AS
    SELECT * FROM read_csv_auto('{DATA_DIR}/users.csv')
""")
con.execute(f"""
    CREATE TABLE ads AS
    SELECT * FROM read_json_auto('{DATA_DIR}/ad_events.json')
""")
con.execute(f"""
    CREATE TABLE cdn AS
    SELECT * FROM read_csv_auto('{DATA_DIR}/cdn_logs.csv')
""")

# ---------------------------------------------------------------------------
# Compute metrics
# ---------------------------------------------------------------------------
# We map each metric name to a SQL query that returns a single scalar.
METRIC_QUERIES = {
    "daily_active_users": """
        SELECT ROUND(AVG(dau), 0) FROM (
            SELECT CAST(timestamp AS DATE) AS d, COUNT(DISTINCT user_id) AS dau
            FROM events GROUP BY 1
        )
    """,
    "weekly_active_users": """
        SELECT COUNT(DISTINCT user_id) FROM events
        WHERE CAST(timestamp AS TIMESTAMP) >= (
            SELECT MAX(CAST(timestamp AS TIMESTAMP)) - INTERVAL '7 days' FROM events
        )
    """,
    "monthly_active_users": """
        SELECT COUNT(DISTINCT user_id) FROM events
        WHERE CAST(timestamp AS TIMESTAMP) >= (
            SELECT MAX(CAST(timestamp AS TIMESTAMP)) - INTERVAL '30 days' FROM events
        )
    """,
    "dau_mau_ratio": """
        WITH bounds AS (
            SELECT MAX(CAST(timestamp AS TIMESTAMP)) AS max_ts FROM events
        ),
        dau AS (
            SELECT COUNT(DISTINCT user_id) AS n FROM events, bounds
            WHERE CAST(timestamp AS DATE) = CAST(bounds.max_ts AS DATE)
        ),
        mau AS (
            SELECT COUNT(DISTINCT user_id) AS n FROM events, bounds
            WHERE CAST(timestamp AS TIMESTAMP) >= bounds.max_ts - INTERVAL '30 days'
        )
        SELECT ROUND(dau.n * 1.0 / NULLIF(mau.n, 0), 4) FROM dau, mau
    """,
    "avg_listen_duration_seconds": """
        SELECT ROUND(AVG(listened_seconds), 1) FROM events
    """,
    "completion_rate": """
        SELECT ROUND(
            COUNT(*) FILTER (WHERE event_type = 'complete') * 1.0 /
            NULLIF(COUNT(*) FILTER (WHERE event_type IN ('play','resume','complete')), 0),
        4) FROM events
    """,
    "listener_retention_d7": """
        WITH first_listen AS (
            SELECT user_id, MIN(CAST(timestamp AS DATE)) AS first_date
            FROM events GROUP BY 1
        ),
        retained AS (
            SELECT fl.user_id
            FROM first_listen fl
            JOIN events e ON fl.user_id = e.user_id
            WHERE CAST(e.timestamp AS DATE) BETWEEN fl.first_date + 6 AND fl.first_date + 8
        )
        SELECT ROUND(COUNT(DISTINCT retained.user_id) * 1.0 /
               NULLIF((SELECT COUNT(*) FROM first_listen), 0), 4)
        FROM retained
    """,
    "monthly_churn_rate": """
        WITH months AS (
            SELECT DISTINCT DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) AS m
            FROM events ORDER BY 1
        ),
        last_two AS (
            SELECT m FROM months ORDER BY m DESC LIMIT 2
        ),
        prev AS (
            SELECT DISTINCT user_id FROM events
            WHERE DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) = (
                SELECT MIN(m) FROM last_two
            )
        ),
        curr AS (
            SELECT DISTINCT user_id FROM events
            WHERE DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) = (
                SELECT MAX(m) FROM last_two
            )
        )
        SELECT ROUND(1.0 - COUNT(DISTINCT curr.user_id) * 1.0 /
               NULLIF((SELECT COUNT(*) FROM prev), 0), 4)
        FROM prev LEFT JOIN curr ON prev.user_id = curr.user_id
        WHERE curr.user_id IS NOT NULL
    """,
    "top_podcasts_by_plays": """
        SELECT COUNT(*) FROM events
    """,
    "trending_episodes": """
        SELECT COUNT(DISTINCT episode_id) FROM events
    """,
    "total_ad_revenue": """
        SELECT ROUND(SUM(revenue_sar), 2) FROM ads
    """,
    "ad_fill_rate": """
        SELECT ROUND(
            COUNT(*) FILTER (WHERE action = 'impression') * 1.0 /
            NULLIF(COUNT(*), 0),
        4) FROM ads
    """,
    "effective_cpm": """
        SELECT ROUND(
            SUM(revenue_sar) /
            NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0) * 1000,
        2) FROM ads
    """,
    "ad_click_through_rate": """
        SELECT ROUND(
            COUNT(*) FILTER (WHERE action = 'click') * 1.0 /
            NULLIF(COUNT(*) FILTER (WHERE action = 'impression'), 0),
        4) FROM ads
    """,
    "rebuffer_rate": """
        SELECT ROUND(AVG(rebuffer_ratio), 4) FROM cdn
    """,
    "median_startup_time_ms": """
        SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY startup_time_ms), 0)
        FROM cdn
    """,
    "streaming_error_rate": """
        SELECT ROUND(
            COUNT(*) FILTER (WHERE error_type IS NOT NULL AND error_type != '') * 1.0
            / COUNT(*),
        4) FROM cdn
    """,
    "listeners_by_country": """
        SELECT COUNT(DISTINCT country) FROM events
        WHERE country IS NOT NULL AND country != ''
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
print(f"  {'Metric':35s} {'Value':>15s} {'Status':>8s}  {'Domain':>14s}")
print("  " + "-" * 76)

STATUS_ICON = {"green": "[OK]", "yellow": "[WARN]", "red": "[CRIT]"}

for r in results:
    val_str = f"{r['value']:,.4f}" if r["value"] is not None else "N/A"
    # Clean up trailing zeros for readability
    if r["value"] is not None and r["value"] == int(r["value"]):
        val_str = f"{int(r['value']):,}"
    icon = STATUS_ICON[r["status"]]
    print(f"  {r['display_name']:35s} {val_str:>15s} {icon:>8s}  {r['domain']:>14s}")

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
    <h1>Podcast Platform -- Metrics Layer Report</h1>
    <p style="text-align:center; color:#666;">
        Computed from raw data. Status:
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
