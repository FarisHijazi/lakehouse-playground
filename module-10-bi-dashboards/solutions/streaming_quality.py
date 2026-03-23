#!/usr/bin/env python3
"""
Module 10 -- Exercise 6: Streaming Quality Monitoring Dashboard
================================================================
CDN and streaming quality analysis: startup time, rebuffer ratio,
error rates, quality by CDN node and ISP, bitrate distribution.

Outputs: ../output/streaming_quality.html
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
    CREATE TABLE cdn AS
    SELECT * FROM read_csv_auto('{DATA_DIR}/cdn_logs.csv')
""")

# ---------------------------------------------------------------------------
# KPI computations
# ---------------------------------------------------------------------------
kpis = con.execute("""
    SELECT
        COUNT(*) AS total_sessions,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY startup_time_ms), 0)
            AS median_startup_ms,
        ROUND(AVG(rebuffer_ratio), 4) AS avg_rebuffer_ratio,
        ROUND(COUNT(*) FILTER (WHERE error_type IS NOT NULL AND error_type != '')
              * 100.0 / COUNT(*), 2) AS error_rate_pct,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY startup_time_ms), 0)
            AS p95_startup_ms
    FROM cdn
""").df().iloc[0]

# ---------------------------------------------------------------------------
# Startup time distribution
# ---------------------------------------------------------------------------
startup_df = con.execute("""
    SELECT startup_time_ms FROM cdn WHERE startup_time_ms IS NOT NULL
""").df()

# ---------------------------------------------------------------------------
# Daily average rebuffer ratio
# ---------------------------------------------------------------------------
rebuffer_daily = con.execute("""
    SELECT CAST(timestamp AS DATE) AS day,
           ROUND(AVG(rebuffer_ratio), 4) AS avg_rebuffer
    FROM cdn
    GROUP BY 1 ORDER BY 1
""").df()

# ---------------------------------------------------------------------------
# Error type breakdown
# ---------------------------------------------------------------------------
error_df = con.execute("""
    SELECT COALESCE(NULLIF(error_type, ''), 'no_error') AS error_type,
           COUNT(*) AS cnt
    FROM cdn
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ---------------------------------------------------------------------------
# Quality by CDN node
# ---------------------------------------------------------------------------
cdn_node_df = con.execute("""
    SELECT cdn_node,
           COUNT(*) AS sessions,
           ROUND(AVG(startup_time_ms), 0) AS avg_startup_ms,
           ROUND(AVG(rebuffer_ratio), 4) AS avg_rebuffer,
           ROUND(COUNT(*) FILTER (WHERE error_type IS NOT NULL AND error_type != '')
                 * 100.0 / COUNT(*), 2) AS error_pct
    FROM cdn
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ---------------------------------------------------------------------------
# Bitrate distribution
# ---------------------------------------------------------------------------
bitrate_df = con.execute("""
    SELECT bitrate, COUNT(*) AS sessions
    FROM cdn
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ---------------------------------------------------------------------------
# Quality by ISP
# ---------------------------------------------------------------------------
isp_df = con.execute("""
    SELECT isp,
           COUNT(*) AS sessions,
           ROUND(AVG(rebuffer_ratio), 4) AS avg_rebuffer,
           ROUND(AVG(startup_time_ms), 0) AS avg_startup_ms
    FROM cdn
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# ---------------------------------------------------------------------------
# Build dashboard
# ---------------------------------------------------------------------------
fig = make_subplots(
    rows=4, cols=2,
    subplot_titles=(
        "", "",
        "Startup Time Distribution (ms)",
        "Daily Avg Rebuffer Ratio",
        "Error Type Breakdown",
        "Bitrate Distribution",
        "Rebuffer Ratio by ISP",
        "Avg Startup Time by CDN Node",
    ),
    specs=[
        [{"type": "indicator"}, {"type": "indicator"}],
        [{"type": "xy"}, {"type": "xy"}],
        [{"type": "xy"}, {"type": "domain"}],
        [{"type": "xy"}, {"type": "xy"}],
    ],
    row_heights=[0.12, 0.30, 0.28, 0.30],
    vertical_spacing=0.08,
    horizontal_spacing=0.12,
)

# KPI cards
fig.add_trace(go.Indicator(
    mode="number",
    value=kpis["median_startup_ms"],
    title={"text": "Median Startup (ms)"},
    number={"font": {"size": 36}, "suffix": " ms"},
), row=1, col=1)

fig.add_trace(go.Indicator(
    mode="number",
    value=kpis["error_rate_pct"],
    title={"text": "Error Rate"},
    number={"font": {"size": 36}, "suffix": "%"},
), row=1, col=2)

# Extra annotations
fig.add_annotation(
    text=f"<b>Avg Rebuffer</b><br>{kpis['avg_rebuffer_ratio']:.4f}",
    xref="paper", yref="paper", x=0.62, y=0.97,
    showarrow=False, font={"size": 14},
)
fig.add_annotation(
    text=f"<b>Total Sessions</b><br>{int(kpis['total_sessions']):,}",
    xref="paper", yref="paper", x=0.88, y=0.97,
    showarrow=False, font={"size": 14},
)

# Startup time histogram
fig.add_trace(go.Histogram(
    x=startup_df["startup_time_ms"],
    nbinsx=50,
    marker_color="#636EFA",
    showlegend=False,
), row=2, col=1)

# Daily rebuffer trend
fig.add_trace(go.Scatter(
    x=rebuffer_daily["day"],
    y=rebuffer_daily["avg_rebuffer"],
    mode="lines",
    line={"color": "#EF553B", "width": 2},
    showlegend=False,
), row=2, col=2)

# Error breakdown bar
errors_only = error_df[error_df["error_type"] != "no_error"]
fig.add_trace(go.Bar(
    x=errors_only["error_type"],
    y=errors_only["cnt"],
    marker_color="#FFA15A",
    showlegend=False,
), row=3, col=1)

# Bitrate pie
fig.add_trace(go.Pie(
    labels=bitrate_df["bitrate"],
    values=bitrate_df["sessions"],
    hole=0.4,
), row=3, col=2)

# Rebuffer by ISP
fig.add_trace(go.Bar(
    x=isp_df["isp"],
    y=isp_df["avg_rebuffer"],
    marker_color="#00CC96",
    showlegend=False,
), row=4, col=1)

# Startup by CDN node
fig.add_trace(go.Bar(
    y=cdn_node_df["cdn_node"],
    x=cdn_node_df["avg_startup_ms"],
    orientation="h",
    marker_color="#AB63FA",
    showlegend=False,
), row=4, col=2)

fig.update_layout(
    title={"text": "Streaming Quality Monitoring", "font": {"size": 24}, "x": 0.5},
    height=1300,
    template="plotly_white",
    margin={"t": 80, "b": 40},
)

output_path = OUTPUT_DIR / "streaming_quality.html"
fig.write_html(str(output_path), include_plotlyjs="cdn")

# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------
print("=" * 65)
print("  STREAMING QUALITY -- Summary Statistics")
print("=" * 65)
print(f"  Total Sessions       : {int(kpis['total_sessions']):,}")
print(f"  Median Startup Time  : {kpis['median_startup_ms']:.0f} ms")
print(f"  P95 Startup Time     : {kpis['p95_startup_ms']:.0f} ms")
print(f"  Avg Rebuffer Ratio   : {kpis['avg_rebuffer_ratio']:.4f}")
print(f"  Error Rate           : {kpis['error_rate_pct']:.2f}%")
print()
print("  Error Types:")
for _, row in error_df.iterrows():
    print(f"    {row['error_type']:25s} {row['cnt']:>8,}")
print()
print("  CDN Node Performance:")
print(f"  {'Node':15s} {'Sessions':>9s} {'AvgStart':>9s} {'Rebuffer':>9s} {'Err%':>6s}")
print("  " + "-" * 52)
for _, row in cdn_node_df.head(10).iterrows():
    print(f"  {row['cdn_node']:15s} {row['sessions']:>9,} {row['avg_startup_ms']:>8.0f}ms"
          f" {row['avg_rebuffer']:>9.4f} {row['error_pct']:>5.2f}%")
print("=" * 65)
print(f"\n  Dashboard saved to: {output_path}")
