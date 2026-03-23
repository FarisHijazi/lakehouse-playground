#!/usr/bin/env python3
"""
Module 10 -- Exercise 3: Podcast Performance Deep-Dive
=======================================================
Drill-down analysis of podcast and episode performance: top podcasts,
top episodes, trends over time, completion rates, and duration distribution.

Outputs: ../output/podcast_performance.html
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
# Load & register data
# ---------------------------------------------------------------------------
con.execute(f"""
    CREATE TABLE events AS
    SELECT * FROM read_json_auto('{DATA_DIR}/listening_events/*.jsonl')
""")
con.execute(f"""
    CREATE TABLE episodes AS
    SELECT * FROM read_json_auto('{DATA_DIR}/episodes.json')
""")
con.execute(f"""
    CREATE TABLE podcasts AS
    SELECT * FROM read_json_auto('{DATA_DIR}/podcasts.json')
""")

# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------
# Top 10 podcasts by total plays
top_podcasts = con.execute("""
    SELECT p.name_en AS podcast, COUNT(*) AS total_plays
    FROM events e
    JOIN episodes ep ON e.episode_id = ep.episode_id
    JOIN podcasts p  ON ep.podcast_id = p.podcast_id
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
""").df()

# Top 10 episodes by unique listeners
top_episodes = con.execute("""
    SELECT ep.title AS episode,
           p.name_en AS podcast,
           COUNT(DISTINCT e.user_id) AS unique_listeners
    FROM events e
    JOIN episodes ep ON e.episode_id = ep.episode_id
    JOIN podcasts p  ON ep.podcast_id = p.podcast_id
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 10
""").df()

# Monthly listens for top 5 podcasts (stacked area)
top5_ids = con.execute("""
    SELECT ep.podcast_id, COUNT(*) AS cnt
    FROM events e
    JOIN episodes ep ON e.episode_id = ep.episode_id
    GROUP BY 1 ORDER BY 2 DESC LIMIT 5
""").df()["podcast_id"].tolist()

monthly_top5 = con.execute(f"""
    SELECT DATE_TRUNC('month', CAST(e.timestamp AS TIMESTAMP)) AS month,
           p.name_en AS podcast,
           COUNT(*) AS listens
    FROM events e
    JOIN episodes ep ON e.episode_id = ep.episode_id
    JOIN podcasts p  ON ep.podcast_id = p.podcast_id
    WHERE ep.podcast_id IN ({','.join("'" + pid + "'" for pid in top5_ids)})
    GROUP BY 1, 2
    ORDER BY 1
""").df()

# Completion rate by podcast
completion_by_podcast = con.execute("""
    SELECT p.name_en AS podcast,
           ROUND(COUNT(*) FILTER (WHERE e.event_type = 'complete') * 100.0 /
                 NULLIF(COUNT(*), 0), 1) AS completion_pct
    FROM events e
    JOIN episodes ep ON e.episode_id = ep.episode_id
    JOIN podcasts p  ON ep.podcast_id = p.podcast_id
    GROUP BY 1
    ORDER BY 2 DESC
""").df()

# Listen duration distribution
duration_df = con.execute("""
    SELECT listened_seconds / 60.0 AS listen_minutes
    FROM events
    WHERE listened_seconds > 0
""").df()

# Summary table
summary_df = con.execute("""
    SELECT p.name_en AS podcast,
           COUNT(*) AS total_plays,
           COUNT(DISTINCT e.user_id) AS unique_listeners,
           ROUND(AVG(e.listened_seconds) / 60.0, 1) AS avg_duration_min,
           ROUND(COUNT(*) FILTER (WHERE e.event_type = 'complete') * 100.0 /
                 NULLIF(COUNT(*), 0), 1) AS completion_pct
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
    subplot_titles=(
        "Top 10 Podcasts by Plays",
        "Top 10 Episodes by Unique Listeners",
        "Monthly Listens -- Top 5 Podcasts",
        "Completion Rate by Podcast",
        "Listen Duration Distribution (minutes)",
    ),
    specs=[
        [{"type": "xy"}, {"type": "xy"}],
        [{"type": "xy", "colspan": 2}, None],
        [{"type": "xy"}, {"type": "xy"}],
    ],
    row_heights=[0.35, 0.35, 0.30],
    vertical_spacing=0.10,
    horizontal_spacing=0.12,
)

# Top 10 podcasts
fig.add_trace(go.Bar(
    y=top_podcasts["podcast"],
    x=top_podcasts["total_plays"],
    orientation="h",
    marker_color="#636EFA",
    name="Plays",
    showlegend=False,
), row=1, col=1)

# Top 10 episodes
fig.add_trace(go.Bar(
    y=top_episodes["episode"].str[:35],
    x=top_episodes["unique_listeners"],
    orientation="h",
    marker_color="#EF553B",
    name="Listeners",
    showlegend=False,
), row=1, col=2)

# Monthly listens stacked area
colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A"]
for i, podcast_name in enumerate(monthly_top5["podcast"].unique()):
    subset = monthly_top5[monthly_top5["podcast"] == podcast_name]
    fig.add_trace(go.Scatter(
        x=subset["month"],
        y=subset["listens"],
        mode="lines",
        stackgroup="one",
        name=podcast_name,
        line={"color": colors[i % len(colors)]},
    ), row=2, col=1)

# Completion rate
fig.add_trace(go.Bar(
    x=completion_by_podcast["podcast"],
    y=completion_by_podcast["completion_pct"],
    marker_color="#00CC96",
    name="Completion %",
    showlegend=False,
), row=3, col=1)

# Duration histogram
fig.add_trace(go.Histogram(
    x=duration_df["listen_minutes"],
    nbinsx=40,
    marker_color="#AB63FA",
    name="Duration",
    showlegend=False,
), row=3, col=2)

fig.update_layout(
    title={"text": "Podcast Performance Deep-Dive", "font": {"size": 24}, "x": 0.5},
    height=1100,
    template="plotly_white",
    margin={"t": 80, "b": 40},
)

# Reverse y-axes for horizontal bar charts so #1 is on top
fig.update_yaxes(autorange="reversed", row=1, col=1)
fig.update_yaxes(autorange="reversed", row=1, col=2)

output_path = OUTPUT_DIR / "podcast_performance.html"
fig.write_html(str(output_path), include_plotlyjs="cdn")

# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------
print("=" * 70)
print("  PODCAST PERFORMANCE -- Summary")
print("=" * 70)
print("\n  Top 10 Podcasts by Total Plays:")
for _, row in top_podcasts.iterrows():
    print(f"    {row['podcast']:30s} {row['total_plays']:>8,} plays")

print(f"\n  Podcast Summary Table:")
print(f"  {'Podcast':30s} {'Plays':>8s} {'Uniq':>6s} {'AvgMin':>7s} {'Compl%':>7s}")
print("  " + "-" * 60)
for _, row in summary_df.iterrows():
    print(f"  {row['podcast']:30s} {row['total_plays']:>8,} {row['unique_listeners']:>6,}"
          f" {row['avg_duration_min']:>7.1f} {row['completion_pct']:>6.1f}%")

print("=" * 70)
print(f"\n  Dashboard saved to: {output_path}")
