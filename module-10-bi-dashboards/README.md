# Module 10: BI & Dashboards

## Overview

Business Intelligence (BI) transforms raw data into actionable insights through
interactive dashboards, reports, and visualizations. In this module, we explore
how a podcast platform like Thmanyah can leverage BI tools and practices to
monitor performance, understand listener behavior, and drive data-informed
decisions.

---

## 1. The BI Tools Landscape

### Open-Source / Self-Hosted

| Tool | Strengths | Best For |
|------|-----------|----------|
| **Apache Superset** | SQL-native, rich chart library, role-based access | Teams comfortable with SQL who want a free, extensible solution |
| **Metabase** | Zero-config setup, friendly UI, embedded analytics | Startups and small teams wanting quick self-service analytics |
| **Evidence** | Code-as-dashboards (Markdown + SQL), version-controlled | Engineering teams who prefer dashboards-as-code |
| **Redash** | Lightweight, multi-datasource, alert system | Ad-hoc querying and simple dashboards |

### Commercial / Enterprise

| Tool | Strengths | Best For |
|------|-----------|----------|
| **Looker (Google)** | LookML modeling layer, governed metrics, Git integration | Organizations needing a strong semantic layer |
| **Tableau** | Best-in-class visualizations, drag-and-drop, Tableau Prep | Analyst-heavy teams focused on visual exploration |
| **Power BI (Microsoft)** | Deep Office 365 integration, DAX language, affordable | Microsoft-centric enterprises |
| **Sigma Computing** | Spreadsheet-like interface on the warehouse | Finance and ops teams migrating from Excel |
| **Preset** | Managed Superset with collaboration features | Teams wanting Superset without operational overhead |

### How to Choose

1. **Team SQL fluency** -- SQL-native tools (Superset, Evidence) vs. drag-and-drop (Tableau, Power BI).
2. **Governance needs** -- Looker's LookML or dbt's metrics layer for single source of truth.
3. **Deployment model** -- Self-hosted (Superset, Metabase) vs. SaaS (Looker, Tableau Cloud).
4. **Embedding requirements** -- Metabase and Looker excel at embedded analytics.
5. **Budget** -- Open-source tools can start at $0; enterprise tools range from $10--$70/user/month.

---

## 2. The Metrics Layer Concept

A **metrics layer** (also called a **semantic layer**) sits between the data
warehouse and BI tools, providing a single, governed definition for every
business metric.

```
 ┌─────────────┐
 │  Data Lake   │
 │  / Warehouse │
 └──────┬───────┘
        │
 ┌──────▼───────┐
 │ Metrics Layer │  ← Canonical definitions (SQL + metadata)
 │  (Semantic)   │
 └──────┬───────┘
        │
 ┌──────▼───────┐
 │  BI Tools /   │  ← Dashboards, Reports, Ad-hoc queries
 │  Applications │
 └───────────────┘
```

### Why It Matters

- **Consistency** -- "Monthly Active Users" means the same thing in every dashboard.
- **DRY** -- Define once, reuse across Superset, Looker, Python notebooks, and APIs.
- **Governance** -- Owners, SLAs, and freshness guarantees per metric.

### Tools for the Metrics Layer

- **dbt Metrics** / **MetricFlow** -- Define metrics in YAML alongside dbt models.
- **Looker LookML** -- Dimensions, measures, and explores as code.
- **Cube.js** -- Open-source headless BI with a REST/GraphQL API.
- **Minerva (Airbnb)** -- Internal metrics platform (inspiration for the community).

---

## 3. OLAP Cubes and Dimensional Modeling for BI

### Dimensional Modeling Recap

BI dashboards are most effective when backed by a well-modeled star or
snowflake schema:

```
              ┌──────────────┐
              │  dim_podcast  │
              └──────┬───────┘
                     │
 ┌────────────┐  ┌───▼──────────────┐  ┌────────────┐
 │  dim_user   ├──┤ fact_listen_event ├──┤ dim_episode │
 └────────────┘  └───┬──────────────┘  └────────────┘
                     │
              ┌──────▼───────┐
              │  dim_date     │
              └──────────────┘
```

### OLAP Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| **Slice** | Filter one dimension to a single value | Country = 'SA' |
| **Dice** | Filter multiple dimensions | Country IN ('SA','AE') AND Platform = 'ios' |
| **Drill-down** | Move from summary to detail | Year → Quarter → Month → Day |
| **Roll-up** | Aggregate to a higher level | Episode → Podcast → Category |
| **Pivot** | Rotate dimensions | Swap rows and columns |

### Pre-Aggregation

For large-scale podcast platforms, pre-aggregate common query patterns:

- **Daily listens per podcast** -- Powers the executive dashboard.
- **Hourly CDN metrics** -- Powers the streaming quality monitor.
- **Weekly cohort retention** -- Powers the engagement analysis.

---

## 4. Building Effective Dashboards

### Design Principles

1. **Start with questions, not charts.** What decision will this dashboard inform?
2. **Inverted pyramid.** Top-level KPIs at the top; drill-down details below.
3. **5-second rule.** A viewer should grasp the headline insight within 5 seconds.
4. **Consistent color encoding.** Same color = same meaning across all charts.
5. **Minimize chart junk.** Remove gridlines, borders, and 3D effects.
6. **Provide context.** Show targets, benchmarks, or period-over-period comparisons.
7. **Mobile-friendly.** Many stakeholders view dashboards on phones.

### Dashboard Layout Pattern

```
┌──────────────────────────────────────────────────┐
│  Title / Date Range Filter                        │
├────────┬────────┬────────┬────────┬───────────────┤
│  KPI 1 │  KPI 2 │  KPI 3 │  KPI 4 │   KPI 5      │
│  DAU   │ Compl% │ AvgMin │ Revenue│   Churn       │
├────────┴────────┴────────┴────────┴───────────────┤
│  Primary Chart (Trend Line / Time Series)          │
├─────────────────────┬────────────────────────────-─┤
│  Secondary Chart 1  │  Secondary Chart 2           │
│  (Breakdown)        │  (Distribution)              │
├─────────────────────┴─────────────────────────────-┤
│  Data Table (Detail / Drill-down)                  │
└────────────────────────────────────────────────────┘
```

---

## 5. KPIs for a Podcast Platform

### User Activity

| KPI | Definition | Formula |
|-----|------------|---------|
| **DAU** | Daily Active Users | COUNT(DISTINCT user_id) WHERE event_date = today |
| **WAU** | Weekly Active Users | COUNT(DISTINCT user_id) WHERE event_date >= today - 7 |
| **MAU** | Monthly Active Users | COUNT(DISTINCT user_id) WHERE event_date >= today - 30 |
| **DAU/MAU Ratio** | Stickiness | DAU / MAU (higher = more engaged) |

### Listener Retention & Churn

| KPI | Definition | Formula |
|-----|------------|---------|
| **D7 Retention** | % of users active 7 days after signup | Users active on day 7 / Users signed up on day 0 |
| **Monthly Churn** | % of users lost per month | (MAU_prev - Retained) / MAU_prev |
| **Resurrection Rate** | Churned users who returned | Reactivated / Previously churned |

### Content Engagement

| KPI | Definition | Formula |
|-----|------------|---------|
| **Avg Listen Duration** | Average seconds listened per session | AVG(listened_seconds) |
| **Completion Rate** | % of episode fully listened | COUNT(complete events) / COUNT(play events) |
| **Episodes per User per Day** | Content consumption depth | COUNT(DISTINCT episode_id) / COUNT(DISTINCT user_id) |

### Top Content

| KPI | Definition | Formula |
|-----|------------|---------|
| **Top Podcasts** | Podcasts ranked by total listens | GROUP BY podcast_id ORDER BY COUNT(*) DESC |
| **Trending Episodes** | Episodes with highest growth rate | Compare current vs. previous period listens |
| **Category Mix** | Distribution of listens across categories | COUNT(*) per category / total COUNT(*) |

### Advertising

| KPI | Definition | Formula |
|-----|------------|---------|
| **Ad Revenue** | Total ad revenue in SAR | SUM(revenue_sar) |
| **Fill Rate** | % of ad slots filled | Impressions / Available slots |
| **CPM** | Cost per thousand impressions | Revenue / Impressions * 1000 |
| **Click-Through Rate** | % of impressions that got clicks | Clicks / Impressions |
| **Ad Completion Rate** | % of ads fully watched | Completed / Impressions |

### Streaming Quality

| KPI | Definition | Formula |
|-----|------------|---------|
| **Rebuffer Rate** | % of sessions with buffering | Sessions with buffer_events > 0 / Total sessions |
| **Avg Startup Time** | Time to first audio (ms) | AVG(startup_time_ms) |
| **Error Rate** | % of sessions with errors | Sessions with error_type IS NOT NULL / Total |
| **P95 Startup Time** | 95th percentile startup latency | PERCENTILE_CONT(0.95) of startup_time_ms |

### Geographic Distribution

| KPI | Definition | Formula |
|-----|------------|---------|
| **Listeners by Country** | User count per country | COUNT(DISTINCT user_id) GROUP BY country |
| **Top Cities** | City-level concentration | COUNT(DISTINCT user_id) GROUP BY city |
| **Regional Growth** | Period-over-period by region | Compare current vs. previous period per region |

---

## 6. Semantic Layer and Metrics Definitions

A well-defined semantic layer for a podcast platform includes:

### Metric Specification Template

```yaml
- name: monthly_active_users
  display_name: "Monthly Active Users (MAU)"
  description: "Distinct users with at least one listening event in the past 30 days"
  owner: growth-team
  type: count_distinct
  field: user_id
  source_table: fact_listen_events
  filters:
    - "event_date >= CURRENT_DATE - INTERVAL '30 days'"
  grain: daily
  dimensions:
    - country
    - platform
    - subscription_type
  tags: [growth, engagement, executive]
  sla:
    freshness: "6 hours"
    quality: "99.5%"
```

### Metric Types

| Type | Description | Example |
|------|-------------|---------|
| **count_distinct** | Unique count of a field | MAU, DAU |
| **sum** | Sum of a numeric field | Total revenue |
| **average** | Average of a numeric field | Avg listen duration |
| **ratio** | One metric divided by another | Completion rate, CTR |
| **cumulative** | Running total over time | Cumulative revenue |
| **period_over_period** | Change vs. previous period | MoM growth |

---

## 7. Self-Service Analytics

### Principles

1. **Curated data catalog** -- Users discover tables, columns, and metrics through a searchable catalog.
2. **Guardrails, not gates** -- Let analysts query freely but surface validated metrics prominently.
3. **Saved explorations** -- Encourage sharing of queries and dashboards across teams.
4. **Training and documentation** -- Metrics definitions, SQL snippets, and video walkthroughs.
5. **Feedback loops** -- Let consumers flag incorrect data or request new metrics.

### Maturity Model

| Level | Description |
|-------|-------------|
| **L1 -- Ad-hoc** | Analysts write SQL; no shared definitions |
| **L2 -- Centralized** | BI team builds dashboards on request |
| **L3 -- Self-service** | Governed metrics layer; business users explore freely |
| **L4 -- Embedded** | Analytics embedded in product (recommendations, alerts) |

---

## Project Structure

```
module-10-bi-dashboards/
├── README.md                        # This file
├── exercises.md                     # Hands-on exercises
├── metrics/
│   └── metrics.yml                  # YAML metric definitions
├── solutions/
│   ├── executive_dashboard.py       # Exercise 2: Executive summary
│   ├── podcast_performance.py       # Exercise 3: Podcast deep-dive
│   ├── user_engagement.py           # Exercise 4: User engagement
│   ├── ad_revenue.py                # Exercise 5: Ad revenue
│   ├── streaming_quality.py         # Exercise 6: Streaming quality
│   ├── geographic_analysis.py       # Exercise 7: Geographic heatmap
│   └── metrics_definitions.py       # Exercise 8: Programmatic metrics layer
└── output/                          # Generated HTML dashboards
```

## Prerequisites

```bash
pip install duckdb plotly pandas
```

## Running the Dashboards

```bash
# Generate all dashboards
cd module-10-bi-dashboards/solutions

python executive_dashboard.py
python podcast_performance.py
python user_engagement.py
python ad_revenue.py
python streaming_quality.py
python geographic_analysis.py
python metrics_definitions.py

# Open any dashboard
open ../output/executive_dashboard.html
```

---

## Further Reading

- [The Metrics Layer (Benn Stancil)](https://benn.substack.com/p/metrics-layer)
- [dbt MetricFlow documentation](https://docs.getdbt.com/docs/build/about-metricflow)
- [Apache Superset documentation](https://superset.apache.org/)
- [Kimball Group -- Dimensional Modeling Techniques](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [Evidence.dev -- BI as Code](https://evidence.dev/)
