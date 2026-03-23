# Module 10: BI & Dashboards

## Overview

Business Intelligence (BI) transforms raw data into actionable insights through
interactive dashboards, reports, and visualizations. In this module, we explore
how a taxi analytics platform can leverage BI tools and practices to monitor
fleet performance, understand trip patterns, and drive data-informed decisions
using NYC Taxi & Limousine Commission (TLC) data.

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

- **Consistency** -- "Average Fare Amount" means the same thing in every dashboard.
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
              │  dim_vendor   │
              └──────┬───────┘
                     │
 ┌────────────┐  ┌───▼──────────────┐  ┌─────────────────┐
 │ dim_zone   ├──┤ fact_taxi_trip    ├──┤ dim_payment_type │
 └────────────┘  └───┬──────────────┘  └─────────────────┘
                     │
              ┌──────▼───────┐
              │  dim_date     │
              └──────────────┘
```

### OLAP Operations

| Operation | Description | Example |
|-----------|-------------|---------|
| **Slice** | Filter one dimension to a single value | Borough = 'Manhattan' |
| **Dice** | Filter multiple dimensions | Borough IN ('Manhattan','Brooklyn') AND payment_type = 'Credit card' |
| **Drill-down** | Move from summary to detail | Year -> Quarter -> Month -> Day |
| **Roll-up** | Aggregate to a higher level | Zone -> Borough -> Citywide |
| **Pivot** | Rotate dimensions | Swap rows and columns |

### Pre-Aggregation

For large-scale taxi analytics platforms, pre-aggregate common query patterns:

- **Daily trips and revenue by borough** -- Powers the executive dashboard.
- **Hourly trip volume** -- Powers the demand pattern monitor.
- **Weekly zone-pair trip counts** -- Powers the geographic route analysis.

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
│ Trips  │AvgFare │AvgDist │Revenue │  AvgTip%      │
├────────┴────────┴────────┴────────┴───────────────┤
│  Primary Chart (Trend Line / Time Series)          │
├─────────────────────┬────────────────────────────-─┤
│  Secondary Chart 1  │  Secondary Chart 2           │
│  (Borough Breakdown)│  (Payment Distribution)      │
├─────────────────────┴─────────────────────────────-┤
│  Data Table (Detail / Drill-down)                  │
└────────────────────────────────────────────────────┘
```

---

## 5. KPIs for a Taxi Analytics Platform

### Trip Volume

| KPI | Definition | Formula |
|-----|------------|---------|
| **Daily Trips** | Total trips completed in a day | COUNT(*) WHERE pickup_date = today |
| **Trips per Hour** | Average hourly trip volume | COUNT(*) / 24 per day |
| **Peak Hour Trips** | Max trips in any single hour | MAX(hourly_count) |
| **Avg Passengers per Trip** | Average passenger count | AVG(passenger_count) |

### Revenue & Fares

| KPI | Definition | Formula |
|-----|------------|---------|
| **Total Revenue** | Sum of all trip fares | SUM(total_amount) |
| **Average Fare** | Mean fare per trip | AVG(fare_amount) |
| **Average Tip Percentage** | Tips as a fraction of fare | AVG(tip_amount / NULLIF(fare_amount, 0)) |
| **Revenue per Mile** | Revenue efficiency | SUM(total_amount) / SUM(trip_distance) |

### Trip Characteristics

| KPI | Definition | Formula |
|-----|------------|---------|
| **Avg Trip Distance** | Mean trip distance in miles | AVG(trip_distance) |
| **Avg Trip Duration** | Mean trip time in minutes | AVG(duration_minutes) |
| **Avg Speed** | Average trip speed | AVG(trip_distance / NULLIF(duration_hours, 0)) |
| **Short Trip Rate** | % of trips under 1 mile | COUNT(*) FILTER (WHERE trip_distance < 1) / COUNT(*) |

### Geographic Distribution

| KPI | Definition | Formula |
|-----|------------|---------|
| **Trips by Borough** | Trip count per pickup borough | COUNT(*) GROUP BY borough |
| **Top Pickup Zones** | Zones ranked by trip volume | COUNT(*) GROUP BY PULocationID ORDER BY COUNT(*) DESC |
| **Top Routes** | Most popular origin-destination pairs | COUNT(*) GROUP BY PULocationID, DOLocationID |
| **Cross-Borough Rate** | % of trips crossing boroughs | Trips where PU_borough != DO_borough / Total |

### Payment Patterns

| KPI | Definition | Formula |
|-----|------------|---------|
| **Payment Type Mix** | Distribution by payment method | COUNT(*) GROUP BY payment_type |
| **Credit Card Rate** | % of trips paid by credit card | COUNT(credit card) / COUNT(*) |
| **Avg Tip by Payment** | Average tip by payment type | AVG(tip_amount) GROUP BY payment_type |

### Weather Impact

| KPI | Definition | Formula |
|-----|------------|---------|
| **Rainy Day Trip Volume** | Trips on rainy vs. dry days | COUNT(*) on precipitation > 0 days |
| **Weather Fare Premium** | Fare increase during bad weather | AVG(fare) rainy / AVG(fare) dry |

---

## 6. Semantic Layer and Metrics Definitions

A well-defined semantic layer for a taxi analytics platform includes:

### Metric Specification Template

```yaml
- name: avg_fare_amount
  display_name: "Average Fare Amount ($)"
  description: "Mean fare_amount across all completed taxi trips"
  owner: analytics-team
  type: average
  field: fare_amount
  source_table: fact_taxi_trips
  filters:
    - "fare_amount > 0"
    - "trip_distance > 0"
  grain: daily
  dimensions:
    - borough
    - payment_type
    - vendor
  tags: [revenue, executive]
  sla:
    freshness: "6 hours"
    quality: "99.5%"
```

### Metric Types

| Type | Description | Example |
|------|-------------|---------|
| **count** | Count of records | Total trips |
| **sum** | Sum of a numeric field | Total revenue |
| **average** | Average of a numeric field | Avg fare amount |
| **ratio** | One metric divided by another | Tip percentage, credit card rate |
| **percentile** | Percentile of a numeric field | P95 trip duration |
| **period_over_period** | Change vs. previous period | WoW trip volume growth |

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
│   ├── geographic_analysis.py       # Exercise 3: Geographic heatmap & routes
│   └── metrics_definitions.py       # Exercise 4: Programmatic metrics layer
└── output/                          # Generated HTML dashboards
```

## Prerequisites

```bash
pip install duckdb plotly pandas pyyaml
```

## Running the Dashboards

```bash
# Generate all dashboards
cd module-10-bi-dashboards/solutions

python executive_dashboard.py
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
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
