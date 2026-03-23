# Lakehouse Playground: Data Engineering Practice Project

A comprehensive, hands-on data engineering practice project simulating a **podcast streaming platform** (inspired by Thmanyah). Build production-grade data pipelines from scratch.

## Architecture Overview

```
Sources → Ingestion → Bronze → Silver → Gold → BI/ML
           (Airflow)   (Raw)   (Clean)  (Business)  (Dashboards)
```

## Scenario

You are the **Data Engineering Team Manager** at a podcast streaming platform. Your platform generates:
- **Listening events** (user plays, pauses, skips, completes episodes)
- **User signups and profiles** (with messy real-world data quality issues)
- **Podcast & episode metadata** (changing titles, descriptions, categories)
- **CDN/streaming logs** (buffering events, bitrate switches, errors)
- **Ad impression & conversion events**

Your job: build the entire data platform from raw event ingestion to BI dashboards.

## Modules

| # | Module | Concepts | Tools |
|---|--------|----------|-------|
| 01 | [Docker & Postgres](module-01-docker-postgres/) | Containerization, relational DBs, SQL | Docker, Postgres |
| 02 | [Data Ingestion](module-02-data-ingestion/) | ETL basics, file formats, schema design | Python, Parquet, JSON, CSV |
| 03 | [Airflow Orchestration](module-03-airflow-orchestration/) | DAGs, task dependencies, scheduling, backfills | Apache Airflow |
| 04 | [Data Warehouse](module-04-data-warehouse/) | Star schema, partitioning, clustering, SQL analytics | DuckDB |
| 05 | [dbt Analytics Engineering](module-05-dbt-analytics/) | Models, tests, docs, seeds, snapshots, macros | dbt-core + dbt-duckdb |
| 06 | [Medallion Architecture](module-06-medallion-architecture/) | Bronze/Silver/Gold, Delta Lake, schema evolution | Delta Lake, PySpark |
| 07 | [Spark Processing](module-07-spark-processing/) | Batch processing, transformations, optimizations | PySpark |
| 08 | [Streaming](module-08-streaming/) | Event streams, windowing, exactly-once processing | Kafka (simulated) |
| 09 | [Data Quality](module-09-data-quality/) | Expectations, anomaly detection, SLAs, freshness | Great Expectations, custom |
| 10 | [BI & Dashboards](module-10-bi-dashboards/) | Metrics, visualizations, stakeholder reporting | Evidence/DuckDB |

## Key Concepts Covered

- **Idempotency**: Re-running pipelines doesn't duplicate data
- **Partitioning**: Organizing data by date/hour for efficient queries
- **Schema Evolution**: Handling source schema changes gracefully
- **Backfilling**: Re-processing historical data with execution_date
- **Slowly Changing Dimensions (SCD)**: Type 1 vs Type 2 for changing podcast metadata
- **Data Quality**: Expectations/checks between layers
- **Orchestration vs Transformation**: Airflow = when/order, dbt/Spark = what/how

## Prerequisites

```bash
# Python 3.10+
python3 --version

# Docker & Docker Compose
docker --version
docker compose version

# pip packages (installed per module)
pip install -r requirements.txt
```

## Quick Start

```bash
# 1. Install base dependencies
pip install -r requirements.txt

# 2. Generate the raw data (messy, realistic podcast data)
python scripts/generate_data.py

# 3. Start with Module 01
cd module-01-docker-postgres
cat README.md
```

## Data

The project uses **generated but realistic data** that simulates a podcast platform:
- Messy user data (duplicates, nulls, encoding issues, mixed formats)
- Listening events with realistic patterns (peak hours, skip rates, completion rates)
- Podcast metadata with changes over time (SCD scenarios)
- Ad events with attribution challenges
- CDN logs with quality metrics

All data generation is in `scripts/generate_data.py` — the data is intentionally imperfect to practice real-world data cleaning.
