# Lakehouse Playground: Data Engineering Practice Project

A comprehensive, hands-on data engineering practice project using **real NYC Taxi & Limousine Commission (TLC) data**. Build production-grade data pipelines from scratch — the same patterns used in Databricks, Snowflake, and modern data platforms.

## Architecture Overview

```
NYC TLC Data → Ingestion → Bronze → Silver → Gold → BI/Analytics
  (Real data)   (Airflow)   (Raw)   (Clean)  (Business)  (Dashboards)
```

## The Data

This project uses **real, publicly available data** from the NYC Taxi & Limousine Commission — not synthetic data. The raw data is naturally messy:

| Dataset | Format | ~Records | Real-World Messiness |
|---------|--------|----------|---------------------|
| Yellow taxi trips | Parquet | 3M+ /month | Null passenger counts, negative fares, zero-distance trips, rate_code=99 |
| Green taxi trips | Parquet | 80k+ /month | Different schema from yellow (ehail_fee, trip_type columns) |
| FHV trips (Uber/Lyft) | Parquet | 15M+ /month | Completely different schema, no fare breakdown |
| Taxi zone lookup | CSV | 265 | Borough/zone mapping for location IDs |
| Rate codes | CSV | 7 | Includes code 99 (unknown — a real data quality issue) |
| Payment types | CSV | 6 | Credit card, cash, no charge, dispute, voided |
| FHV bases | CSV | 4 | Uber (HV0003), Lyft (HV0005), Via, Juno |
| NYC daily weather | CSV | 365 | Central Park station — for enrichment joins |

**8 tables, 3 fact tables, 5 dimension tables, real data quality issues to solve.**

## Modules

| # | Module | Concepts | Tools |
|---|--------|----------|-------|
| 01 | [Docker & Postgres](module-01-docker-postgres/) | Containerization, relational DBs, SQL, bulk loading | Docker, Postgres |
| 02 | [Data Ingestion](module-02-data-ingestion/) | ETL basics, file formats, partitioning, deduplication | Python, Parquet, Pandas |
| 03 | [Airflow Orchestration](module-03-airflow-orchestration/) | DAGs, task dependencies, scheduling, backfills | Apache Airflow |
| 04 | [Data Warehouse](module-04-data-warehouse/) | Star schema, dimensional modeling, SQL analytics | DuckDB |
| 05 | [dbt Analytics Engineering](module-05-dbt-analytics/) | Models, tests, docs, seeds, snapshots, macros | dbt-core + dbt-duckdb |
| 06 | [Medallion Architecture](module-06-medallion-architecture/) | Bronze/Silver/Gold, Delta Lake, schema evolution | Delta Lake, PySpark, **Databricks patterns** |
| 07 | [Spark Processing](module-07-spark-processing/) | Batch processing, transformations, optimizations | PySpark, **Databricks context** |
| 08 | [Streaming](module-08-streaming/) | Event streams, windowing, watermarks, exactly-once | Structured Streaming |
| 09 | [Data Quality](module-09-data-quality/) | Expectations, anomaly detection, contracts, SLAs | Great Expectations, custom |
| 10 | [BI & Dashboards](module-10-bi-dashboards/) | Metrics, visualizations, stakeholder reporting | DuckDB, Python |

## Databricks Context

Modules 06-07 include **Databricks-specific patterns** throughout:
- Delta Lake table operations (CREATE, MERGE, TIME TRAVEL, OPTIMIZE)
- Unity Catalog references
- Delta Live Tables (DLT) equivalents
- Databricks SQL syntax
- Photon engine and adaptive query execution notes

The local PySpark code mirrors exactly what you'd write in Databricks notebooks.

## Key Concepts Covered

- **Idempotency**: Re-running pipelines doesn't duplicate data
- **Partitioning**: Organizing data by pickup date/borough for efficient queries
- **Schema Evolution**: Handling TLC schema changes across years (e.g., airport_fee added in 2019)
- **Backfilling**: Re-processing historical taxi data with execution_date
- **Slowly Changing Dimensions (SCD)**: Type 2 for zone boundary changes
- **Data Quality**: Real data quality issues — negative fares, null counts, impossible speeds
- **Orchestration vs Transformation**: Airflow = when/order, dbt/Spark = what/how

## Prerequisites

```bash
# Python 3.10+
python3 --version

# Docker & Docker Compose
docker --version
docker compose version

# pip packages
pip install -r requirements.txt
```

## Quick Start

```bash
# 1. Install base dependencies
pip install -r requirements.txt

# 2. Download real NYC taxi data (~200-400MB)
python scripts/download_data.py

# 3. Start with Module 01
cd module-01-docker-postgres
cat README.md
```

### Download Options

```bash
# Default: 3 months yellow + 1 month green + 1 month FHV
python scripts/download_data.py

# Quick mode: sample 100k rows per file (faster, ~20MB total)
python scripts/download_data.py --sample 100000

# Just yellow taxi (smallest download)
python scripts/download_data.py --yellow-only

# More data (6 months)
python scripts/download_data.py --months 6

# Skip the huge FHV (Uber/Lyft) file
python scripts/download_data.py --no-fhv
```

## Data Source

NYC Taxi & Limousine Commission Trip Record Data:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

This is one of the most widely-used public datasets in data engineering education and interviews. The data is real, big, and messy — exactly what you need to practice.
