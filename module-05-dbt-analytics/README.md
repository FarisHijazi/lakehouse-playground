# Module 05: dbt Analytics Engineering

## Why This Module Exists

You have raw taxi trip data sitting in parquet files. You have a warehouse (DuckDB) that can
read those files. But raw data is not analytics-ready data. Column names are inconsistent
across yellow and green taxi schemas, negative fares appear from disputes and adjustments,
timestamps need validation, and business logic (like "what counts as an airport trip?") lives
in someone's head instead of in version-controlled SQL.

dbt (data build tool) solves this. It is the transformation layer -- the "T" in ELT -- that
turns raw, messy source data into clean, tested, documented models that analysts and
dashboards can trust.

## What is dbt?

dbt is an open-source tool that lets you transform data inside your warehouse using SQL
SELECT statements. You write models (SQL files), and dbt handles the boilerplate: creating
tables/views, managing dependencies, running tests, and generating documentation.

**What dbt is:**
- A SQL-first transformation framework
- A way to apply software engineering practices (version control, testing, CI/CD) to analytics
- A dependency manager that builds models in the right order
- A documentation and lineage tool

**What dbt is not:**
- An extraction tool (it does not pull data from APIs or databases)
- A loading tool (it does not move data into your warehouse)
- An orchestrator (it does not schedule jobs -- that is Airflow's job)

## dbt in Production: Databricks and Other Warehouses

This module uses **dbt-duckdb** for local development, but the same patterns apply directly
to production data platforms. In real-world lakehouse architectures, dbt is commonly paired
with **Databricks** using the **dbt-databricks** adapter. The key differences:

| Aspect | dbt-duckdb (this module) | dbt-databricks (production) |
|--------|--------------------------|----------------------------|
| Warehouse | Local DuckDB file | Databricks SQL Warehouse or cluster |
| Storage | Local parquet files | Delta Lake on cloud object storage |
| Scale | Single machine, GBs | Distributed compute, TBs to PBs |
| Profile | `type: duckdb` | `type: databricks` |
| Catalog | DuckDB schemas | Unity Catalog (catalog.schema.table) |
| Materialization | table, view | table, view, incremental (Delta) |

Everything you learn here -- model layers, `ref()`, testing, documentation, macros,
snapshots -- transfers directly. The SQL is the same. The project structure is the same.
Only the connection profile changes.

Other common adapters include dbt-bigquery, dbt-snowflake, and dbt-redshift. The dbt
ecosystem is adapter-agnostic by design.

## dbt vs Traditional ETL

| Aspect | Traditional ETL | dbt (ELT approach) |
|--------|----------------|-------------------|
| Where transforms run | External tool (Informatica, SSIS) | Inside the warehouse |
| Language | Proprietary, Java, Python | SQL + Jinja |
| Testing | Manual, ad-hoc | Built-in schema and data tests |
| Documentation | Separate wiki (always stale) | Lives with the code, auto-generated |
| Dependencies | Manually managed | Automatic via `ref()` and `source()` |
| Version control | Often neglected | Git-native by design |
| Learning curve | Weeks of vendor training | SQL knowledge + a few hours |

## Core Concepts

### Models

A model is a SQL SELECT statement saved as a `.sql` file. dbt compiles it, wraps it in
CREATE TABLE or CREATE VIEW, and executes it in your warehouse.

Models are organized in layers:

```
models/
  staging/         -- 1:1 with source tables, light cleaning
  intermediate/    -- business logic joins, enrichment
  marts/           -- final tables for analysts and dashboards
```

**Staging models** (`stg_`):
- One model per source table
- Rename columns to consistent conventions
- Cast types, validate timestamps
- No business logic, no joins

**Intermediate models** (`int_`):
- Join staging models together
- Apply business logic (zone enrichment, derived metrics)
- Not exposed to end users directly

**Mart models** (`mart_`):
- Final, consumption-ready tables
- Aggregated, pivoted, or denormalized as needed
- Named for the business concept they represent

### The `ref()` Function

The single most important concept in dbt. Instead of hardcoding table names:

```sql
-- Bad: hardcoded reference
SELECT * FROM stg_yellow_trips

-- Good: dbt-managed reference
SELECT * FROM {{ ref('stg_yellow_trips') }}
```

`ref()` does two things:
1. Resolves the correct table/view name (handling schemas, environments)
2. Builds a dependency graph so dbt knows the execution order

### The `source()` Function

References raw data that dbt does not manage:

```sql
SELECT * FROM {{ source('raw', 'yellow_tripdata') }}
```

Sources are defined in YAML, with freshness checks, descriptions, and column docs.

### Tests

dbt has two kinds of tests:

**Schema tests** (declared in YAML):
```yaml
columns:
  - name: location_id
    tests:
      - unique
      - not_null
```

Built-in tests: `unique`, `not_null`, `accepted_values`, `relationships`.

**Data tests** (standalone SQL files in `tests/`):
```sql
-- tests/assert_positive_fare.sql
-- Any rows returned = test failure
SELECT *
FROM {{ ref('int_trips_enriched') }}
WHERE fare_amount < 0
```

### Documentation

dbt generates a static website with:
- Model descriptions and column-level docs
- A visual lineage graph (DAG) showing how models connect
- Test results and source freshness

```bash
dbt docs generate
dbt docs serve
```

### Seeds

CSV files that dbt loads into your warehouse as tables. Good for:
- Borough reference data
- Category mappings
- Static reference data

### Snapshots

Track changes over time using Slowly Changing Dimensions (SCD Type 2). If a taxi
zone changes its borough assignment or name, the snapshot preserves both the old
and new values with valid_from/valid_to timestamps.

### Macros

Reusable Jinja functions. Write once, use across models:

```sql
{% macro clean_fare(column_name) %}
    case
        when {{ column_name }} < 0 then 0
        else {{ column_name }}
    end
{% endmacro %}
```

## dbt Best Practices

### Naming Conventions
- Staging: `stg_{source_table}` (e.g., `stg_yellow_trips`)
- Intermediate: `int_{concept}` (e.g., `int_trips_enriched`)
- Marts: `mart_{concept}` (e.g., `mart_daily_metrics`)

### Model Configuration
- Staging models: materialized as `view` (lightweight, always fresh)
- Intermediate models: materialized as `view` or `ephemeral`
- Mart models: materialized as `table` (pre-computed for performance)

### General Rules
1. Every model uses `ref()` or `source()` -- never hardcoded table names
2. Every model has a description in its schema.yml
3. Primary keys have `unique` and `not_null` tests
4. Foreign keys have `relationships` tests
5. CTEs over subqueries for readability
6. One model per file, one file per model

## How dbt Fits in the Modern Data Stack

```
                    +-----------+
  Sources -------> |  Ingestion | (Fivetran, Airbyte, custom scripts)
                    +-----------+
                         |
                         v
                    +-----------+
                    | Warehouse | (DuckDB, Databricks, BigQuery, Snowflake)
                    +-----------+
                         |
                         v
                    +-----------+
                    |    dbt    | <-- YOU ARE HERE
                    +-----------+
                         |
                         v
                    +-----------+
                    |    BI     | (Metabase, Looker, Tableau)
                    +-----------+
```

dbt sits between ingestion and consumption. Raw data lands in the warehouse first (EL),
then dbt transforms it in place (T). This is the ELT pattern.

## The Data

You will work with real NYC Taxi and Limousine Commission (TLC) trip data:

| Dataset | Format | Description |
|---------|--------|-------------|
| `yellow_tripdata_*.parquet` | Parquet | Yellow taxi trip records (Manhattan-centric) |
| `green_tripdata_*.parquet` | Parquet | Green taxi trip records (outer boroughs) |
| `fhvhv_tripdata_*.parquet` | Parquet | For-hire vehicles: Uber, Lyft (high volume) |
| `taxi_zone_lookup.csv` | CSV | 265 taxi zones across 5 boroughs |
| `nyc_weather_2023.csv` | CSV | Daily weather from Central Park NOAA station |
| `vendors.csv` | CSV | Vendor ID to name mapping |
| `rate_codes.csv` | CSV | Rate code descriptions |
| `payment_types.csv` | CSV | Payment type descriptions |
| `fhv_bases.csv` | CSV | FHV base license numbers and company names |

### Known Data Issues (Your dbt Models Must Handle These)
- Negative fare amounts from disputes and adjustments
- Zero-distance trips (meter not engaged or very short trips)
- Trips with dropoff before pickup (timestamp errors)
- Passenger counts of 0 or unreasonably high values
- Location IDs not in the zone lookup (zone 264, 265 edge cases)
- Schema differences between yellow and green taxi data
- FHV data has a completely different fare structure

## Getting Started

### Prerequisites
- Python 3.9+
- Install dbt-duckdb: `pip install dbt-duckdb`

### Quick Start

```bash
# 1. Navigate to this module
cd module-05-dbt-analytics

# 2. Verify dbt is installed
dbt --version

# 3. Test the connection
dbt debug

# 4. Load seed data
dbt seed

# 5. Run all models
dbt run

# 6. Run all tests
dbt test

# 7. Generate docs
dbt docs generate
dbt docs serve
```

### Project Structure

```
module-05-dbt-analytics/
  dbt_project.yml          -- Project configuration
  profiles.yml             -- Connection profile (DuckDB)
  models/
    staging/               -- 1:1 source cleaning
      stg_yellow_trips.sql
      stg_green_trips.sql
      stg_fhv_trips.sql
      stg_zones.sql
      stg_weather.sql
      schema.yml           -- Tests and docs for staging models
    intermediate/          -- Business logic
      int_trips_enriched.sql
      int_daily_trip_summary.sql
      schema.yml
    marts/                 -- Analytics-ready tables
      mart_daily_metrics.sql
      mart_zone_performance.sql
      mart_hourly_patterns.sql
      mart_weather_impact.sql
      schema.yml
  snapshots/
    scd_zones.sql          -- SCD Type 2 for zone metadata
  macros/
    generate_schema_name.sql
    date_spine.sql
    clean_fare.sql
  tests/
    assert_no_orphaned_trips.sql
    assert_positive_fare.sql
    assert_valid_trip_duration.sql
  seeds/
    borough_info.csv
  exercises.md             -- 10 guided exercises + bonus challenges
  README.md
```

## Next Steps

Work through the exercises in `exercises.md`. They are ordered from setup through
building all three model layers, testing, snapshots, macros, and documentation.
