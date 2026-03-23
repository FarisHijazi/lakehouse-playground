# Module 05: dbt Analytics Engineering

## Why This Module Exists

You have raw data sitting in files. You have a warehouse (DuckDB) that can read those files.
But raw data is not analytics-ready data. Column names are inconsistent, dates come in three
different formats, duplicates lurk in the listening events, and business logic (like "what
counts as an active user?") lives in someone's head instead of in version-controlled SQL.

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
  intermediate/    -- business logic joins, sessionization
  marts/           -- final tables for analysts and dashboards
```

**Staging models** (`stg_`):
- One model per source table
- Rename columns to consistent conventions
- Cast types, parse dates, trim strings
- Deduplicate if needed
- No business logic, no joins

**Intermediate models** (`int_`):
- Join staging models together
- Apply business logic (sessionization, enrichment)
- Not exposed to end users directly

**Mart models** (`mart_`):
- Final, consumption-ready tables
- Aggregated, pivoted, or denormalized as needed
- Named for the business concept they represent

### The `ref()` Function

The single most important concept in dbt. Instead of hardcoding table names:

```sql
-- Bad: hardcoded reference
SELECT * FROM stg_users

-- Good: dbt-managed reference
SELECT * FROM {{ ref('stg_users') }}
```

`ref()` does two things:
1. Resolves the correct table/view name (handling schemas, environments)
2. Builds a dependency graph so dbt knows the execution order

### The `source()` Function

References raw data that dbt does not manage:

```sql
SELECT * FROM {{ source('raw', 'users') }}
```

Sources are defined in YAML, with freshness checks, descriptions, and column docs.

### Tests

dbt has two kinds of tests:

**Schema tests** (declared in YAML):
```yaml
columns:
  - name: user_id
    tests:
      - unique
      - not_null
```

Built-in tests: `unique`, `not_null`, `accepted_values`, `relationships`.

**Data tests** (standalone SQL files in `tests/`):
```sql
-- tests/assert_positive_listen_duration.sql
-- Any rows returned = test failure
SELECT *
FROM {{ ref('stg_listening_events') }}
WHERE listened_seconds < 0
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
- Country code lookups
- Category mappings
- Static reference data

### Snapshots

Track changes over time using Slowly Changing Dimensions (SCD Type 2). If a podcast
changes its category, the snapshot preserves both the old and new values with
valid_from/valid_to timestamps.

### Macros

Reusable Jinja functions. Write once, use across models:

```sql
{% macro cents_to_dollars(column_name) %}
  ({{ column_name }} / 100.0)::numeric(10,2)
{% endmacro %}
```

## dbt Best Practices

### Naming Conventions
- Staging: `stg_{source}_{table}` (e.g., `stg_raw_users`)
- Intermediate: `int_{concept}` (e.g., `int_listens_enriched`)
- Marts: `mart_{concept}` (e.g., `mart_daily_listens`)

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
                    | Warehouse | (DuckDB, BigQuery, Snowflake, Redshift)
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

You will work with the same podcast platform dataset from previous modules:

| Dataset | Format | Records | Description |
|---------|--------|---------|-------------|
| `users.csv` | CSV | 5,000 | User profiles (messy dates, mixed genders) |
| `podcasts.json` | JSON | 10 | Podcast metadata (Arabic/English names) |
| `episodes.json` | JSON | 784 | Episode details (duration, season) |
| `listening_events/` | JSONL | ~200k | Daily event files across years |
| `ad_events.json` | JSON | 18,071 | Ad impressions and revenue |
| `cdn_logs.csv` | CSV | 50,000 | CDN delivery metrics |

### Known Data Issues (Your dbt Models Must Handle These)
- `users.csv`: signup_date in three formats (YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY)
- `users.csv`: gender values are inconsistent (m/male/M, f/female/F)
- `users.csv`: missing cities (nulls and empty strings)
- `listening_events/`: duplicate event_ids across files
- `listening_events/`: some listened_seconds values are negative or absurdly large

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

# 4. Run all models
dbt run

# 5. Run all tests
dbt test

# 6. Generate docs
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
      stg_users.sql
      stg_podcasts.sql
      stg_episodes.sql
      stg_listening_events.sql
      stg_ad_events.sql
      schema.yml           -- Tests and docs for staging models
    intermediate/          -- Business logic
      int_listens_enriched.sql
      int_user_sessions.sql
      schema.yml
    marts/                 -- Analytics-ready tables
      mart_daily_listens.sql
      mart_podcast_performance.sql
      mart_user_cohorts.sql
      mart_ad_revenue.sql
      schema.yml
  snapshots/
    scd_podcasts.sql       -- SCD Type 2 for podcast metadata
  macros/
    generate_schema_name.sql
    date_spine.sql
    clean_date.sql
  tests/
    assert_positive_listen_duration.sql
    assert_no_orphaned_listens.sql
    assert_revenue_not_negative.sql
  seeds/
    country_codes.csv
  exercises.md             -- 10 guided exercises + bonus challenges
  README.md
```

## Next Steps

Work through the exercises in `exercises.md`. They are ordered from setup through
building all three model layers, testing, snapshots, macros, and documentation.
