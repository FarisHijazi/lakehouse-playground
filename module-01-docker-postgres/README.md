# Module 01: Docker & Postgres

## Why This Module Exists

Every data pipeline starts somewhere. Before you build a lakehouse, before you orchestrate
workflows, before you write a single transformation — you need to know how to stand up a
database and load data into it. This module teaches that foundation.

## Why Docker Matters for Data Engineering

In production data engineering, "it works on my machine" is not acceptable. A pipeline that
breaks because someone has a different Postgres version, different locale settings, or missing
system libraries is a pipeline that will fail at 3 AM.

Docker solves this by packaging your database, your tools, and your configuration into
containers that run identically everywhere — your laptop, your colleague's laptop, CI/CD,
staging, production.

**What you will learn:**
- How to define infrastructure as code with `docker-compose.yml`
- How to initialize a database with schema scripts on first boot
- How containers isolate dependencies and make environments reproducible

## Why Postgres for Data Engineers

Postgres is not just "a relational database." It is the starting point for most analytical
workloads and the engine behind many data tools (Airflow metadata, dbt targets, feature
stores). Understanding Postgres deeply — its type system, indexing strategies, query planner,
and COPY protocol — makes you effective across the entire data stack.

**What you will learn:**
- Schema design for analytical workloads (fact tables + dimension tables)
- Proper data types, constraints, and indexing
- The COPY protocol for bulk loading (orders of magnitude faster than INSERT)
- Reading query plans with EXPLAIN ANALYZE
- Creating views to encapsulate business logic

## The Data

You will work with **real NYC Taxi & Limousine Commission (TLC) trip data**:

| Dataset | Format | ~Records | Description |
|---------|--------|----------|-------------|
| Yellow taxi trips | Parquet | 3M+/month | Main taxi trips — the core fact table |
| Green taxi trips | Parquet | 80k+/month | Borough taxis (different schema!) |
| FHV trips (Uber/Lyft) | Parquet | 15M+/month | For-hire vehicles — completely different schema |
| Taxi zone lookup | CSV | 265 | Borough, zone name, service zone |
| Vendors | CSV | 2 | CMT, VeriFone |
| Rate codes | CSV | 7 | Standard, JFK, Newark, etc. |
| Payment types | CSV | 6 | Credit card, cash, dispute, etc. |
| FHV bases | CSV | 4 | Uber, Lyft, Via, Juno |
| NYC weather | CSV | 365 | Daily weather from Central Park |

**Real data quality issues you'll encounter:**
- `passenger_count` is a FLOAT (yes, really) and often null or zero
- Negative `fare_amount` and `total_amount` values
- Trips with zero distance but non-zero fares
- `rate_code_id` = 99 (unknown)
- Pickup datetime after dropoff datetime
- `store_and_fwd_flag` inconsistencies across vendors

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
- Python 3.10+ with `psycopg2-binary` and `pyarrow` (`pip install psycopg2-binary pyarrow`)

### Quick Start

```bash
# 0. Download the data (if you haven't already)
cd ..
python scripts/download_data.py
cd module-01-docker-postgres

# 1. Start the containers
docker compose up -d

# 2. Wait for Postgres to be ready (about 5 seconds)
docker compose logs postgres

# 3. Load the data
python load_data.py

# 4. Connect to pgAdmin at http://localhost:8080
#    Email: admin@lakehouse.dev / Password: admin

# 5. Or connect directly via psql
docker compose exec postgres psql -U lakehouse -d nyc_taxi
```

## Exercises

Work through `exercises.md` in order. Each exercise builds on the previous one.

## File Structure

```
module-01-docker-postgres/
  docker-compose.yml   # Container definitions (Postgres + pgAdmin)
  init.sql             # Schema — 8 tables (auto-runs on first boot)
  load_data.py         # Data loading script (parquet + CSV → Postgres)
  exercises.md         # Hands-on exercises
  README.md            # This file
```

## When You Are Done

You should be able to:
1. Explain why Docker matters for reproducible data environments
2. Design a Postgres schema for analytical workloads with fact + dimension tables
3. Bulk-load data from Parquet and CSV using COPY (10-50x faster than INSERT)
4. Write analytical SQL queries against real NYC taxi data
5. Use indexes and EXPLAIN ANALYZE to optimize query performance
6. Create views that encapsulate business logic for downstream consumers
