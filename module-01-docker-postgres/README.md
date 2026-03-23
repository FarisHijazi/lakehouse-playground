# Module 01: Docker & Postgres

## Why This Module Exists

Every data pipeline starts somewhere. Before you build a lakehouse, before you orchestrate
workflows, before you write a single transformation -- you need to know how to stand up a
database and load data into it. This module teaches that foundation.

## Why Docker Matters for Data Engineering

In production data engineering, "it works on my machine" is not acceptable. A pipeline that
breaks because someone has a different Postgres version, different locale settings, or missing
system libraries is a pipeline that will fail at 3 AM.

Docker solves this by packaging your database, your tools, and your configuration into
containers that run identically everywhere -- your laptop, your colleague's laptop, CI/CD,
staging, production.

**What you will learn:**
- How to define infrastructure as code with `docker-compose.yml`
- How to initialize a database with schema scripts on first boot
- How containers isolate dependencies and make environments reproducible

## Why Postgres for Data Engineers

Postgres is not just "a relational database." It is the starting point for most analytical
workloads and the engine behind many data tools (Airflow metadata, dbt targets, feature
stores). Understanding Postgres deeply -- its type system, indexing strategies, query planner,
and COPY protocol -- makes you effective across the entire data stack.

**What you will learn:**
- Schema design for analytical workloads (not just OLTP)
- Proper data types, constraints, and indexing
- The COPY protocol for bulk loading (orders of magnitude faster than INSERT)
- Reading query plans with EXPLAIN ANALYZE
- Creating views to encapsulate business logic

## The Data

You will work with a podcast platform dataset inspired by Thmanyah, containing:

| Dataset | Format | Records | Description |
|---------|--------|---------|-------------|
| `podcasts.json` | JSON | 10 | Podcast metadata (Arabic/English names, categories) |
| `episodes.json` | JSON | 784 | Episode details (duration, season, episode number) |
| `users.csv` | CSV | 5,000 | User profiles (intentionally messy data!) |
| `listening_events/` | JSONL | ~200k | Daily listening event files |
| `cdn_logs.csv` | CSV | 50,000 | CDN delivery logs (bitrate, buffering, errors) |
| `ad_events.json` | JSON | 18,071 | Ad impressions (advertisers, revenue) |

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
- Python 3.9+ with `psycopg2-binary` (`pip install psycopg2-binary`)

### Quick Start

```bash
# 1. Start the containers
docker compose up -d

# 2. Wait for Postgres to be ready (about 5 seconds)
docker compose logs postgres

# 3. Load the data
python load_data.py

# 4. Connect to pgAdmin at http://localhost:8080
#    Email: admin@lakehouse.dev / Password: admin

# 5. Or connect directly via psql
docker compose exec postgres psql -U lakehouse -d podcast_platform
```

## Exercises

Work through `exercises.md` in order. Each exercise builds on the previous one. Solutions
are in `solutions/solutions.sql` -- but try to solve them yourself first.

## File Structure

```
module-01-docker-postgres/
  docker-compose.yml   # Container definitions
  init.sql             # Schema (auto-runs on first boot)
  load_data.py         # Data loading script (the solution)
  exercises.md         # Hands-on exercises to complete
  solutions/
    solutions.sql      # SQL solutions to all exercises
  README.md            # This file
```

## When You Are Done

You should be able to:
1. Explain why Docker matters for reproducible data environments
2. Design a Postgres schema for analytical workloads
3. Bulk-load data from multiple file formats (JSON, CSV, JSONL)
4. Write analytical SQL queries against real-world data
5. Use indexes and EXPLAIN ANALYZE to optimize query performance
6. Create views that encapsulate business logic for downstream consumers
