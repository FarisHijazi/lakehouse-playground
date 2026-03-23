# Module 03: Airflow Orchestration

## Overview

In any data engineering project, you need a way to schedule, monitor, and manage
the dozens (or hundreds) of data pipelines that keep your platform running.
**Apache Airflow** is the industry-standard tool for this job. It lets you define
workflows as code (Python), schedule them, monitor them through a web UI, and
handle retries and alerting automatically.

In this module you will learn Airflow from scratch and apply it to our NYC taxi
data platform, building DAGs that move taxi trip data through the Bronze, Silver,
and Gold layers of a medallion architecture.

---

## What Is Airflow?

Airflow is a **workflow orchestration** platform. Think of it as a sophisticated
cron scheduler with:

- A rich web UI for monitoring
- Dependency management between tasks
- Automatic retries and failure alerting
- Full history and audit logging
- Extensibility through plugins and providers

Airflow does **not** process data itself. It tells *other* systems (Spark, Python
scripts, dbt, SQL engines) when to run and in what order. This is a critical
distinction: Airflow is the **conductor**, not the **orchestra**.

---

## Core Concepts

### DAG (Directed Acyclic Graph)

A DAG is a collection of tasks with defined dependencies. "Directed" means tasks
have a specific order. "Acyclic" means no circular dependencies (Task A cannot
depend on Task B if Task B already depends on Task A).

```python
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",          # Run once per day
    catchup=False,              # Don't backfill past dates on first run
) as dag:
    ...
```

### Tasks

A task is a single unit of work inside a DAG. Each task is an instance of an
**Operator**.

### Operators

Operators define *what* a task does. The most common ones:

| Operator | Purpose |
|---|---|
| `PythonOperator` | Run a Python callable |
| `BashOperator` | Run a bash command |
| `EmptyOperator` | No-op placeholder (useful for grouping) |
| `FileSensor` | Wait for a file to appear |
| `ExternalTaskSensor` | Wait for a task in another DAG |

### Sensors

Sensors are special operators that **wait** for a condition to be true before
proceeding. They are perfect for waiting on file arrivals or upstream DAG
completion.

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath="/opt/airflow/data/raw/yellow_tripdata_2023-01.parquet",
    poke_interval=60,       # Check every 60 seconds
    timeout=3600,           # Give up after 1 hour
    mode="poke",            # Or "reschedule" to free up the worker
)
```

### Hooks

Hooks provide interfaces to external systems (databases, cloud storage, APIs).
Operators use hooks under the hood. You rarely use them directly unless building
custom operators.

### XComs (Cross-Communication)

XComs let tasks pass small pieces of data to each other. For example, one task
can push the number of rows processed and a downstream task can read it.

```python
# Push
def push_func(ti):
    ti.xcom_push(key="row_count", value=42)

# Pull
def pull_func(ti):
    count = ti.xcom_pull(task_ids="push_task", key="row_count")
    print(f"Received {count} rows")
```

> **Warning**: XComs are stored in Airflow's metadata database. Never pass large
> datasets through XComs. Pass *file paths* or *row counts*, not dataframes.

---

## Task Dependencies

You define execution order using `>>` (downstream) and `<<` (upstream):

```python
task_a >> task_b >> task_c        # A -> B -> C (sequential)
task_a >> [task_b, task_c]        # A -> B and A -> C (fan-out)
[task_b, task_c] >> task_d        # B -> D and C -> D (fan-in)
```

---

## Scheduling

### Cron Expressions

Airflow uses standard cron expressions:

| Expression | Meaning |
|---|---|
| `0 0 * * *` | Midnight every day |
| `0 6 * * 1` | 6 AM every Monday |
| `0 */2 * * *` | Every 2 hours |
| `@daily` | Alias for `0 0 * * *` |
| `@hourly` | Alias for `0 * * * *` |
| `@weekly` | Alias for `0 0 * * 0` |
| `@monthly` | Alias for `0 0 1 * *` |
| `None` | Only triggered manually |

### The `execution_date` / `logical_date` Concept

This is the most confusing part of Airflow for beginners.

When a DAG runs on a schedule, the `execution_date` (called `logical_date` in
Airflow 2.2+) represents the **start of the data interval**, not the time the
DAG actually runs.

Example: A monthly DAG scheduled for the 1st of each month processes the
**previous month's** data. The DAG run triggered at `2023-02-01 00:00` has
`execution_date = 2023-01-01`.

This matters because it makes your pipelines **idempotent**: you can re-run the
same month and get the same result.

Template variables for use in your DAGs:
- `{{ ds }}` - The logical date as `YYYY-MM-DD` (e.g., `2023-01-01`)
- `{{ ds_nodash }}` - Same but without dashes: `20230101`
- `{{ data_interval_start }}` - Start of the data interval (datetime)
- `{{ data_interval_end }}` - End of the data interval (datetime)

---

## Idempotency and Backfilling

### Idempotency

A pipeline is **idempotent** if running it multiple times with the same input
produces the same output. This is critical for reliability.

Rules for idempotent DAGs:
1. **Use `execution_date`** to scope your data, not "today's date"
2. **Overwrite outputs**, don't append (use `mode="overwrite"` or delete before writing)
3. **Don't rely on external mutable state** that changes between runs

### Backfilling

If your DAG has `catchup=True` (the default), Airflow will create DAG runs for
every missed schedule interval between `start_date` and now. This is called
**backfilling**.

You can also backfill manually from the CLI:
```bash
airflow dags backfill -s 2023-01-01 -e 2023-06-30 my_dag_id
```

---

## Best Practices

1. **Airflow is for orchestration, not transformation.** Don't write heavy pandas
   or Spark logic inside your DAG file. Instead, call external scripts or systems.

2. **Keep DAG files lightweight.** The scheduler parses all DAG files regularly.
   Heavy imports at the top level slow down the entire system.

3. **Use templates** (`{{ ds }}`, `{{ params }}`) instead of hardcoded values.

4. **Set retries and timeouts.** Every task should have sensible defaults:
   ```python
   default_args = {
       "retries": 2,
       "retry_delay": timedelta(minutes=5),
       "execution_timeout": timedelta(hours=1),
   }
   ```

5. **Use meaningful task IDs.** `ingest_yellow_trips` is better than `task_1`.

6. **Test locally first.** You can run DAG files as plain Python scripts to check
   for syntax errors: `python my_dag.py`

7. **One DAG per pipeline.** Don't cram everything into a single mega-DAG. Use
   `ExternalTaskSensor` or dataset-triggered DAGs to link them.

---

## Common Pitfalls

| Pitfall | Solution |
|---|---|
| Using `datetime.now()` instead of `execution_date` | Always use `{{ ds }}` or `logical_date` for data scoping |
| Passing DataFrames through XComs | Pass file paths or metadata only |
| Top-level imports of heavy libraries | Import inside your task functions |
| Not setting `catchup=False` and drowning in backfill runs | Set `catchup=False` unless you explicitly need backfill |
| Tasks that are not idempotent | Overwrite outputs; use `execution_date` for partitioning |
| Giant monolithic DAGs | Split into smaller DAGs connected by sensors or datasets |
| Hardcoded file paths | Use Airflow Variables, Connections, or template variables |

---

## Architecture in This Module

```
Raw Parquet/CSV files (data/raw/)
  - yellow_tripdata_2023-XX.parquet
  - green_tripdata_2023-XX.parquet
  - taxi_zones.csv, vendors.csv, etc.
        |
        v
  [Bronze Layer] -- Ingest raw Parquet -> partitioned by pickup date
        |
        v
  [Silver Layer] -- Clean trips: handle nulls, filter outliers,
                    add derived columns (duration, speed, etc.)
        |
        v
  [Gold Layer]   -- Aggregate metrics (daily trip stats, zone popularity,
                    revenue summaries)
```

Each layer is implemented as a separate DAG, with a final "full pipeline" DAG
that ties them together using TaskGroups.

---

## Getting Started

```bash
# 1. Start Airflow
cd module-03-airflow-orchestration
docker compose up -d

# 2. Wait for initialization (~30-60 seconds), then open the UI
open http://localhost:8080
# Login: airflow / airflow

# 3. Enable DAGs in the UI and trigger them manually or wait for the schedule

# 4. When finished
docker compose down
```

---

## File Structure

```
module-03-airflow-orchestration/
├── README.md                           # This file
├── docker-compose.yml                  # Airflow infrastructure
├── exercises.md                        # Hands-on exercises
└── dags/
    ├── solution_01_hello_world.py      # Exercise 2: First DAG
    ├── solution_02_ingest_trips.py     # Exercise 3: Raw -> Bronze
    ├── solution_03_bronze_to_silver.py # Exercise 4: Bronze -> Silver
    ├── solution_04_silver_to_gold.py   # Exercise 5: Silver -> Gold
    └── solution_05_full_pipeline.py    # Exercise 9: Complete pipeline
```

---

## Prerequisites

- Docker and Docker Compose
- Completion of Module 01 (Docker basics) and Module 02 (Data Ingestion)
- Basic Python knowledge
- The raw data files in `data/raw/` (included in this repository)
