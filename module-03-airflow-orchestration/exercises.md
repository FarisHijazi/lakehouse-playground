# Module 03: Exercises

Work through these exercises in order. Each one builds on the previous, taking you
from your first Airflow interaction to a production-style multi-layer pipeline.

---

## Exercise 1: Start Airflow and Explore the UI

**Goal**: Get Airflow running locally and understand the web interface.

**Steps:**

1. Navigate to the module directory and start the services:
   ```bash
   cd module-03-airflow-orchestration
   docker compose up -d
   ```

2. Wait 30-60 seconds for initialization, then open the Airflow UI:
   ```
   http://localhost:8080
   ```
   Login with username `airflow` and password `airflow`.

3. Explore the following pages:
   - **DAGs list**: The home page. Shows all discovered DAGs, their schedule,
     last run status, and toggle to enable/disable.
   - **Grid view**: Click any DAG name. This shows a matrix of runs (columns)
     and tasks (rows) with color-coded status.
   - **Graph view**: Shows task dependencies as a visual flowchart.
   - **Calendar view**: Shows historical run outcomes by date.
   - **Code view**: Shows the actual Python code of the DAG file.

4. Check the running containers:
   ```bash
   docker compose ps
   ```

5. View scheduler logs to confirm DAGs are being parsed:
   ```bash
   docker compose logs airflow-scheduler --tail 50
   ```

**Questions to answer:**
1. How many services does `docker compose ps` show?
2. What port is the webserver running on?
3. What executor is configured? (Check Admin > Configuration in the UI)
4. What happens when you toggle a DAG from "paused" to "active"?

---

## Exercise 2: Write Your First DAG

**Goal**: Create a simple DAG with print tasks and dependencies to learn the basics.

**Requirements:**

Create a file `dags/my_first_dag.py` that:

1. Defines a DAG called `my_first_dag` with:
   - `start_date` of January 1, 2024
   - `schedule` set to `@daily`
   - `catchup=False`
   - `tags=["exercise"]`

2. Contains four tasks:
   - `start`: An `EmptyOperator` (a no-op used as an entry point)
   - `print_date`: A `BashOperator` that runs `date`
   - `greet`: A `PythonOperator` that prints `"Hello from Airflow!"`
   - `end`: An `EmptyOperator`

3. Sets dependencies so the execution order is:
   ```
   start -> [print_date, greet] -> end
   ```
   (start runs first, then print_date and greet run in parallel, then end)

4. After saving the file, wait for the scheduler to pick it up (~30 seconds),
   then find it in the UI, unpause it, and trigger it manually.

**Verification:**
- The DAG appears in the UI without import errors
- All four tasks show green (success) after a manual trigger
- You can view the log output of each task in the Grid or Graph view

**Hint**: Look at `dags/solution_01_hello_world.py` if you get stuck.

---

## Exercise 3: Build a DAG to Ingest Raw Events to Bronze (Parquet)

**Goal**: Write a DAG that reads raw JSONL listening events for a specific date
and writes them to Parquet format in the Bronze layer.

**Requirements:**

Create a file `dags/my_ingest_events.py` that:

1. Defines a DAG called `ingest_listening_events` with:
   - `start_date` of January 10, 2018 (first available data date)
   - `schedule` set to `@daily`
   - `catchup=False`
   - Appropriate `default_args` with retries

2. Contains these tasks:
   - `check_source_file`: Verify the raw JSONL file exists for `{{ ds }}`
   - `ingest_to_bronze`: Read the JSONL file, convert to Parquet, save to
     `data/processed/bronze/listening_events/date={{ ds }}/events.parquet`
   - `log_record_count`: Print how many records were ingested (use XComs)

3. Uses `{{ ds }}` (the logical date) to build file paths, making the DAG
   idempotent and backfill-friendly.

4. The ingestion task should:
   - Read from `/opt/airflow/data/raw/listening_events/events_{{ ds }}.jsonl`
   - Write to `/opt/airflow/data/processed/bronze/listening_events/date={{ ds }}/events.parquet`
   - Use Snappy compression
   - Push the row count to XComs

**Key concepts practiced:**
- Using `execution_date` / `{{ ds }}` for idempotent processing
- Partitioning output by date
- XCom for passing metadata between tasks
- Error handling when source files are missing

**Verification:**
- Trigger the DAG manually (set the logical date to `2018-01-10`)
- Check that a Parquet file was created at the expected path
- Verify the row count in the task logs

**Hint**: Look at `dags/solution_02_ingest_events.py` for the reference solution.

---

## Exercise 4: Build a DAG for Bronze to Silver (Clean and Deduplicate)

**Goal**: Write a DAG that reads Bronze Parquet data, applies cleaning and
deduplication logic, and writes validated data to the Silver layer.

**Requirements:**

Create a file `dags/my_bronze_to_silver.py` that:

1. Defines a DAG called `bronze_to_silver_events` with:
   - `start_date` of January 10, 2018
   - `schedule` set to `@daily`
   - `catchup=False`

2. Contains these tasks:
   - `check_bronze_data`: Verify Bronze Parquet exists for `{{ ds }}`
   - `clean_and_deduplicate`: Read Bronze data and apply these transformations:
     - Drop duplicate `event_id` values (keep first occurrence)
     - Drop rows where `user_id` or `episode_id` is null
     - Validate that `listened_seconds` is non-negative (set negatives to 0)
     - Validate that `event_type` is one of: `play`, `pause`, `resume`,
       `complete`, `skip` (drop unknown types)
     - Add a `processed_at` timestamp column
   - `write_silver`: Write cleaned data to
     `data/processed/silver/listening_events/date={{ ds }}/events.parquet`
   - `log_quality_metrics`: Print metrics via XComs:
     - Total rows in, rows out
     - Number of duplicates removed
     - Number of invalid rows dropped

3. Dependencies: `check_bronze_data >> clean_and_deduplicate >> write_silver >> log_quality_metrics`

**Key concepts practiced:**
- Data cleaning and validation patterns
- Quality metrics and observability
- Multi-step task chains
- Separation of read/transform/write logic

**Hint**: Look at `dags/solution_03_bronze_to_silver.py` for the reference solution.

---

## Exercise 5: Build a DAG for Silver to Gold (Aggregate Metrics)

**Goal**: Write a DAG that reads cleaned Silver data and produces aggregated
Gold-layer tables for analytics.

**Requirements:**

Create a file `dags/my_silver_to_gold.py` that:

1. Defines a DAG called `silver_to_gold_metrics` with:
   - `start_date` of January 10, 2018
   - `schedule` set to `@daily`
   - `catchup=False`

2. Produces three Gold tables for each date:
   - **Daily episode metrics**
     (`data/processed/gold/daily_episode_metrics/date={{ ds }}/metrics.parquet`):
     - `episode_id`, `total_listens`, `unique_listeners`, `total_seconds`,
       `avg_listen_seconds`, `completion_rate` (fraction of `complete` events)
   - **Daily platform metrics**
     (`data/processed/gold/daily_platform_metrics/date={{ ds }}/metrics.parquet`):
     - `platform`, `total_listens`, `unique_users`, `total_seconds`
   - **Daily country metrics**
     (`data/processed/gold/daily_country_metrics/date={{ ds }}/metrics.parquet`):
     - `country`, `total_listens`, `unique_users`, `total_seconds`

3. Each aggregation should be its own task so they can run in parallel:
   ```
   check_silver >> [episode_metrics, platform_metrics, country_metrics] >> done
   ```

**Key concepts practiced:**
- Fan-out pattern (one check task, multiple parallel aggregations)
- Writing meaningful aggregations
- Gold layer design for analytical consumption

**Hint**: Look at `dags/solution_04_silver_to_gold.py` for the reference solution.

---

## Exercise 6: Implement Backfilling with execution_date

**Goal**: Use Airflow's backfill mechanism to process historical data.

**Steps:**

1. Make sure your ingestion DAG (Exercise 3) is working for a single date.

2. Enable `catchup=True` on the DAG (or use a dedicated copy).

3. Backfill a date range using the CLI:
   ```bash
   docker compose exec airflow-scheduler \
     airflow dags backfill \
       -s 2018-01-10 \
       -e 2018-01-31 \
       ingest_listening_events
   ```

4. Monitor the backfill in the Airflow UI:
   - Watch the Grid view fill in with green squares
   - Notice that each run uses a different `execution_date`

5. Verify idempotency: run the same backfill command again. The output Parquet
   files should be identical (overwritten, not duplicated).

**Questions to answer:**
1. How many DAG runs were created for the date range?
2. What happens for dates where no source file exists (e.g., `2018-01-11`)?
3. If you backfill the same range again, do you get duplicate data?

---

## Exercise 7: Add Error Handling, Retries, and Alerting

**Goal**: Make your DAGs production-ready with proper failure handling.

**Requirements:**

Modify any of your existing DAGs to add:

1. **Retries**: Set `retries=2` and `retry_delay=timedelta(minutes=1)` in
   `default_args`.

2. **Timeouts**: Add `execution_timeout=timedelta(minutes=30)` to long-running
   tasks to prevent them from hanging indefinitely.

3. **Failure callbacks**: Add an `on_failure_callback` that logs the error
   context:
   ```python
   def task_failure_callback(context):
       task_id = context['task_instance'].task_id
       dag_id = context['task_instance'].dag_id
       execution_date = context['execution_date']
       exception = context.get('exception', 'Unknown')
       print(f"ALERT: Task {task_id} in DAG {dag_id} failed!")
       print(f"  Execution date: {execution_date}")
       print(f"  Exception: {exception}")
       # In production, you would send a Slack/email/PagerDuty alert here
   ```

4. **SLA monitoring**: Add `sla=timedelta(hours=1)` to critical tasks. (Note:
   SLAs are checked by the scheduler and generate SLA miss entries in the UI.)

5. **Graceful handling of missing data**: Instead of failing when a source file
   is missing, use `AirflowSkipException` to skip downstream tasks:
   ```python
   from airflow.exceptions import AirflowSkipException

   def check_file(ds):
       path = f"/opt/airflow/data/raw/listening_events/events_{ds}.jsonl"
       if not os.path.exists(path):
           raise AirflowSkipException(f"No data for {ds}, skipping.")
   ```

**Verification:**
- Trigger the DAG for a date with no data file. The check task should show
  "skipped" (pink) status, and downstream tasks should also be skipped.
- Simulate a failure (e.g., raise an exception in a task). Verify that the task
  retries and that the failure callback prints the alert message.

---

## Exercise 8: Use Sensors to Wait for File Arrival

**Goal**: Use a `FileSensor` to wait for raw data files before processing.

**Requirements:**

Create a modified version of your ingestion DAG that:

1. Starts with a `FileSensor` that waits for the daily JSONL file:
   ```python
   from airflow.sensors.filesystem import FileSensor

   wait_for_file = FileSensor(
       task_id="wait_for_daily_file",
       filepath="/opt/airflow/data/raw/listening_events/events_{{ ds }}.jsonl",
       poke_interval=30,         # Check every 30 seconds
       timeout=600,              # Give up after 10 minutes
       mode="poke",              # Keep the worker slot while waiting
       soft_fail=True,           # Mark as skipped instead of failed on timeout
   )
   ```

2. Only runs the ingestion task after the sensor succeeds.

3. Experiment with both `mode="poke"` and `mode="reschedule"`:
   - `poke`: Holds the worker slot while waiting (simpler, uses resources)
   - `reschedule`: Frees the worker between checks (production-friendly)

**Questions to answer:**
1. What is the difference between `poke` and `reschedule` modes in terms of
   worker slot usage?
2. What happens when `soft_fail=True` and the sensor times out?
3. When would you use a sensor vs. a simple file existence check in Python?

**Hint**: You need to set up an Airflow Connection for the `FileSensor` to work.
In the Airflow UI, go to Admin > Connections and create a connection:
- Connection ID: `fs_default`
- Connection Type: `File (path)`
- Extra: `{"path": "/"}`

---

## Exercise 9: Create a Full Pipeline DAG with TaskGroups

**Goal**: Combine all layers (Bronze, Silver, Gold) into a single DAG using
TaskGroups for organization.

**Requirements:**

Create a file `dags/my_full_pipeline.py` that:

1. Defines a DAG called `full_listening_pipeline` with:
   - `start_date` of January 10, 2018
   - `schedule` set to `@daily`
   - `catchup=False`

2. Uses three `TaskGroup` blocks:
   - `bronze_layer`: Contains the ingestion tasks (check file, ingest to Parquet)
   - `silver_layer`: Contains cleaning/dedup tasks
   - `gold_layer`: Contains the three aggregation tasks running in parallel

3. Dependencies between groups:
   ```
   bronze_layer >> silver_layer >> gold_layer
   ```

4. Includes `default_args` with retries, timeouts, and failure callbacks.

5. Ends with a final summary task that prints:
   - Logical date processed
   - Row counts at each layer (via XComs)
   - Processing duration

**Key concepts practiced:**
- TaskGroups for logical organization (they appear as collapsible groups in the UI)
- End-to-end pipeline design
- Combining everything from previous exercises

**Verification:**
- The DAG appears in the UI with three collapsible TaskGroups
- Triggering it for a valid date processes data through all three layers
- The summary task prints a complete report

**Hint**: Look at `dags/solution_05_full_pipeline.py` for the reference solution.

---

## Bonus Challenges

### Bonus 1: Dynamic DAGs
Create a DAG factory function that generates one DAG per data source (listening
events, CDN logs, ad events). Use a loop in your DAG file to register multiple
DAGs with the Airflow scheduler.

### Bonus 2: Dataset-Triggered DAGs (Airflow 2.4+)
Instead of using sensors or time-based schedules to chain DAGs, use Airflow
Datasets. Have the Bronze DAG produce a Dataset event when it writes output,
and have the Silver DAG trigger automatically when that Dataset is updated.

### Bonus 3: Custom Operator
Write a custom `JsonlToParquetOperator` that encapsulates the JSONL-to-Parquet
conversion logic. It should accept `source_path` and `dest_path` as parameters
with Jinja templating support.

---

## Cleanup

When you are done with the exercises:

```bash
cd module-03-airflow-orchestration

# Stop all services
docker compose down

# Stop and remove all data (full reset)
docker compose down -v
```

To remove processed data created by the DAGs:

```bash
rm -rf ../data/processed/bronze/listening_events
rm -rf ../data/processed/silver/listening_events
rm -rf ../data/processed/gold/daily_episode_metrics
rm -rf ../data/processed/gold/daily_platform_metrics
rm -rf ../data/processed/gold/daily_country_metrics
```
