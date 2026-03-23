# Module 03: Exercises

Work through these exercises in order. Each one builds on the previous, taking you
from your first Airflow interaction to a production-style multi-layer pipeline
processing NYC taxi trip data.

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

## Exercise 3: Build a DAG to Ingest Taxi Trips to Bronze (Parquet)

**Goal**: Write a DAG that reads raw monthly taxi trip Parquet files and
repartitions them by pickup date into the Bronze layer.

**Requirements:**

Create a file `dags/my_ingest_trips.py` that:

1. Defines a DAG called `ingest_taxi_trips` with:
   - `start_date` of January 1, 2023 (first available data month)
   - `schedule` set to `@monthly`
   - `catchup=False`
   - Appropriate `default_args` with retries

2. Contains these tasks:
   - `check_source_files`: Verify raw Parquet files exist for yellow and green
     taxi trips for the logical month
   - `ingest_yellow_trips`: Read the yellow taxi Parquet file, write to
     `data/bronze/yellow_taxi_trips/year=YYYY/month=MM/trips.parquet`
   - `ingest_green_trips`: Read the green taxi Parquet file, write to
     `data/bronze/green_taxi_trips/year=YYYY/month=MM/trips.parquet`
   - `log_record_counts`: Print how many records were ingested (use XComs)

3. Uses `{{ ds }}` (the logical date) to derive year/month, making the DAG
   idempotent and backfill-friendly.

4. The ingestion tasks should:
   - Read from `/opt/airflow/data/raw/yellow_tripdata_YYYY-MM.parquet`
   - Write to `/opt/airflow/data/bronze/yellow_taxi_trips/year=YYYY/month=MM/trips.parquet`
   - Use Snappy compression
   - Push the row count to XComs

**Key concepts practiced:**
- Using `execution_date` / `{{ ds }}` for idempotent processing
- Partitioning output by year and month
- XCom for passing metadata between tasks
- Fan-out pattern (yellow and green trips processed in parallel)

**Verification:**
- Trigger the DAG manually (set the logical date to `2023-01-01`)
- Check that Parquet files were created at the expected paths
- Verify the row counts in the task logs

**Hint**: Look at `dags/solution_02_ingest_trips.py` for the reference solution.

---

## Exercise 4: Build a DAG for Bronze to Silver (Clean and Validate Trips)

**Goal**: Write a DAG that reads Bronze Parquet trip data, applies cleaning and
validation logic, and writes validated data to the Silver layer.

**Requirements:**

Create a file `dags/my_bronze_to_silver.py` that:

1. Defines a DAG called `bronze_to_silver_trips` with:
   - `start_date` of January 1, 2023
   - `schedule` set to `@monthly`
   - `catchup=False`

2. Contains these tasks:
   - `check_bronze_data`: Verify Bronze Parquet exists for the logical month
   - `clean_yellow_trips`: Read Bronze data and apply these transformations:
     - Drop rows where pickup or dropoff datetime is null
     - Drop rows where passenger_count is null or <= 0
     - Filter out trips with unreasonable distances (> 200 miles or < 0)
     - Filter out trips with unreasonable fares (> $1000 or < 0)
     - Add `trip_duration_minutes` column (dropoff - pickup time difference)
     - Add `speed_mph` column (distance / duration in hours)
     - Filter out trips with unreasonable speeds (> 100 mph)
     - Add a `processed_at` timestamp column
   - `clean_green_trips`: Same cleaning rules for green taxi trips
   - `log_quality_metrics`: Print metrics via XComs:
     - Total rows in, rows out
     - Number of null rows dropped
     - Number of outlier rows dropped

3. Dependencies:
   ```
   check_bronze_data >> [clean_yellow_trips, clean_green_trips] >> log_quality_metrics
   ```

**Key concepts practiced:**
- Data cleaning and validation patterns for transportation data
- Quality metrics and observability
- Derived columns (duration, speed)
- Fan-out pattern for parallel processing of taxi types

**Hint**: Look at `dags/solution_03_bronze_to_silver.py` for the reference solution.

---

## Exercise 5: Build a DAG for Silver to Gold (Aggregate Metrics)

**Goal**: Write a DAG that reads cleaned Silver data and produces aggregated
Gold-layer tables for analytics.

**Requirements:**

Create a file `dags/my_silver_to_gold.py` that:

1. Defines a DAG called `silver_to_gold_metrics` with:
   - `start_date` of January 1, 2023
   - `schedule` set to `@monthly`
   - `catchup=False`

2. Produces three Gold tables for each month:
   - **Daily trip metrics**
     (`data/processed/gold/daily_trip_metrics/year=YYYY/month=MM/metrics.parquet`):
     - `pickup_date`, `taxi_type`, `total_trips`, `total_passengers`,
       `total_distance_miles`, `total_fare_amount`, `avg_trip_duration_minutes`,
       `avg_speed_mph`
   - **Zone popularity**
     (`data/processed/gold/zone_popularity/year=YYYY/month=MM/metrics.parquet`):
     - `pickup_zone_id`, `taxi_type`, `total_pickups`, `total_dropoffs`,
       `avg_fare_amount`
   - **Revenue summary**
     (`data/processed/gold/revenue_summary/year=YYYY/month=MM/metrics.parquet`):
     - `taxi_type`, `payment_type`, `total_trips`, `total_fare_amount`,
       `total_tip_amount`, `total_total_amount`, `avg_tip_percentage`

3. Each aggregation should be its own task so they can run in parallel:
   ```
   check_silver >> [daily_metrics, zone_popularity, revenue_summary] >> done
   ```

**Key concepts practiced:**
- Fan-out pattern (one check task, multiple parallel aggregations)
- Writing meaningful aggregations for transportation analytics
- Gold layer design for analytical consumption

**Hint**: Look at `dags/solution_04_silver_to_gold.py` for the reference solution.

---

## Exercise 6: Implement Backfilling with execution_date

**Goal**: Use Airflow's backfill mechanism to process historical data.

**Steps:**

1. Make sure your ingestion DAG (Exercise 3) is working for a single month.

2. Enable `catchup=True` on the DAG (or use a dedicated copy).

3. Backfill a date range using the CLI:
   ```bash
   docker compose exec airflow-scheduler \
     airflow dags backfill \
       -s 2023-01-01 \
       -e 2023-06-01 \
       ingest_taxi_trips
   ```

4. Monitor the backfill in the Airflow UI:
   - Watch the Grid view fill in with green squares
   - Notice that each run uses a different `execution_date`

5. Verify idempotency: run the same backfill command again. The output Parquet
   files should be identical (overwritten, not duplicated).

**Questions to answer:**
1. How many DAG runs were created for the date range?
2. What happens for months where no source file exists?
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

4. **SLA monitoring**: Add `sla=timedelta(hours=1)` to critical tasks.

5. **Graceful handling of missing data**: Instead of failing when a source file
   is missing, use `AirflowSkipException` to skip downstream tasks:
   ```python
   from airflow.exceptions import AirflowSkipException

   def check_file(ds):
       year_month = ds[:7]  # "2023-01"
       path = f"/opt/airflow/data/raw/yellow_tripdata_{year_month}.parquet"
       if not os.path.exists(path):
           raise AirflowSkipException(f"No data for {year_month}, skipping.")
   ```

**Verification:**
- Trigger the DAG for a month with no data file. The check task should show
  "skipped" (pink) status, and downstream tasks should also be skipped.
- Simulate a failure (e.g., raise an exception in a task). Verify that the task
  retries and that the failure callback prints the alert message.

---

## Exercise 8: Use Sensors to Wait for File Arrival

**Goal**: Use a `FileSensor` to wait for raw data files before processing.

**Requirements:**

Create a modified version of your ingestion DAG that:

1. Starts with a `FileSensor` that waits for the monthly Parquet file:
   ```python
   from airflow.sensors.filesystem import FileSensor

   wait_for_file = FileSensor(
       task_id="wait_for_yellow_file",
       filepath="/opt/airflow/data/raw/yellow_tripdata_{{ ds[:7] }}.parquet",
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

1. Defines a DAG called `full_taxi_pipeline` with:
   - `start_date` of January 1, 2023
   - `schedule` set to `@monthly`
   - `catchup=False`

2. Uses three `TaskGroup` blocks:
   - `bronze_layer`: Contains the ingestion tasks (check file, ingest yellow
     and green trips to Parquet)
   - `silver_layer`: Contains cleaning/validation tasks for both taxi types
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
- Triggering it for a valid month processes data through all three layers
- The summary task prints a complete report

**Hint**: Look at `dags/solution_05_full_pipeline.py` for the reference solution.

---

## Bonus Challenges

### Bonus 1: Dynamic DAGs
Create a DAG factory function that generates one DAG per taxi type (yellow,
green, FHV). Use a loop in your DAG file to register multiple DAGs with the
Airflow scheduler.

### Bonus 2: Dataset-Triggered DAGs (Airflow 2.4+)
Instead of using sensors or time-based schedules to chain DAGs, use Airflow
Datasets. Have the Bronze DAG produce a Dataset event when it writes output,
and have the Silver DAG trigger automatically when that Dataset is updated.

### Bonus 3: Custom Operator
Write a custom `ParquetCleanOperator` that encapsulates the trip cleaning logic.
It should accept `source_path`, `dest_path`, and cleaning thresholds as
parameters with Jinja templating support.

### Bonus 4: Weather Enrichment
Extend your Silver layer to join trip data with the `daily_weather.csv` file.
Add weather columns (temperature, precipitation) to each trip based on the
pickup date. Analyze how weather affects trip patterns in the Gold layer.

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
rm -rf ../data/bronze/yellow_taxi_trips
rm -rf ../data/bronze/green_taxi_trips
rm -rf ../data/processed/silver/yellow_taxi_trips
rm -rf ../data/processed/silver/green_taxi_trips
rm -rf ../data/processed/gold/daily_trip_metrics
rm -rf ../data/processed/gold/zone_popularity
rm -rf ../data/processed/gold/revenue_summary
```
