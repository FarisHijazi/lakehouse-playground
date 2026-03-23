# Module 05: Exercises

Work through these exercises in order. Each one builds on the previous, taking you
from setting up a dbt project to building a fully tested, documented analytics layer.

---

## Exercise 1: Initialize a dbt Project with dbt-duckdb

**Goal**: Set up a working dbt project connected to DuckDB.

**Steps:**

1. Install dbt-duckdb if you have not already:
   ```bash
   pip install dbt-duckdb
   ```

2. Navigate to the module directory:
   ```bash
   cd module-05-dbt-analytics
   ```

3. Verify dbt is installed:
   ```bash
   dbt --version
   ```

4. Examine the project configuration files:
   - `dbt_project.yml` -- project name, model paths, materialization defaults
   - `profiles.yml` -- DuckDB connection, database file path

5. Test the connection:
   ```bash
   dbt debug
   ```
   This should show "All checks passed!" if everything is configured correctly.

6. Run a seed to verify end-to-end connectivity:
   ```bash
   dbt seed
   ```

**Questions to answer:**
1. What version of dbt-core and dbt-duckdb did `dbt --version` report?
2. What database file path is configured in `profiles.yml`?
3. Where will dbt place compiled SQL files?
4. What materialization is set for staging models in `dbt_project.yml`?

---

## Exercise 2: Create Source Definitions for Raw Data

**Goal**: Define source YAML that documents where the raw data lives.

**Context**: With dbt-duckdb, you cannot define sources the traditional way (pointing
to existing database tables). Instead, the staging models read directly from files
using DuckDB's `read_parquet()` and `read_csv_auto()` functions. However, you
still document the raw data as sources in YAML for lineage and documentation.

**Steps:**

1. Open `models/staging/schema.yml` and examine the source definitions.

2. Notice how each source table includes metadata about its file path and format.

3. Understand the difference between traditional dbt sources and our approach:
   - Traditional: `{{ source('raw', 'yellow_trips') }}` resolves to a database table
   - Our approach: staging models use `read_parquet('../data/raw/yellow_tripdata_*.parquet')` directly,
     and source YAML serves as documentation

4. Run `dbt docs generate` and `dbt docs serve` to see the source documentation.

**Questions to answer:**
1. How many source tables are defined?
2. What file formats does the raw data include (parquet, CSV)?
3. Why can we not use `{{ source() }}` directly with file-based sources in DuckDB?
4. What metadata fields are documented for each source table?

---

## Exercise 3: Build Staging Models

**Goal**: Create the staging layer -- one model per raw data source, performing
light cleaning and type casting.

**Steps:**

1. Review each staging model. For each one, identify:
   - What raw data file it reads
   - What cleaning or transformations it applies
   - What column renames or type casts it performs

2. Start with `stg_yellow_trips.sql`:
   ```bash
   dbt run --select stg_yellow_trips
   ```

3. Query the result to verify the data loaded correctly:
   ```bash
   dbt run --select stg_yellow_trips && \
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('SELECT pickup_date, count(*) FROM staging.stg_yellow_trips GROUP BY 1 ORDER BY 1 LIMIT 10').fetchdf())
   con.close()
   "
   ```

4. Run all staging models:
   ```bash
   dbt run --select staging
   ```

5. Check for any errors. Common issues:
   - File path not found (check relative paths from the dbt project directory)
   - Parquet schema mismatches between monthly files
   - Type casting failures for numeric columns

**Questions to answer:**
1. How does `stg_yellow_trips.sql` handle filtering invalid trips?
2. What does `stg_green_trips.sql` do to normalize column names with the yellow trips?
3. How are negative fare values handled?
4. What zone lookup enrichments happen in the staging layer vs later?
5. Why do staging models use `view` materialization instead of `table`?

---

## Exercise 4: Build Intermediate Models

**Goal**: Build the intermediate layer that joins staging models and applies
business logic.

**Steps:**

1. Review `int_trips_enriched.sql`:
   - What tables does it join?
   - What derived metrics does it compute (trip duration, speed, etc.)?
   - Why is `left join` used instead of `inner join`?

2. Run the intermediate models:
   ```bash
   dbt run --select intermediate
   ```

3. Verify the enrichment worked:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT pickup_borough, count(*) as trips, round(avg(trip_distance), 1) as avg_dist
       FROM intermediate.int_trips_enriched
       GROUP BY 1 ORDER BY 2 DESC LIMIT 5
   ''').fetchdf())
   con.close()
   "
   ```

4. Review `int_daily_trip_summary.sql`:
   - How does it aggregate trips by day?
   - What metrics are computed per day?
   - Does it include weather data?

5. Check daily summary statistics:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT
           count(*) as total_days,
           round(avg(total_trips), 0) as avg_daily_trips,
           round(avg(total_fare_amount), 2) as avg_daily_revenue
       FROM intermediate.int_daily_trip_summary
   ''').fetchdf())
   con.close()
   "
   ```

**Questions to answer:**
1. How many columns does `int_trips_enriched` have?
2. How is trip duration calculated, and what units is it in?
3. What happens if a trip references a zone that does not exist in the lookup?
4. How does the daily summary handle different taxi types (yellow, green)?
5. What weather fields are joined into the daily summary?

---

## Exercise 5: Build Mart Models

**Goal**: Build the analytics-ready mart layer with aggregated, consumption-ready tables.

**Steps:**

1. Run all mart models:
   ```bash
   dbt run --select marts
   ```

2. Explore `mart_daily_metrics`:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT trip_date, total_trips, total_revenue, avg_trip_distance
       FROM marts.mart_daily_metrics
       ORDER BY trip_date DESC LIMIT 10
   ''').fetchdf())
   con.close()
   "
   ```

3. Explore `mart_zone_performance`:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT pickup_zone, pickup_borough, total_trips, avg_fare, avg_tip_pct
       FROM marts.mart_zone_performance
       ORDER BY total_trips DESC LIMIT 10
   ''').fetchdf())
   con.close()
   "
   ```

4. Explore `mart_hourly_patterns`:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT pickup_hour, avg_trips, avg_fare, avg_trip_distance
       FROM marts.mart_hourly_patterns
       ORDER BY pickup_hour
   ''').fetchdf())
   con.close()
   "
   ```

5. Explore `mart_weather_impact`:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT weather_category, avg_trips, avg_fare, avg_tip_pct
       FROM marts.mart_weather_impact
       ORDER BY avg_trips DESC
   ''').fetchdf())
   con.close()
   "
   ```

**Questions to answer:**
1. Why are mart models materialized as `table` instead of `view`?
2. What is the difference between `total_trips` and `total_revenue` in `mart_daily_metrics`?
3. How is `avg_tip_pct` calculated in `mart_zone_performance`?
4. What does `pickup_hour` represent in `mart_hourly_patterns`?
5. How does `mart_weather_impact` categorize weather conditions?

---

## Exercise 6: Add Schema Tests

**Goal**: Understand and run dbt's built-in schema tests.

**Steps:**

1. Open each `schema.yml` file and identify all tests defined:
   ```bash
   grep -r "tests:" models/ --include="*.yml" -A 3
   ```

2. Run all tests:
   ```bash
   dbt test
   ```

3. Examine the output. For any failures, investigate:
   ```bash
   # Run tests for a specific model
   dbt test --select stg_yellow_trips

   # Show compiled SQL for a test
   cat target/compiled/taxi_analytics/models/staging/schema.yml/unique_stg_yellow_trips_trip_id.sql
   ```

4. Understand the four built-in test types:
   - **unique**: Verifies no duplicate values in a column
   - **not_null**: Verifies no null values
   - **accepted_values**: Verifies values are in a defined list
   - **relationships**: Verifies foreign key integrity

5. Look at the `relationships` test on `stg_yellow_trips.PULocationID`:
   - What does it check?
   - What happens if a trip references a LocationID not in `stg_zones`?

6. Try adding a new test. Add this to a column in `models/staging/schema.yml`:
   ```yaml
   - name: fare_amount
     tests:
       - not_null
   ```
   Then run `dbt test --select stg_yellow_trips` and see if it passes or fails.

**Questions to answer:**
1. How many schema tests are defined across all schema.yml files?
2. Which models have `unique` tests on their primary keys?
3. Did all tests pass? If not, which ones failed and why?
4. What SQL does dbt generate for a `unique` test?
5. What SQL does dbt generate for a `relationships` test?

---

## Exercise 7: Write Custom Data Tests

**Goal**: Write standalone SQL tests in the `tests/` directory.

**Steps:**

1. Review the existing custom tests:
   - `tests/assert_positive_fare.sql`
   - `tests/assert_no_orphaned_trips.sql`
   - `tests/assert_valid_trip_duration.sql`

2. Understand the convention: any rows returned by the query = test failure.

3. Run the custom tests:
   ```bash
   dbt test --select test_type:data
   ```

4. Write a new custom test. Create `tests/assert_valid_pickup_dates.sql`:
   ```sql
   -- Fail if any trip has a pickup date outside 2023 or in the future.
   select
       tpep_pickup_datetime
   from {{ ref('stg_yellow_trips') }}
   where tpep_pickup_datetime < '2023-01-01'
      or tpep_pickup_datetime > '2023-12-31'
   ```

5. Write another test. Create `tests/assert_reasonable_speed.sql`:
   ```sql
   -- Fail if any trip has an implied speed over 200 mph (likely a data quality issue).
   select
       trip_distance,
       trip_duration_minutes
   from {{ ref('int_trips_enriched') }}
   where trip_duration_minutes > 0
     and (trip_distance / (trip_duration_minutes / 60.0)) > 200
   ```

6. Run all tests:
   ```bash
   dbt test
   ```

**Questions to answer:**
1. What is the difference between schema tests and data tests?
2. How does dbt determine if a data test passes or fails?
3. Did `assert_no_orphaned_trips` pass? What would it mean if it failed?
4. Can you think of other data quality tests that would be useful for this dataset?

---

## Exercise 8: Create a Snapshot for SCD Type 2 on Zones

**Goal**: Use dbt snapshots to track changes to taxi zone metadata over time.

**Steps:**

1. Review `snapshots/scd_zones.sql`:
   ```bash
   cat snapshots/scd_zones.sql
   ```

2. Understand the snapshot configuration:
   - `strategy='check'`: Detects changes by comparing column values
   - `check_cols`: Which columns to watch for changes
   - `unique_key`: The natural key of the source

3. Run the snapshot for the first time:
   ```bash
   dbt snapshot
   ```

4. Query the snapshot table:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT LocationID, Borough, Zone, service_zone, dbt_valid_from, dbt_valid_to
       FROM snapshots.scd_zones
       ORDER BY LocationID, dbt_valid_from
       LIMIT 20
   ''').fetchdf())
   con.close()
   "
   ```

5. Understand the SCD Type 2 columns that dbt adds:
   - `dbt_scd_id`: Surrogate key for each version of a record
   - `dbt_updated_at`: When dbt detected the change
   - `dbt_valid_from`: When this version became active
   - `dbt_valid_to`: When this version was superseded (null = current)

6. To simulate a change, you would modify the source data and run `dbt snapshot`
   again. The old row would get a `dbt_valid_to` timestamp, and a new row would
   appear with the updated values.

**Questions to answer:**
1. What is SCD Type 2? How is it different from SCD Type 1?
2. What does `strategy='check'` mean vs `strategy='timestamp'`?
3. What columns would you see if a zone changed its Borough from "Manhattan" to a new borough?
4. Why is `dbt_valid_to` null for current records?
5. When would you use snapshots vs just overwriting the table?

---

## Exercise 9: Write Macros

**Goal**: Create reusable Jinja macros and understand how they work.

**Steps:**

1. Review the existing macros:
   - `macros/generate_schema_name.sql` -- controls schema naming
   - `macros/date_spine.sql` -- generates a continuous date series
   - `macros/clean_fare.sql` -- cleans and validates fare amounts

2. Understand how `clean_fare` is used. The staging models could be
   simplified to use this macro:
   ```sql
   -- Instead of inline CASE statements:
   {{ clean_fare('fare_amount') }} as fare_amount
   ```

3. Test the `date_spine` macro. Create a temporary model or run:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT unnest(generate_series(
           cast('2023-01-01' as date),
           cast('2023-01-10' as date),
           interval '1 day'
       )) as date_day
   ''').fetchdf())
   con.close()
   "
   ```

4. Write a new macro. Create `macros/safe_divide.sql`:
   ```sql
   {% macro safe_divide(numerator, denominator, default_value=0) %}
       case
           when {{ denominator }} is null or {{ denominator }} = 0
               then {{ default_value }}
           else {{ numerator }} * 1.0 / {{ denominator }}
       end
   {% endmacro %}
   ```

5. Write another macro. Create `macros/classify_trip_distance.sql`:
   ```sql
   {% macro classify_trip_distance(distance_column) %}
       case
           when {{ distance_column }} <= 1.0 then 'short'
           when {{ distance_column }} <= 5.0 then 'medium'
           when {{ distance_column }} <= 15.0 then 'long'
           else 'very_long'
       end
   {% endmacro %}
   ```

6. Compile and verify macros are valid:
   ```bash
   dbt compile
   ```

**Questions to answer:**
1. What is Jinja and how does dbt use it?
2. What is the purpose of `generate_schema_name` and when is it called?
3. How do macro arguments work in Jinja?
4. Where can macros be used? (models, tests, other macros, schema.yml?)
5. What is the difference between `{% macro %}` and `{{ macro_call() }}`?

---

## Exercise 10: Run dbt docs generate and Explore Lineage

**Goal**: Generate dbt documentation and explore the data lineage graph.

**Steps:**

1. First, make sure all models are built:
   ```bash
   dbt run
   dbt snapshot
   ```

2. Generate documentation:
   ```bash
   dbt docs generate
   ```

3. Serve the documentation site:
   ```bash
   dbt docs serve --port 8081
   ```
   Open `http://localhost:8081` in your browser.

4. Explore the documentation site:
   - **Model list**: Browse all models organized by directory
   - **Source list**: See the raw data sources you defined
   - **DAG/Lineage**: Click the graph icon (bottom right) to see the full
     dependency graph

5. In the lineage graph:
   - Click on `mart_zone_performance` and trace its lineage back to the sources
   - Identify how many models are between the raw data and the final mart
   - Click on individual nodes to see their SQL and documentation

6. Explore model documentation:
   - Click any model to see its description, columns, and tests
   - Notice how column descriptions from `schema.yml` appear here
   - Check which columns have tests defined

7. Try using `--select` with graph operators to understand dependencies:
   ```bash
   # Show everything upstream of mart_zone_performance
   dbt ls --select +mart_zone_performance

   # Show everything downstream of stg_yellow_trips
   dbt ls --select stg_yellow_trips+

   # Show only staging models
   dbt ls --select staging
   ```

**Questions to answer:**
1. How many total models are in the project (staging + intermediate + marts)?
2. What does the lineage graph look like for `mart_weather_impact`? How many
   upstream dependencies does it have?
3. Which staging model has the most downstream dependents?
4. What information does the documentation site show for each model?
5. How would you add a description for a specific column in the documentation?
6. What command regenerates the documentation after model changes?

---

## Bonus Challenges

### Challenge A: Build a New Mart Model

Create `mart_borough_comparison.sql` that compares taxi activity across
boroughs (Manhattan, Brooklyn, Queens, Bronx, Staten Island). Include metrics like:
- Average trip distance per borough
- Average fare and tip percentage per borough
- Number of trips per borough
- Peak hour of the day per borough

### Challenge B: Add a Pre-hook for Data Quality

Add a pre-hook to `mart_daily_metrics` that logs a warning if the model is
being built with less than 7 days of data:

```yaml
models:
  - name: mart_daily_metrics
    config:
      pre-hook: "SELECT CASE WHEN count(distinct trip_date) < 7 THEN 1/0 END FROM ..."
```

### Challenge C: Create an Analysis

Create `analyses/top_zones_by_month.sql` that finds the top 10 pickup zones by
trip volume for each month. Use dbt's analysis feature (compiled but not materialized).

### Challenge D: Test-Driven Development

Write all tests first for a new model `mart_payment_analysis`, then build
the model to make the tests pass. The model should include:
- Payment type breakdown (cash vs card vs other)
- Average fare and tip by payment type
- Percentage of trips by payment type per borough
- Trend of cashless payments over time
