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
using DuckDB's `read_csv_auto()` and `read_json_auto()` functions. However, you
still document the raw data as sources in YAML for lineage and documentation.

**Steps:**

1. Open `models/staging/schema.yml` and examine the source definitions.

2. Notice how each source table includes metadata about its file path and format.

3. Understand the difference between traditional dbt sources and our approach:
   - Traditional: `{{ source('raw', 'users') }}` resolves to a database table
   - Our approach: staging models use `read_csv_auto('../data/raw/users.csv')` directly,
     and source YAML serves as documentation

4. Run `dbt docs generate` and `dbt docs serve` to see the source documentation.

**Questions to answer:**
1. How many source tables are defined?
2. What file formats does the raw data include?
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

2. Start with `stg_users.sql`:
   ```bash
   dbt run --select stg_users
   ```

3. Query the result to verify the date parsing worked:
   ```bash
   dbt run --select stg_users && \
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('SELECT signup_date, count(*) FROM staging.stg_users GROUP BY 1 ORDER BY 1 LIMIT 10').fetchdf())
   con.close()
   "
   ```

4. Run all staging models:
   ```bash
   dbt run --select staging
   ```

5. Check for any errors. Common issues:
   - File path not found (check relative paths from the dbt project directory)
   - JSON parsing errors (check DuckDB's auto-detection)
   - Type casting failures (check date format handling)

**Questions to answer:**
1. How does `stg_users.sql` handle the three different date formats?
2. What does `stg_listening_events.sql` do about duplicate event IDs?
3. How are negative `listened_seconds` values handled?
4. What gender values exist after standardization?
5. Why do staging models use `view` materialization instead of `table`?

---

## Exercise 4: Build Intermediate Models

**Goal**: Build the intermediate layer that joins staging models and applies
business logic.

**Steps:**

1. Review `int_listens_enriched.sql`:
   - What tables does it join?
   - What derived metric does it compute?
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
       SELECT podcast_name_en, count(*) as events, round(avg(listen_pct), 1) as avg_pct
       FROM intermediate.int_listens_enriched
       GROUP BY 1 ORDER BY 2 DESC LIMIT 5
   ''').fetchdf())
   con.close()
   "
   ```

4. Review `int_user_sessions.sql`:
   - How is a "session" defined?
   - What window functions does it use?
   - What is the session gap threshold?

5. Check session statistics:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT
           count(*) as total_sessions,
           round(avg(events_in_session), 1) as avg_events,
           round(avg(total_listened_seconds / 60.0), 1) as avg_minutes
       FROM intermediate.int_user_sessions
   ''').fetchdf())
   con.close()
   "
   ```

**Questions to answer:**
1. How many columns does `int_listens_enriched` have?
2. What is the `listen_pct` column and how is it calculated?
3. What happens if a listening event references an episode that does not exist?
4. How does the sessionization algorithm detect session boundaries?
5. What is the session gap threshold in seconds?

---

## Exercise 5: Build Mart Models

**Goal**: Build the analytics-ready mart layer with aggregated, consumption-ready tables.

**Steps:**

1. Run all mart models:
   ```bash
   dbt run --select marts
   ```

2. Explore `mart_daily_listens`:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT event_date, sum(total_events) as events, sum(unique_listeners) as listeners
       FROM marts.mart_daily_listens
       GROUP BY 1 ORDER BY 1 DESC LIMIT 10
   ''').fetchdf())
   con.close()
   "
   ```

3. Explore `mart_podcast_performance`:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT podcast_name_en, total_listened_hours, completion_rate, listeners_per_episode
       FROM marts.mart_podcast_performance
       ORDER BY total_listened_hours DESC
   ''').fetchdf())
   con.close()
   "
   ```

4. Explore `mart_user_cohorts`:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT cohort_month, months_since_signup, active_users
       FROM marts.mart_user_cohorts
       WHERE subscription_type = 'premium'
       ORDER BY 1, 2
       LIMIT 15
   ''').fetchdf())
   con.close()
   "
   ```

5. Explore `mart_ad_revenue`:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT advertiser, sum(total_revenue_sar) as revenue,
              round(avg(click_through_rate), 2) as avg_ctr
       FROM marts.mart_ad_revenue
       GROUP BY 1 ORDER BY 2 DESC LIMIT 5
   ''').fetchdf())
   con.close()
   "
   ```

**Questions to answer:**
1. Why are mart models materialized as `table` instead of `view`?
2. What is the difference between `total_events` and `unique_listeners` in `mart_daily_listens`?
3. How is `completion_rate` calculated in `mart_podcast_performance`?
4. What does `months_since_signup` represent in `mart_user_cohorts`?
5. What is `click_through_rate` and how is it derived?

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
   dbt test --select stg_users

   # Show compiled SQL for a test
   cat target/compiled/podcast_analytics/models/staging/schema.yml/unique_stg_users_user_id.sql
   ```

4. Understand the four built-in test types:
   - **unique**: Verifies no duplicate values in a column
   - **not_null**: Verifies no null values
   - **accepted_values**: Verifies values are in a defined list
   - **relationships**: Verifies foreign key integrity

5. Look at the `relationships` test on `stg_episodes.podcast_id`:
   - What does it check?
   - What happens if an episode references a podcast_id not in `stg_podcasts`?

6. Try adding a new test. Add this to a column in `models/staging/schema.yml`:
   ```yaml
   - name: age
     tests:
       - not_null
   ```
   Then run `dbt test --select stg_users` and see if it passes or fails.

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
   - `tests/assert_positive_listen_duration.sql`
   - `tests/assert_no_orphaned_listens.sql`
   - `tests/assert_revenue_not_negative.sql`

2. Understand the convention: any rows returned by the query = test failure.

3. Run the custom tests:
   ```bash
   dbt test --select test_type:data
   ```

4. Write a new custom test. Create `tests/assert_valid_signup_dates.sql`:
   ```sql
   -- Fail if any user signed up before the platform launch (2018) or in the future.
   select
       user_id,
       signup_date
   from {{ ref('stg_users') }}
   where signup_date < '2015-01-01'
      or signup_date > current_date
   ```

5. Write another test. Create `tests/assert_listen_pct_reasonable.sql`:
   ```sql
   -- Fail if listen percentage exceeds 200% (likely a data quality issue).
   select
       event_id,
       listen_pct
   from {{ ref('int_listens_enriched') }}
   where listen_pct > 200
   ```

6. Run all tests:
   ```bash
   dbt test
   ```

**Questions to answer:**
1. What is the difference between schema tests and data tests?
2. How does dbt determine if a data test passes or fails?
3. Did `assert_no_orphaned_listens` pass? What would it mean if it failed?
4. Can you think of other data quality tests that would be useful for this dataset?

---

## Exercise 8: Create a Snapshot for SCD Type 2 on Podcasts

**Goal**: Use dbt snapshots to track changes to podcast metadata over time.

**Steps:**

1. Review `snapshots/scd_podcasts.sql`:
   ```bash
   cat snapshots/scd_podcasts.sql
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
       SELECT podcast_id, name_en, category, dbt_valid_from, dbt_valid_to
       FROM snapshots.scd_podcasts
       ORDER BY podcast_id, dbt_valid_from
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
3. What columns would you see if a podcast changed its category from "Business" to "Technology"?
4. Why is `dbt_valid_to` null for current records?
5. When would you use snapshots vs just overwriting the table?

---

## Exercise 9: Write Macros

**Goal**: Create reusable Jinja macros and understand how they work.

**Steps:**

1. Review the existing macros:
   - `macros/generate_schema_name.sql` -- controls schema naming
   - `macros/date_spine.sql` -- generates a continuous date series
   - `macros/clean_date.sql` -- parses dates in multiple formats

2. Understand how `clean_date` is used. The `stg_users.sql` model could be
   simplified to use this macro:
   ```sql
   -- Instead of the inline CASE statement:
   {{ clean_date('signup_date') }} as signup_date
   ```

3. Test the `date_spine` macro. Create a temporary model or run:
   ```bash
   python3 -c "
   import duckdb
   con = duckdb.connect('../data/warehouse.duckdb')
   print(con.sql('''
       SELECT unnest(generate_series(
           cast('2024-01-01' as date),
           cast('2024-01-10' as date),
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

5. Write another macro. Create `macros/classify_listen_engagement.sql`:
   ```sql
   {% macro classify_listen_engagement(listen_pct_column) %}
       case
           when {{ listen_pct_column }} >= 80 then 'high'
           when {{ listen_pct_column }} >= 40 then 'medium'
           when {{ listen_pct_column }} > 0 then 'low'
           else 'none'
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
   - Click on `mart_podcast_performance` and trace its lineage back to the sources
   - Identify how many models are between the raw data and the final mart
   - Click on individual nodes to see their SQL and documentation

6. Explore model documentation:
   - Click any model to see its description, columns, and tests
   - Notice how column descriptions from `schema.yml` appear here
   - Check which columns have tests defined

7. Try using `--select` with graph operators to understand dependencies:
   ```bash
   # Show everything upstream of mart_podcast_performance
   dbt ls --select +mart_podcast_performance

   # Show everything downstream of stg_users
   dbt ls --select stg_users+

   # Show only staging models
   dbt ls --select staging
   ```

**Questions to answer:**
1. How many total models are in the project (staging + intermediate + marts)?
2. What does the lineage graph look like for `mart_user_cohorts`? How many
   upstream dependencies does it have?
3. Which staging model has the most downstream dependents?
4. What information does the documentation site show for each model?
5. How would you add a description for a specific column in the documentation?
6. What command regenerates the documentation after model changes?

---

## Bonus Challenges

### Challenge A: Build a New Mart Model

Create `mart_platform_comparison.sql` that compares listening behavior across
platforms (ios, android, web, smart_speaker, car_play). Include metrics like:
- Average session length per platform
- Average listen completion percentage per platform
- Number of unique users per platform
- Distribution of subscription types per platform

### Challenge B: Add a Pre-hook for Data Quality

Add a pre-hook to `mart_daily_listens` that logs a warning if the model is
being built with less than 7 days of data:

```yaml
models:
  - name: mart_daily_listens
    config:
      pre-hook: "SELECT CASE WHEN count(distinct event_date) < 7 THEN 1/0 END FROM ..."
```

### Challenge C: Create an Analysis

Create `analyses/top_users_by_month.sql` that finds the top 10 users by listening
hours for each month. Use dbt's analysis feature (compiled but not materialized).

### Challenge D: Test-Driven Development

Write all tests first for a new model `mart_episode_performance`, then build
the model to make the tests pass. The model should include:
- Episode-level metrics (total listens, unique listeners, avg completion)
- Ranking within its podcast
- Days since publication
