# Module 09: Data Quality

## Why Data Quality Matters

Bad data leads to bad decisions. In a taxi trip analytics platform, poor data quality has
concrete consequences:

- **Revenue loss**: If fare amounts are miscalculated (negative fares, missing surcharges),
  the TLC loses revenue and drivers lose trust. A 5% undercount in trip fares can mean
  millions in lost revenue across the fleet.
- **Broken analytics**: If trip records contain impossible distances or zero passenger counts,
  fleet optimization models produce unreliable results and route planning becomes fiction.
- **Misleading KPIs**: If pickup/dropoff location IDs reference nonexistent zones, or
  timestamps arrive in inconsistent formats, metrics like "average trip duration by borough"
  become meaningless.
- **Wasted engineering time**: Teams spend 40-60% of their time finding and fixing data issues
  instead of building features. Every downstream consumer re-discovers the same problems.
- **Regulatory risk**: Incorrect trip records or missing fields can violate TLC reporting
  requirements and trigger compliance audits.

Data quality is not a nice-to-have. It is the foundation that determines whether your data
platform creates value or creates confusion.

## Data Quality Dimensions

There are six core dimensions for measuring data quality. Each answers a different question
about your data:

### 1. Accuracy

Does the data reflect reality?

- A trip distance of -5 miles is physically impossible.
- A fare amount of $50,000 for a 2-mile trip is inaccurate.
- A trip duration longer than 24 hours for a standard metered ride is wrong.

Accuracy is hard to validate without a source of truth, but you can catch obvious violations
with range checks and cross-references.

### 2. Completeness

Is all expected data present?

- Are there null values where nulls should not exist (e.g., pickup datetime, location IDs)?
- Are there missing months in a monthly partition sequence?
- Did a vendor fail to send trip data for an entire day?

Completeness checks look for gaps -- null fields, missing rows, missing partitions.

### 3. Consistency

Does the same fact look the same everywhere?

- VendorID stored as 1, 2 in yellow trips but as text codes in other systems.
- Timestamps in different timezone offsets across different parquet files.
- A location ID is 132 in the trip table but maps to a different zone in different lookups.

Inconsistency forces every consumer to write their own normalization logic, which leads to
divergent results.

### 4. Timeliness

Does the data arrive when expected?

- If January trip data is not available until March, monthly dashboards show stale numbers.
- Late-arriving trip records (records filed days after the actual trip) distort daily
  aggregates after they have already been published.
- A vendor system that stops sending data silently is worse than one that fails loudly.

### 5. Validity

Does the data conform to expected formats and business rules?

- Payment type is one of: 1 (Credit card), 2 (Cash), 3 (No charge), 4 (Dispute), 5 (Unknown), 6 (Voided).
- `passenger_count` is between 0 and 9.
- `trip_distance` is non-negative.
- `PULocationID` and `DOLocationID` are between 1 and 265.

Validity checks enforce the schema contract between producers and consumers.

### 6. Uniqueness

Is each entity represented exactly once?

- Duplicate trip records mean double-counting revenue and trip volumes.
- Duplicate zone entries in the lookup table create ambiguous location mappings.
- Duplicate vendor records inflate fleet size metrics.

Deduplication is one of the most common data engineering tasks because source systems
frequently send duplicates (retries, at-least-once delivery, reprocessed batches).

## Great Expectations Framework

[Great Expectations](https://greatexpectations.io/) is an open-source Python framework for
data validation. It provides a declarative way to define what "good data" looks like.

### Core Concepts

- **Expectation**: A single assertion about your data. Example:
  `expect_column_values_to_not_be_null(column="PULocationID")`.
- **Expectation Suite**: A collection of expectations for one dataset.
- **Validator**: Runs expectations against a batch of data.
- **Data Docs**: Auto-generated HTML reports showing pass/fail results.
- **Checkpoint**: A runnable unit that validates data and optionally triggers actions on
  failure.

### Example Expectations

```python
# Column should never be null
validator.expect_column_values_to_not_be_null("tpep_pickup_datetime")

# Values should be in a known set
validator.expect_column_values_to_be_in_set(
    "payment_type", [1, 2, 3, 4, 5, 6]
)

# Values should be in a numeric range
validator.expect_column_values_to_be_between(
    "trip_distance", min_value=0, max_value=500
)

# Table should have a minimum number of rows
validator.expect_table_row_count_to_be_between(min_value=10000)
```

### When to Use Great Expectations

Great Expectations shines when you need:
- Reusable, version-controlled validation rules
- Auto-generated documentation of data quality
- Integration with Airflow, Spark, or other orchestrators
- A checkpoint that can block a pipeline on failure

For this module, we build equivalent checks from scratch using pandas and DuckDB so you
understand the mechanics. In production, adopt Great Expectations or a similar framework
(Soda, dbt tests) instead of maintaining custom code.

## dbt Tests

dbt has built-in testing that runs as part of your transformation pipeline.

### Schema Tests (Built-in)

```yaml
# models/schema.yml
models:
  - name: stg_yellow_trips
    columns:
      - name: tpep_pickup_datetime
        tests:
          - not_null
      - name: PULocationID
        tests:
          - not_null
      - name: payment_type
        tests:
          - accepted_values:
              values: [1, 2, 3, 4, 5, 6]
```

### Custom Data Tests

```sql
-- tests/assert_no_negative_fares.sql
SELECT *
FROM {{ ref('stg_yellow_trips') }}
WHERE fare_amount < -50
```

If this query returns any rows, the test fails.

### Source Freshness

```yaml
# models/sources.yml
sources:
  - name: raw
    tables:
      - name: yellow_tripdata
        loaded_at_field: tpep_pickup_datetime
        freshness:
          warn_after: {count: 30, period: day}
          error_after: {count: 60, period: day}
```

dbt tests run after transformations. They catch problems early -- before bad data reaches
dashboards and reports.

## Data Contracts

A data contract is a formal agreement between a data producer and its consumers about:

1. **Schema**: What columns exist, their types, and whether they are nullable.
2. **Quality rules**: What invariants must hold (no nulls, valid ranges, valid location IDs).
3. **SLAs**: When data will be available and how fresh it will be.
4. **Ownership**: Who is responsible when things break.
5. **Semantics**: What each field actually means (a data dictionary).

### Why Contracts Matter

Without contracts, producers change schemas without warning and break every downstream
consumer. With contracts:
- Schema changes require explicit negotiation.
- Quality expectations are codified, not assumed.
- Ownership is clear, so incidents get routed to the right team.

### Contract as Code

Store contracts as YAML files in version control. Validate incoming data against the contract
before loading it into the warehouse. See `contracts/` directory for examples.

```yaml
# contracts/yellow_trips_contract.yml
name: yellow_tripdata
owner: taxi-data-team@nyctlc.gov
description: Yellow taxi trip records from NYC TLC
schema:
  - name: tpep_pickup_datetime
    type: datetime
    nullable: false
  - name: PULocationID
    type: integer
    nullable: false
quality_rules:
  - rule: fare_amount >= -50 and fare_amount <= 5000
  - rule: payment_type in [1, 2, 3, 4, 5, 6]
freshness:
  max_delay_hours: 720
```

## Data Observability

Data observability applies the same principles as application monitoring (metrics, logs,
alerts) to data pipelines. The five pillars:

### 1. Freshness

How recent is the latest record? If your yellow trip data was last updated 90 days ago,
something is broken. Freshness monitoring detects silent failures -- when a pipeline stops
producing data without raising an error.

### 2. Volume

How many records arrived this month? If you normally receive 500,000 yellow taxi trips per
month and this month you got 5,000, that is a problem. Volume anomaly detection compares
current counts to a historical baseline (rolling average, standard deviation bands).

### 3. Schema

Did the schema change? A new column is usually fine. A dropped column or type change breaks
downstream consumers. Schema monitoring tracks DDL changes and alerts on breaking changes.

### 4. Distribution

Has the statistical profile of a column changed? If `trip_distance` normally averages 3.5
miles and this month it averages 0.5, something is wrong -- maybe a bug is truncating values,
or a GPS issue is flooding the system with short-distance records.

### 5. Lineage

Where did this data come from, and what depends on it? When a quality issue is found, lineage
tells you which upstream source is responsible and which downstream dashboards are affected.

## SLAs and SLOs for Data Pipelines

### SLA (Service Level Agreement)

A promise to stakeholders: "Monthly taxi trip analytics will be available by the 5th of the
following month, covering all trips from the previous month."

SLAs are external commitments. Breaking them has business consequences.

### SLO (Service Level Objective)

An internal target that is stricter than the SLA: "The pipeline will complete by the 3rd,
giving us a 2-day buffer before the SLA deadline."

### SLI (Service Level Indicator)

The metric you actually measure: "Pipeline completion time", "Data freshness in days",
"Percentage of records passing quality checks."

### Example SLOs for This Platform

| Pipeline | SLO | SLI |
|----------|-----|-----|
| Yellow trip ingestion | < 30 days delay | max(now - max(tpep_pickup_datetime)) |
| Zone lookup sync | < 90 days delay | max(now - last_modified) |
| Quality score | > 95% per dataset | Weighted quality score |
| Duplicate rate | < 0.1% | count(dupes) / count(*) |
| Null rate on required fields | < 1% | count(nulls) / count(*) |

## Circuit Breakers

A circuit breaker is a safety mechanism that halts pipeline execution when data quality drops
below an acceptable threshold. It prevents bad data from propagating downstream.

### How It Works

```
[Ingest Data] --> [Run Quality Checks] --> [Circuit Breaker Decision]
                                                |           |
                                             PASS         FAIL
                                              |             |
                                     [Load to Warehouse]  [Halt Pipeline]
                                                          [Send Alert]
                                                          [Log Failure]
```

### Implementation Pattern

```python
class CircuitBreaker:
    def __init__(self, threshold=90.0):
        self.threshold = threshold  # minimum acceptable quality score

    def evaluate(self, quality_score, dataset_name):
        if quality_score < self.threshold:
            raise DataQualityError(
                f"Circuit breaker OPEN for {dataset_name}: "
                f"score {quality_score} < threshold {self.threshold}"
            )
        return True  # circuit closed, proceed
```

### When to Trip the Circuit Breaker

- **Critical failures** (score < 50): Missing primary keys, schema mismatch, zero rows.
  Action: halt immediately, page on-call.
- **Warning failures** (score 50-90): Elevated nulls, unusual volume, minor duplicates.
  Action: log warning, proceed with caution, alert via Slack.
- **Passing** (score > 90): Normal operation. Log metrics for trending.

The threshold depends on the business context. A financial reconciliation pipeline might
require 99.9%. An internal analytics pipeline might tolerate 85%.

## Anomaly Detection on Data Volumes

Simple statistical methods catch most volume anomalies without machine learning:

### Rolling Average with Standard Deviation Bands

```python
rolling_mean = monthly_counts.rolling(window=3).mean()
rolling_std = monthly_counts.rolling(window=3).std()
upper_bound = rolling_mean + 2 * rolling_std
lower_bound = rolling_mean - 2 * rolling_std

is_anomaly = (this_month_count < lower_bound) | (this_month_count > upper_bound)
```

### Percentage Change Check

```python
pct_change = abs(this_month_count - last_month_count) / last_month_count
is_anomaly = pct_change > 0.50  # >50% drop or spike
```

### Seasonal Patterns

Taxi ridership has seasonal patterns (holidays, weather, events). Compare this month's count
to the same month last year, not just the previous month:

```python
same_month_last_year = monthly_counts.shift(12)
pct_change = abs(this_month - same_month_last_year) / same_month_last_year
```

## Root Cause Analysis for Data Quality Issues

When a quality check fails, follow this systematic approach:

### 1. Identify the Scope

- Which dataset is affected?
- Which specific columns or records?
- When did the issue start? (Compare this month's profile to last month's.)

### 2. Trace the Lineage

- Where does this data come from? (TLC vendor system, taxi meter, GPS device)
- What transformations happened between source and failure point?
- Did an upstream pipeline change recently?

### 3. Check Common Causes

| Symptom | Likely Cause |
|---------|-------------|
| Sudden null spike | Vendor system schema change or meter malfunction |
| Duplicate surge | Reprocessed batch, at-least-once delivery |
| Volume drop to zero | Pipeline failure, vendor system outage |
| Volume drop 50% | Partial load, timezone bug, filter change |
| New unexpected values | TLC added a payment type or rate code |
| Negative fare amounts | Refunds, adjustments, or data entry errors |

### 4. Fix and Prevent

- Fix the immediate issue (backfill, reprocess, manual correction).
- Add a quality check that would have caught it earlier.
- Update the data contract if the source legitimately changed.
- Communicate with upstream producers to prevent recurrence.

## Exercises Overview

This module includes 9 exercises that build a complete data quality system:

1. **Profile all raw datasets** -- understand what quality issues exist
2. **Build custom quality checks** -- completeness, uniqueness, range, pattern
3. **Referential integrity** -- validate foreign keys across tables
4. **Freshness monitoring** -- detect stale data
5. **Volume anomaly detection** -- catch unusual record counts
6. **Quality scoring** -- assign 0-100 scores to each dataset
7. **Circuit breakers** -- halt pipelines on critical failures
8. **Quality report** -- generate a comprehensive report
9. **Data contracts** -- define and enforce schema + quality rules

Work through them in order. Each exercise builds on concepts from the previous ones.

## Running the Solutions

All solutions use pandas and DuckDB. Install dependencies:

```bash
pip install pandas duckdb pyyaml pyarrow
```

Run any solution from the repository root:

```bash
python module-09-data-quality/solutions/profile_all_data.py
python module-09-data-quality/solutions/quality_checks.py
python module-09-data-quality/solutions/referential_integrity.py
python module-09-data-quality/solutions/freshness_checks.py
python module-09-data-quality/solutions/volume_anomaly.py
python module-09-data-quality/solutions/quality_scorer.py
python module-09-data-quality/solutions/circuit_breaker.py
python module-09-data-quality/solutions/quality_report.py
python module-09-data-quality/solutions/data_contracts.py
```

Each script prints detailed output showing what was checked and what failed. The taxi trip
data has real-world quality issues -- null passenger counts, zero-distance trips, negative
fares, location IDs outside valid ranges, and more. These exercises teach you to find and
handle real-world data problems.
