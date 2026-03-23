# Module 09: Data Quality

## Why Data Quality Matters

Bad data leads to bad decisions. In a podcast analytics platform, poor data quality has
concrete consequences:

- **Revenue loss**: If ad impression counts are wrong (duplicates, missing events), advertisers
  lose trust and reduce spend. A 5% overcount in impressions can trigger contract penalties.
- **Broken recommendations**: If listening events contain bot traffic or duplicate plays, the
  recommendation engine promotes content nobody actually listens to.
- **Misleading KPIs**: If user signup dates arrive in three different formats and some get
  parsed wrong, your "monthly active users" metric becomes fiction.
- **Wasted engineering time**: Teams spend 40-60% of their time finding and fixing data issues
  instead of building features. Every downstream consumer re-discovers the same problems.
- **Regulatory risk**: Incorrect user demographics or missing consent fields can violate GDPR
  or local data protection laws.

Data quality is not a nice-to-have. It is the foundation that determines whether your data
platform creates value or creates confusion.

## Data Quality Dimensions

There are six core dimensions for measuring data quality. Each answers a different question
about your data:

### 1. Accuracy

Does the data reflect reality?

- A user's age is 250 -- that is inaccurate.
- A negative `startup_time_ms` in CDN logs is physically impossible.
- A listening duration longer than the episode itself is wrong.

Accuracy is hard to validate without a source of truth, but you can catch obvious violations
with range checks and cross-references.

### 2. Completeness

Is all expected data present?

- Are there null values where nulls should not exist (e.g., user email, event timestamp)?
- Are there missing dates in a daily partition sequence?
- Did a source system fail to send data for an entire day?

Completeness checks look for gaps -- null fields, missing rows, missing partitions.

### 3. Consistency

Does the same fact look the same everywhere?

- Gender stored as "m", "M", "male", "Male", "MALE" across different records.
- Dates in "2024-01-15", "01/15/2024", "15-01-2024" formats in the same column.
- A user's country is "SA" in the users table but "KSA" in the events table.

Inconsistency forces every consumer to write their own normalization logic, which leads to
divergent results.

### 4. Timeliness

Does the data arrive when expected?

- If listening events for today are not available until tomorrow afternoon, real-time dashboards
  show stale numbers.
- Late-arriving events (events with timestamps days before they actually arrive) distort
  daily aggregates after they have already been published.
- A source system that stops sending data silently is worse than one that fails loudly.

### 5. Validity

Does the data conform to expected formats and business rules?

- Email addresses match a pattern like `*@*.*`.
- Event types are one of: play, pause, seek, complete, skip.
- `listened_seconds` is non-negative.
- `revenue_sar` is non-negative for ad impressions.

Validity checks enforce the schema contract between producers and consumers.

### 6. Uniqueness

Is each entity represented exactly once?

- Duplicate `event_id` values in listening events mean double-counting.
- Duplicate `user_id` values in the users table mean ambiguous identity.
- Duplicate CDN log entries inflate bandwidth metrics.

Deduplication is one of the most common data engineering tasks because source systems
frequently send duplicates (retries, at-least-once delivery, replay from Kafka).

## Great Expectations Framework

[Great Expectations](https://greatexpectations.io/) is an open-source Python framework for
data validation. It provides a declarative way to define what "good data" looks like.

### Core Concepts

- **Expectation**: A single assertion about your data. Example:
  `expect_column_values_to_not_be_null(column="user_id")`.
- **Expectation Suite**: A collection of expectations for one dataset.
- **Validator**: Runs expectations against a batch of data.
- **Data Docs**: Auto-generated HTML reports showing pass/fail results.
- **Checkpoint**: A runnable unit that validates data and optionally triggers actions on
  failure.

### Example Expectations

```python
# Column should never be null
validator.expect_column_values_to_not_be_null("user_id")

# Values should be in a known set
validator.expect_column_values_to_be_in_set(
    "event_type", ["play", "pause", "seek", "complete", "skip"]
)

# Values should be in a numeric range
validator.expect_column_values_to_be_between(
    "listened_seconds", min_value=0, max_value=36000
)

# Column should have no duplicates
validator.expect_column_values_to_be_unique("event_id")

# Table should have a minimum number of rows
validator.expect_table_row_count_to_be_between(min_value=1000)
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
  - name: stg_users
    columns:
      - name: user_id
        tests:
          - unique
          - not_null
      - name: email
        tests:
          - not_null
      - name: subscription_type
        tests:
          - accepted_values:
              values: ['free', 'premium', 'premium_annual', 'trial']
```

### Custom Data Tests

```sql
-- tests/assert_no_negative_listen_seconds.sql
SELECT *
FROM {{ ref('stg_listening_events') }}
WHERE listened_seconds < 0
```

If this query returns any rows, the test fails.

### Source Freshness

```yaml
# models/sources.yml
sources:
  - name: raw
    tables:
      - name: listening_events
        loaded_at_field: timestamp
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}
```

dbt tests run after transformations. They catch problems early -- before bad data reaches
dashboards and reports.

## Data Contracts

A data contract is a formal agreement between a data producer and its consumers about:

1. **Schema**: What columns exist, their types, and whether they are nullable.
2. **Quality rules**: What invariants must hold (no nulls, unique keys, valid ranges).
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
# contracts/listening_events_contract.yml
name: listening_events
owner: platform-team@podcast.com
description: User listening activity events
schema:
  - name: event_id
    type: string
    nullable: false
    unique: true
  - name: user_id
    type: string
    nullable: false
quality_rules:
  - rule: listened_seconds >= 0
  - rule: event_type in ['play', 'pause', 'seek', 'complete', 'skip']
freshness:
  max_delay_hours: 6
```

## Data Observability

Data observability applies the same principles as application monitoring (metrics, logs,
alerts) to data pipelines. The five pillars:

### 1. Freshness

How recent is the latest record? If your listening events table was last updated 36 hours ago,
something is broken. Freshness monitoring detects silent failures -- when a pipeline stops
producing data without raising an error.

### 2. Volume

How many records arrived today? If you normally receive 50,000 listening events per day and
today you got 500, that is a problem. Volume anomaly detection compares today's count to a
historical baseline (rolling average, standard deviation bands).

### 3. Schema

Did the schema change? A new column is usually fine. A dropped column or type change breaks
downstream consumers. Schema monitoring tracks DDL changes and alerts on breaking changes.

### 4. Distribution

Has the statistical profile of a column changed? If `listened_seconds` normally averages 600
and today it averages 50, something is wrong -- maybe a bug is truncating values, or bot
traffic is flooding the system with short plays.

### 5. Lineage

Where did this data come from, and what depends on it? When a quality issue is found, lineage
tells you which upstream source is responsible and which downstream dashboards are affected.

## SLAs and SLOs for Data Pipelines

### SLA (Service Level Agreement)

A promise to stakeholders: "Daily listening analytics will be available by 6:00 AM local time,
covering all events up to midnight."

SLAs are external commitments. Breaking them has business consequences.

### SLO (Service Level Objective)

An internal target that is stricter than the SLA: "The pipeline will complete by 4:00 AM,
giving us a 2-hour buffer before the SLA deadline."

### SLI (Service Level Indicator)

The metric you actually measure: "Pipeline completion time", "Data freshness in hours",
"Percentage of records passing quality checks."

### Example SLOs for This Platform

| Pipeline | SLO | SLI |
|----------|-----|-----|
| Listening events ingestion | < 2 hours delay | max(now - max(timestamp)) |
| User data sync | < 6 hours delay | max(now - max(signup_date)) |
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
rolling_mean = daily_counts.rolling(window=7).mean()
rolling_std = daily_counts.rolling(window=7).std()
upper_bound = rolling_mean + 2 * rolling_std
lower_bound = rolling_mean - 2 * rolling_std

is_anomaly = (today_count < lower_bound) | (today_count > upper_bound)
```

### Percentage Change Check

```python
pct_change = abs(today_count - yesterday_count) / yesterday_count
is_anomaly = pct_change > 0.50  # >50% drop or spike
```

### Day-of-Week Seasonality

Podcast listening has weekly patterns (weekdays vs weekends). Compare today's count to the
same day last week, not yesterday:

```python
same_day_last_week = daily_counts.shift(7)
pct_change = abs(today - same_day_last_week) / same_day_last_week
```

## Root Cause Analysis for Data Quality Issues

When a quality check fails, follow this systematic approach:

### 1. Identify the Scope

- Which dataset is affected?
- Which specific columns or records?
- When did the issue start? (Compare today's profile to yesterday's.)

### 2. Trace the Lineage

- Where does this data come from? (Source system, API, Kafka topic)
- What transformations happened between source and failure point?
- Did an upstream pipeline change recently?

### 3. Check Common Causes

| Symptom | Likely Cause |
|---------|-------------|
| Sudden null spike | Source system schema change or API error |
| Duplicate surge | Kafka consumer replay, at-least-once delivery |
| Volume drop to zero | Pipeline failure, source system outage |
| Volume drop 50% | Partial load, timezone bug, filter change |
| New unexpected values | Source added an enum value without notice |
| Date parse failures | Source changed date format |

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
pip install pandas duckdb pyyaml
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

Each script prints detailed output showing what was checked and what failed. The data at
`data/raw/` has intentional quality issues -- mixed date formats, nulls, duplicates, negative
values, inconsistent gender codes, and more. These exercises teach you to find and handle
real-world data problems.
