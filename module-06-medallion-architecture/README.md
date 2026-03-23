# Module 06 - Medallion Architecture

## Overview

The **Medallion Architecture** (also called multi-hop architecture) is a data design
pattern that organizes data in a lakehouse into three progressive layers of quality
and refinement: **Bronze**, **Silver**, and **Gold**. Each layer serves a distinct
purpose and builds upon the previous one, creating a clear data quality pipeline
from raw ingestion to business-ready analytics.

```
  Raw Sources          Bronze             Silver              Gold
 +-----------+     +------------+     +-------------+    +--------------+
 | JSON      |---->| Raw as-is  |---->| Cleaned     |--->| Aggregated   |
 | CSV       |     | Append-only|     | Deduplicated|    | Business KPIs|
 | JSONL     |     | Schema-on- |     | Typed &     |    | Dashboard-   |
 | Logs      |     | read       |     | Validated   |    | ready tables |
 +-----------+     +------------+     +-------------+    +--------------+
                     "Land it"         "Refine it"         "Serve it"
```

---

## Why Layers Matter

### Separation of Concerns

Each layer has one job. Bronze is responsible for reliable ingestion. Silver handles
data quality. Gold focuses on business logic. When something breaks, you know exactly
where to look. When business requirements change, you only modify the Gold layer
without re-ingesting everything.

### Data Quality Progression

Raw data is messy: mixed date formats, duplicate records, null values, late-arriving
events. Rather than trying to fix everything in a single monolithic ETL job, the
medallion pattern applies transformations incrementally. Each layer adds a specific
set of guarantees.

### Reprocessing and Recovery

Because Bronze retains the raw data, you can always reprocess Silver and Gold layers
when you discover bugs in your transformation logic or when business definitions
change. This is not possible with traditional ETL that transforms data in-place.

### Team Autonomy

Data engineers own Bronze and Silver. Analytics engineers and data scientists work
with Silver and Gold. Each team operates on the layer appropriate to their skill set
and responsibilities.

---

## Delta Lake: Why It Matters

Delta Lake is an open-source storage layer that brings reliability to data lakes.
While this module uses Parquet files for compatibility (PySpark and Delta Lake proper
are covered in Module 07), it is important to understand why Delta Lake improves on
raw Parquet.

### What Delta Lake Adds Over Raw Parquet

| Capability | Raw Parquet | Delta Lake |
|---|---|---|
| **ACID Transactions** | No. A failed write can leave partial/corrupt files. | Yes. Writes are atomic -- they either fully succeed or fully roll back. |
| **Time Travel** | No. Once overwritten, previous data is gone. | Yes. Every version is retained. You can query data as of any past version or timestamp. |
| **MERGE (Upsert)** | Not supported. You must read-modify-write manually. | Native MERGE command: `MERGE INTO target USING source ON condition WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`. |
| **Schema Evolution** | Manual. Adding columns requires rewriting all files or careful union logic. | Built-in. `mergeSchema` option automatically reconciles new columns. |
| **Schema Enforcement** | None. Any file can be dumped into a directory. | Rejects writes that do not match the table schema unless evolution is enabled. |
| **Audit History** | None. | Full transaction log showing who changed what and when. |
| **Concurrent Writes** | Unsafe. Two writers can corrupt data. | Optimistic concurrency control handles multiple writers safely. |

### The Delta Transaction Log

Delta Lake stores a `_delta_log/` directory alongside the Parquet data files. This
log contains JSON entries for every transaction (add file, remove file, metadata
change). The log is the source of truth -- it defines which Parquet files are
"current" and enables time travel, rollback, and ACID guarantees.

---

## The Three Layers in Detail

### Bronze Layer (Raw / Landing)

**Purpose:** Faithful copy of source data. No transformations, no filtering, no
deduplication.

**Principles:**
- **Append-only.** Never update or delete records in Bronze. If a source sends
  corrected data, append it -- deduplication happens in Silver.
- **Schema-on-read.** Store data in its original schema. If the source adds a new
  column tomorrow, Bronze should not break.
- **Metadata enrichment.** Add ingestion metadata: `_ingested_at` timestamp,
  `_source_file` name, `_batch_id`. This enables lineage and debugging.
- **Partitioning.** Partition by ingestion date (`_ingested_date`) to enable
  efficient incremental processing.

**What gets stored:**
- Raw JSON parsed into columnar format (Parquet) but with original field names and
  values
- CSV files read with minimal type inference
- JSONL files preserved record-by-record

**Example quality issues that Bronze retains:**
- Mixed date formats (`2024-01-15`, `15/01/2024`, `01-15-2024`)
- Null/missing values
- Duplicate records
- Inconsistent categorical values (`male`, `Male`, `m`, `M`)
- Negative values where only positive make sense
- Late-arriving events (events timestamped days before the file date)

```python
# Bronze philosophy: land it as-is, add metadata
df["_ingested_at"] = datetime.now().isoformat()
df["_source_file"] = source_filename
df.to_parquet(bronze_path, partition_cols=["_ingested_date"])
```

### Silver Layer (Cleaned / Conformed)

**Purpose:** Single source of truth for each entity. Cleaned, deduplicated, typed,
validated -- but still at the same granularity as the source.

**Principles:**
- **Deduplication.** Remove exact duplicates and apply business logic for near-
  duplicates (e.g., keep the latest record per primary key).
- **Type casting.** Parse dates into proper date types. Cast numeric strings to
  numbers. Standardize categorical values.
- **Validation.** Apply data quality rules: non-null primary keys, valid ranges
  (age between 0 and 120), referential integrity where possible.
- **Normalization.** Standardize formats: gender values to `male`/`female`/`unknown`,
  country codes to ISO format, timestamps to UTC.
- **Derived columns.** Add useful computed fields: `signup_year`, `age_group`,
  `listening_duration_minutes`.
- **Quarantine.** Records that fail critical validations are written to a separate
  `_quarantine` table for investigation, not silently dropped.

**What changes from Bronze:**
- Duplicates removed
- Date columns parsed to consistent `YYYY-MM-DD` format
- Gender values normalized (`m`, `M`, `male`, `Male` all become `male`)
- Null values handled (filled with defaults or flagged)
- Invalid records quarantined
- Consistent column naming (snake_case)

```python
# Silver philosophy: clean it, keep it granular
df = df.drop_duplicates(subset=["user_id"])
df["signup_date"] = pd.to_datetime(df["signup_date"], format="mixed", dayfirst=False)
df["gender"] = df["gender"].str.lower().map(GENDER_MAP).fillna("unknown")
```

### Gold Layer (Business / Aggregated)

**Purpose:** Business-level tables optimized for specific analytical use cases.
Aggregated, joined, and shaped for dashboards, reports, and ML features.

**Principles:**
- **Use-case driven.** Each Gold table serves a specific business question or
  dashboard. Do not create a "general purpose" Gold table.
- **Pre-aggregated.** Daily metrics, weekly rollups, cohort tables. End users should
  not need to write complex GROUP BY queries.
- **Joined and enriched.** Combine data from multiple Silver tables. A listening
  metric table joins events with users, episodes, and podcasts.
- **Slowly changing dimensions.** Handle historical changes in dimension attributes
  (e.g., a podcast changing category).
- **Optimized for read.** Sorted, partitioned, and columnar for fast analytical
  queries.

**Example Gold tables:**
- `gold_daily_listening_metrics` -- DAU, total listens, avg completion rate per day
- `gold_podcast_performance` -- Per-podcast aggregate performance metrics
- `gold_user_retention_cohorts` -- Cohort retention analysis by signup month
- `gold_ad_revenue` -- Revenue by advertiser, campaign, ad type, and time period

```python
# Gold philosophy: answer business questions directly
daily_metrics = (
    silver_events
    .groupby("event_date")
    .agg(
        dau=("user_id", "nunique"),
        total_listens=("event_id", "count"),
        avg_completion=("completion_rate", "mean"),
    )
)
```

---

## Schema Evolution Handling

As source systems evolve, new columns appear. The medallion architecture handles
this gracefully:

1. **Bronze:** New columns are automatically captured because we read source files
   dynamically. The `_source_file` and `_ingested_at` metadata tells us when the
   new column first appeared.

2. **Silver:** Schema evolution requires an explicit decision. Options:
   - Add the column with null backfill for historical records
   - Maintain a schema registry that tracks expected vs. actual schemas
   - Use union-by-name (Parquet default) to merge old and new schemas

3. **Gold:** Usually unaffected unless the new column is relevant to a business
   metric, in which case the Gold table definition is updated.

In Delta Lake, schema evolution is handled with:
```python
df.write.format("delta").option("mergeSchema", "true").mode("append").save(path)
```

In this module (using Parquet + pandas), we simulate schema evolution by detecting
new columns and using `pd.concat` with `join="outer"` to merge schemas.

---

## Comparison with Traditional ETL

| Aspect | Traditional ETL | Medallion Architecture |
|---|---|---|
| **Data retention** | Source data often discarded after transformation | Bronze retains everything |
| **Reprocessing** | Requires re-extraction from source systems | Reprocess from Bronze at any time |
| **Debugging** | Difficult -- intermediate states not saved | Each layer is queryable |
| **Schema changes** | Often requires pipeline redesign | Handled incrementally per layer |
| **Team collaboration** | Monolithic pipeline owned by one team | Clear layer ownership |
| **Testing** | End-to-end only | Each layer testable independently |
| **Time to insight** | Must wait for full pipeline | Bronze available immediately |
| **Complexity** | Hidden in one big job | Distributed across clear stages |

---

## Databricks Unity Catalog Concepts

While this module uses local files and DuckDB, production medallion architectures
often run on Databricks with Unity Catalog for governance. Key concepts:

### Three-Level Namespace

```
catalog.schema.table
  |       |      |
  |       |      +-- e.g., gold_daily_metrics
  |       +--------- e.g., gold, silver, bronze
  +----------------- e.g., podcast_platform
```

Example fully qualified names:
- `podcast_platform.bronze.raw_users`
- `podcast_platform.silver.clean_users`
- `podcast_platform.gold.daily_listening_metrics`

### Governance Features

- **Access Control:** Fine-grained permissions at catalog, schema, or table level.
  Data engineers get write access to bronze/silver; analysts get read-only access
  to silver/gold.
- **Data Lineage:** Unity Catalog automatically tracks which tables were used to
  produce which other tables. You can visualize the full flow from Bronze to Gold.
- **Audit Logging:** Every query and table access is logged for compliance.
- **Data Discovery:** Searchable catalog of all tables with descriptions, tags, and
  ownership information.

### Managed vs. External Tables

- **Managed tables:** Unity Catalog controls both metadata and data files. Dropping
  the table deletes the data.
- **External tables:** Unity Catalog manages metadata only. Data lives in your own
  cloud storage (S3, ADLS, GCS). Dropping the table keeps the data.

For a podcast platform, you might use managed tables for Gold (curated, governed)
and external tables for Bronze (controlled storage location, retained even if
catalog metadata changes).

---

## Module Structure

```
module-06-medallion-architecture/
  README.md                         # This file
  exercises.md                      # Hands-on exercises
  solutions/
    bronze_layer.py                 # Exercise 1: Bronze ingestion
    silver_users.py                 # Exercise 2: Clean users
    silver_events.py                # Exercise 3: Clean listening events
    silver_cdn.py                   # Exercise 4: Clean CDN logs
    gold_daily_metrics.py           # Exercise 5: Daily listening metrics
    gold_podcast_performance.py     # Exercise 6: Podcast performance
    gold_user_retention.py          # Exercise 7: User retention cohorts
    gold_ad_revenue.py              # Exercise 8: Ad revenue analytics
    schema_evolution.py             # Exercise 9: Schema evolution
    merge_upsert.py                 # Exercise 10: MERGE / upsert
```

## Prerequisites

```bash
pip install pandas pyarrow duckdb
```

## Running the Solutions

Run the solutions in order -- Gold depends on Silver, which depends on Bronze:

```bash
# Step 1: Build Bronze layer
python solutions/bronze_layer.py

# Step 2: Build Silver layer
python solutions/silver_users.py
python solutions/silver_events.py
python solutions/silver_cdn.py

# Step 3: Build Gold layer
python solutions/gold_daily_metrics.py
python solutions/gold_podcast_performance.py
python solutions/gold_user_retention.py
python solutions/gold_ad_revenue.py

# Bonus: Schema evolution and upsert patterns
python solutions/schema_evolution.py
python solutions/merge_upsert.py
```

Each script prints progress information, data quality statistics, and sample output.
