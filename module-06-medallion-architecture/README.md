# Module 06 - Medallion Architecture

## Overview

The **Medallion Architecture** (also called multi-hop architecture) is a data design
pattern that organizes data in a lakehouse into three progressive layers of quality
and refinement: **Bronze**, **Silver**, and **Gold**. Databricks popularized this
pattern as THE standard approach for building production data pipelines on Delta Lake.

This module implements the medallion architecture locally with PySpark, using real
NYC Taxi & Limousine Commission (TLC) trip data. Every script includes comments
showing the equivalent Databricks notebook code, Delta Live Tables (DLT) syntax,
and Unity Catalog patterns you would use in a production Databricks environment.

```
  Raw Sources            Bronze               Silver                Gold
 +---------------+   +--------------+    +----------------+   +-----------------+
 | yellow_trip   |-->| Raw as-is    |--->| Cleaned trips  |-->| Daily metrics   |
 | green_trip    |   | + metadata   |    | Deduped        |   | Zone analytics  |
 | fhvhv_trip    |   | + _ingested  |    | Typed/validated|   | Hourly patterns |
 | taxi_zones    |   | + _source    |    | + derived cols |   | Weather impact  |
 | weather       |   | Append-only  |    | Quarantined    |   | Dashboard-ready |
 +---------------+   +--------------+    +----------------+   +-----------------+
                        "Land it"           "Refine it"          "Serve it"
```

---

## Databricks Context: Why This Pattern Matters

### The Databricks Lakehouse

Databricks built its entire platform around the lakehouse concept. The medallion
architecture is not just a suggestion -- it is the recommended and documented way
to organize data in Databricks. When you create a new Databricks workspace, the
default catalog structure assumes Bronze/Silver/Gold layers.

### Delta Live Tables (DLT)

In Databricks, you would implement this pipeline using **Delta Live Tables**, which
provides:

- **Declarative pipeline definitions**: You declare what each table should contain,
  and DLT handles orchestration, dependency management, and incremental processing.
- **Built-in data quality**: `@dlt.expect` decorators enforce data quality rules
  at each layer.
- **Automatic lineage tracking**: DLT tracks data flow across layers automatically.

```python
# Databricks DLT equivalent of what we build locally:
import dlt
from pyspark.sql.functions import *

@dlt.table(comment="Raw yellow taxi trips, ingested as-is")
def bronze_yellow_trips():
    return (
        spark.readStream
        .format("cloudFiles")          # Auto Loader
        .option("cloudFiles.format", "parquet")
        .load("/mnt/raw/yellow_tripdata_*.parquet")
        .select("*", "_metadata.file_path", "_metadata.file_modification_time")
    )

@dlt.table(comment="Cleaned yellow taxi trips")
@dlt.expect_or_drop("valid_fare", "fare_amount >= 0")
@dlt.expect_or_drop("valid_pickup", "tpep_pickup_datetime IS NOT NULL")
def silver_yellow_trips():
    return (
        dlt.read("bronze_yellow_trips")
        .filter(col("tpep_pickup_datetime").isNotNull())
        .withColumn("trip_duration_minutes", ...)
    )

@dlt.table(comment="Daily trip metrics by taxi type")
def gold_daily_metrics():
    return (
        dlt.read("silver_yellow_trips")
        .groupBy("pickup_date")
        .agg(count("*").alias("total_trips"), ...)
    )
```

### Unity Catalog

In production Databricks, tables are organized in a three-level namespace:

```
catalog.schema.table
  |       |      |
  |       |      +-- e.g., gold_daily_metrics
  |       +--------- e.g., gold, silver, bronze
  +----------------- e.g., nyc_taxi
```

Example fully qualified names:
- `nyc_taxi.bronze.raw_yellow_trips`
- `nyc_taxi.silver.clean_yellow_trips`
- `nyc_taxi.gold.daily_trip_metrics`
- `nyc_taxi.gold.zone_analytics`

### Governance Features

- **Access Control:** Data engineers get write access to bronze/silver; analysts
  get read-only access to silver/gold.
- **Data Lineage:** Unity Catalog automatically tracks which tables produced which
  other tables. You can visualize the full flow from Bronze to Gold.
- **Audit Logging:** Every query and table access is logged for compliance.
- **Data Discovery:** Searchable catalog of all tables with descriptions, tags, and
  ownership information.

---

## Why Layers Matter

### Separation of Concerns

Each layer has one job. Bronze is responsible for reliable ingestion. Silver handles
data quality. Gold focuses on business logic. When something breaks, you know exactly
where to look. When business requirements change, you only modify the Gold layer
without re-ingesting everything.

### Data Quality Progression

Raw taxi data is messy: negative fares, null pickup/dropoff times, impossible speeds,
zero-distance trips. Rather than trying to fix everything in a single monolithic ETL
job, the medallion pattern applies transformations incrementally.

### Reprocessing and Recovery

Because Bronze retains the raw data, you can always reprocess Silver and Gold layers
when you discover bugs in your transformation logic or when business definitions
change. This is essential for taxi data, where TLC periodically revises data quality
standards.

### Team Autonomy

Data engineers own Bronze and Silver. Analytics engineers and data scientists work
with Silver and Gold. Each team operates on the layer appropriate to their skill set.

---

## Delta Lake: Why It Matters

Delta Lake is an open-source storage layer that brings reliability to data lakes.
This module uses Parquet files with PySpark for local compatibility, but in a
Databricks environment you would use Delta format for all tables.

### What Delta Lake Adds Over Raw Parquet

| Capability | Raw Parquet | Delta Lake |
|---|---|---|
| **ACID Transactions** | No. A failed write can leave partial/corrupt files. | Yes. Writes are atomic. |
| **Time Travel** | No. Once overwritten, previous data is gone. | Yes. Query data as of any past version or timestamp. |
| **MERGE (Upsert)** | Not supported. Read-modify-write manually. | Native `MERGE INTO` command for handling late-arriving taxi records. |
| **Schema Evolution** | Manual. Adding columns requires rewriting all files. | Built-in. `mergeSchema` option reconciles new columns (e.g., when `airport_fee` was added in 2019). |
| **Schema Enforcement** | None. Any file can be dumped into a directory. | Rejects writes that do not match the table schema unless evolution is enabled. |
| **Z-Ordering** | Not available. | `OPTIMIZE ... ZORDER BY (pickup_datetime)` for fast time-range queries. |
| **Concurrent Writes** | Unsafe. Two writers can corrupt data. | Optimistic concurrency control handles multiple writers safely. |

### The Delta Transaction Log

Delta Lake stores a `_delta_log/` directory alongside the Parquet data files. This
log contains JSON entries for every transaction. The log is the source of truth --
it defines which Parquet files are "current" and enables time travel, rollback, and
ACID guarantees.

```sql
-- In Databricks, you can time-travel to see yesterday's data:
SELECT * FROM nyc_taxi.silver.yellow_trips VERSION AS OF 42;
SELECT * FROM nyc_taxi.silver.yellow_trips TIMESTAMP AS OF '2024-01-15';
```

---

## The Three Layers in Detail (NYC Taxi Context)

### Bronze Layer (Raw / Landing)

**Purpose:** Faithful copy of TLC source data. No transformations, no filtering.

**What gets stored:**
- Raw parquet files from TLC with original column names and values
- CSV reference data (taxi zones, vendors, rate codes) read with minimal type inference
- Weather data preserved as-is

**Metadata added:**
- `_ingested_at`: When the pipeline ran
- `_source_file`: Original filename (e.g., `yellow_tripdata_2023-01.parquet`)
- `_batch_id`: UUID identifying this ingestion batch

**Quality issues Bronze retains:**
- Negative fares and tip amounts
- Null pickup/dropoff datetimes
- Trips with zero distance but non-zero fare
- Trips with impossibly high speeds (data entry errors)
- Future-dated trips (timestamp errors)

```python
# In Databricks, you would use Auto Loader for streaming ingestion:
# spark.readStream.format("cloudFiles")
#   .option("cloudFiles.format", "parquet")
#   .option("cloudFiles.schemaLocation", "/mnt/schema/yellow_trips")
#   .load("/mnt/raw/yellow_tripdata_*.parquet")
```

### Silver Layer (Cleaned / Conformed)

**Purpose:** Single source of truth for each trip type. Cleaned, deduplicated,
validated, with computed columns added -- but still at individual trip granularity.

**What changes from Bronze:**
- Trips with null pickup/dropoff times removed
- Negative fares flagged or removed
- Computed columns added: `trip_duration_minutes`, `speed_mph`, `is_airport_trip`, `is_rush_hour`
- Deduplication on natural keys
- Type casting and validation applied
- Invalid records quarantined

**Silver tables:**
- `silver_yellow_trips` -- Cleaned yellow taxi trips
- `silver_green_trips` -- Cleaned green taxi trips
- `silver_fhv_trips` -- Cleaned FHV/rideshare trips

```python
# In Databricks DLT, you would use expectations:
# @dlt.expect_or_drop("valid_fare", "fare_amount >= 0")
# @dlt.expect_or_drop("valid_duration", "trip_duration_minutes > 0")
# @dlt.expect_or_quarantine("reasonable_speed", "speed_mph < 100")
```

### Gold Layer (Business / Aggregated)

**Purpose:** Business-level tables optimized for specific analytical use cases.

**Gold tables in this module:**
- `gold_daily_metrics` -- Daily trip counts, revenue, avg distance by taxi type
- `gold_zone_analytics` -- Top zones, revenue by borough, trip characteristics per zone
- `gold_hourly_patterns` -- Hourly distributions, weekday vs weekend, rush hour analysis
- `gold_weather_impact` -- Trip volume and revenue correlated with weather

```sql
-- In Databricks, Gold tables are often materialized views:
-- CREATE MATERIALIZED VIEW nyc_taxi.gold.daily_metrics AS
-- SELECT pickup_date, taxi_type, COUNT(*) as total_trips, ...
-- FROM nyc_taxi.silver.yellow_trips
-- GROUP BY pickup_date, taxi_type;
```

---

## Schema Evolution Handling (NYC Taxi Example)

The NYC TLC data provides a real-world example of schema evolution: the
`airport_fee` column was added to yellow taxi data starting in 2019. Earlier
data files do not contain this column.

1. **Bronze:** New columns are automatically captured because we read source files
   dynamically. The `_source_file` metadata tells us when the new column appeared.

2. **Silver:** Schema evolution requires an explicit decision. In Delta Lake:
   ```python
   df.write.format("delta").option("mergeSchema", "true").mode("append").save(path)
   ```

3. **Gold:** Usually unaffected unless the new column is relevant to a business
   metric.

---

## Comparison: Local PySpark vs. Databricks

| Aspect | This Module (Local PySpark) | Databricks Production |
|---|---|---|
| **Storage format** | Parquet files on local disk | Delta Lake on cloud storage (S3/ADLS/GCS) |
| **Compute** | Local SparkSession | Databricks clusters (autoscaling) |
| **Ingestion** | `spark.read.parquet()` | Auto Loader (`cloudFiles`) with streaming |
| **Pipeline orchestration** | Run scripts in order | Delta Live Tables (declarative) |
| **Data quality** | Manual validation code | DLT Expectations (`@dlt.expect`) |
| **Catalog** | File paths | Unity Catalog (3-level namespace) |
| **MERGE/Upsert** | DuckDB SQL simulation | Native Delta Lake `MERGE INTO` |
| **Schema evolution** | Manual `unionByName` | `mergeSchema` option on Delta writes |
| **Governance** | None | Unity Catalog ACLs, lineage, audit logs |

---

## Module Structure

```
module-06-medallion-architecture/
  README.md                           # This file
  exercises.md                        # Hands-on exercises
  solutions/
    bronze_layer.py                   # Exercise 1: Ingest raw taxi data
    silver_yellow_trips.py            # Exercise 2: Clean yellow taxi trips
    silver_green_trips.py             # Exercise 3: Clean green taxi trips
    silver_fhv_trips.py              # Exercise 4: Clean FHV/rideshare trips
    gold_daily_metrics.py             # Exercise 5: Daily trip metrics
    gold_zone_analytics.py            # Exercise 6: Zone-level analytics
    gold_hourly_patterns.py           # Exercise 7: Hourly/temporal patterns
    gold_weather_impact.py            # Exercise 8: Weather impact analysis
    schema_evolution.py               # Exercise 9: Schema evolution
    merge_upsert.py                   # Exercise 10: MERGE / upsert
```

## Prerequisites

```bash
pip install pyspark pyarrow duckdb
```

## Running the Solutions

Run the solutions in order -- Gold depends on Silver, which depends on Bronze:

```bash
# Step 1: Build Bronze layer
python solutions/bronze_layer.py

# Step 2: Build Silver layer
python solutions/silver_yellow_trips.py
python solutions/silver_green_trips.py
python solutions/silver_fhv_trips.py

# Step 3: Build Gold layer
python solutions/gold_daily_metrics.py
python solutions/gold_zone_analytics.py
python solutions/gold_hourly_patterns.py
python solutions/gold_weather_impact.py

# Bonus: Schema evolution and upsert patterns
python solutions/schema_evolution.py
python solutions/merge_upsert.py
```

Each script prints progress information, data quality statistics, and sample output.
All scripts include comments showing the Databricks DLT / Unity Catalog equivalent.
