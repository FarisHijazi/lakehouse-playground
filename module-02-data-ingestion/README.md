# Module 02: Data Ingestion

## Overview

Data ingestion is the first step in any data pipeline. It involves moving data from
source systems into your lakehouse, transforming raw formats into efficient analytical
formats, and handling real-world data quality issues along the way.

In this module, you will work with a podcast platform's raw data: user profiles with
messy fields, streaming events with duplicates and late arrivals, CDN logs, and ad
impression data. By the end, you will have built production-grade ingestion scripts
that clean, deduplicate, partition, and store data in columnar formats.

---

## Key Concepts

### ETL vs ELT

| Aspect | ETL (Extract-Transform-Load) | ELT (Extract-Load-Transform) |
|--------|------------------------------|-------------------------------|
| **When transforms happen** | Before loading into the target | After loading into the target |
| **Where transforms run** | External process (Spark, Python) | Inside the target system (SQL, dbt) |
| **Best for** | Structured warehouses, sensitive data filtering | Cloud data lakes, exploratory analytics |
| **Tradeoff** | Slower to ingest, cleaner storage | Faster to ingest, raw data preserved |

**Modern lakehouse pattern**: Most teams today use **ELT**. Raw data lands in a Bronze
layer as-is, then transformations produce Silver and Gold layers. This preserves the
original data for debugging, reprocessing, and new use cases that were not anticipated
at ingest time.

### File Formats

#### Row-Oriented Formats

| Format | Description | Use Case |
|--------|-------------|----------|
| **CSV** | Comma-separated values. Human-readable, universally supported. No schema, no types. | Data exchange, exports from legacy systems, small datasets. |
| **JSON** | Nested key-value structure. Self-describing. Verbose, slow to parse at scale. | API responses, config files, semi-structured data. |
| **JSONL** | One JSON object per line. Streamable, appendable. | Log files, streaming event data, incremental appends. |

#### Columnar Formats

| Format | Description | Use Case |
|--------|-------------|----------|
| **Parquet** | Apache column-oriented format. Built-in schema, compression, predicate pushdown. The default choice for analytics. | Analytical queries, data lake storage, anything read-heavy. |
| **ORC** | Optimized Row Columnar. Similar to Parquet. Historically preferred in Hive ecosystems. | Hive-based pipelines, legacy Hadoop environments. |
| **Avro** | Row-oriented binary format with embedded schema. Good for write-heavy streaming. | Kafka topics, schema evolution, data serialization between services. |

#### Table Formats (Lakehouse)

| Format | Description | Use Case |
|--------|-------------|----------|
| **Delta Lake** | Adds ACID transactions, time travel, and schema enforcement on top of Parquet. | Production lakehouse tables requiring reliability. |
| **Apache Iceberg** | Open table format with hidden partitioning, schema evolution, and snapshot isolation. | Multi-engine lakehouse (Spark + Trino + Flink). |
| **Apache Hudi** | Optimized for upserts and incremental processing on data lakes. | CDC pipelines, near-real-time analytics. |

### When to Use Which Format

```
Source data (CSV, JSON, JSONL)
    │
    ▼
Bronze layer ──► Store as-is, or convert to Parquet for space savings
    │
    ▼
Silver layer ──► Parquet or Delta (cleaned, typed, deduplicated)
    │
    ▼
Gold layer   ──► Delta or Parquet (aggregated, business-ready)
```

**Rules of thumb:**
- If humans need to read it: CSV or JSON.
- If you are storing event streams for replay: JSONL or Avro.
- If you are storing data for analytical queries: **Parquet**. Almost always Parquet.
- If you need ACID transactions, time travel, or upserts: Delta Lake, Iceberg, or Hudi.

### Schema-on-Read vs Schema-on-Write

| Approach | When schema is enforced | Pros | Cons |
|----------|------------------------|------|------|
| **Schema-on-Read** | At query time | Flexible, fast ingest, handles evolving sources | Errors surface late, inconsistent downstream |
| **Schema-on-Write** | At write/ingest time | Data quality guaranteed, fast queries | Slower ingest, schema changes require migration |

**Lakehouse approach**: Use schema-on-read for the Bronze layer (store raw data as-is),
then enforce schema-on-write at the Silver layer (validate types, reject bad records).

### Compression

| Algorithm | Compression Ratio | Speed | Splittable | Best For |
|-----------|--------------------|-------|------------|----------|
| **Snappy** | Moderate | Very fast | Yes (in Parquet) | Default for Parquet. Best balance of speed and size. |
| **Gzip** | High | Slow | No | Archival storage where read speed is less important. |
| **Zstd** | High | Fast | Yes (in Parquet) | Modern alternative to Gzip. Better ratio than Snappy, faster than Gzip. |
| **LZ4** | Low-Moderate | Fastest | Yes | Real-time systems where decompression speed is critical. |

**Recommendation**: Use **Snappy** as the default for Parquet files. Switch to **Zstd**
if storage cost is a concern and your tooling supports it.

---

## What You Will Build

In this module you will write Python scripts that:

1. **Profile** the raw data to discover quality issues before writing any transforms.
2. **Convert** raw CSV/JSON to Parquet with explicit schemas and compression.
3. **Clean** messy user data: parse mixed date formats, normalize gender values, handle nulls.
4. **Deduplicate** listening events using event IDs and timestamp-based logic.
5. **Partition** events by date into a Parquet directory structure for efficient querying.
6. **Benchmark** file formats to see the real-world impact of format choice.
7. **Implement incremental ingestion** so only new source files are processed.

## Prerequisites

```bash
pip install pandas pyarrow fastparquet
```

## Directory Structure

```
module-02-data-ingestion/
├── README.md              # This file
├── exercises.md           # Hands-on exercise descriptions
└── solutions/
    ├── profile_data.py        # Exercise 1: Data profiling
    ├── convert_formats.py     # Exercise 2: Format conversion
    ├── clean_users.py         # Exercise 3: User data cleaning
    ├── deduplicate_events.py  # Exercise 4: Event deduplication
    ├── partition_events.py    # Exercise 5: Date partitioning
    ├── compare_formats.py     # Exercise 6: Format benchmarks
    └── incremental_ingest.py  # Exercise 7: Incremental ingestion
```
