# Module 04: Data Warehouse

## Overview

In this module, you will build a **star schema data warehouse** for podcast analytics using **DuckDB** as a local warehouse engine. You will learn core data warehousing concepts, design dimension and fact tables, implement Slowly Changing Dimensions (SCD), and write analytical SQL queries.

By the end of this module you will have a fully functional `warehouse.duckdb` file containing dimension tables, fact tables, and a library of analytical queries that answer real business questions about podcast listening behaviour.

---

## Key Concepts

### 1. Data Warehouse vs Data Lake vs Lakehouse

| Aspect | Data Warehouse | Data Lake | Lakehouse |
|---|---|---|---|
| **Data format** | Structured (tables, schemas) | Raw (JSON, CSV, logs, images) | Structured + semi-structured on open formats |
| **Schema** | Schema-on-write (enforced at load time) | Schema-on-read (interpreted at query time) | Schema-on-write with flexibility |
| **Query engine** | SQL-optimised (BigQuery, Redshift, Snowflake) | Spark, Presto, Hive | Unified SQL + programmatic (Delta Lake, Iceberg) |
| **Users** | Analysts, BI tools | Data scientists, ML engineers | Everyone |
| **Data quality** | High (curated, validated) | Variable (raw dumps) | High (ACID transactions, validation) |
| **Cost** | Higher per TB (compute + storage coupled) | Low storage, high compute | Balanced (open storage, elastic compute) |

**When to use what:**
- **Data Warehouse** -- You need fast, reliable SQL analytics for business reporting. Data is well-understood and changes predictably.
- **Data Lake** -- You need to store everything cheaply first, figure out the schema later. Good for ML training data, log archives.
- **Lakehouse** -- You want the best of both: cheap open-format storage with warehouse-grade query performance and ACID guarantees. This is where the industry is heading (Delta Lake, Apache Iceberg, Apache Hudi).

### 2. Star Schema vs Snowflake Schema

#### Star Schema

The star schema is the most common data warehouse design pattern. It consists of:

- **Fact tables** at the centre -- contain measurable events (listens, ad impressions, CDN requests).
- **Dimension tables** radiating out -- contain descriptive attributes (users, podcasts, episodes, dates).

```
                 dim_users
                    |
dim_dates --- fact_listens --- dim_episodes
                                    |
                               dim_podcasts
```

**Advantages:**
- Simple to understand and query (fewer JOINs)
- Excellent query performance (denormalized dimensions)
- BI tools (Looker, Tableau, Power BI) expect this layout

#### Snowflake Schema

A snowflake schema normalizes dimension tables further. For example, instead of storing `category_name` directly in `dim_podcasts`, you would create a separate `dim_categories` table.

```
dim_dates --- fact_listens --- dim_episodes --- dim_podcasts --- dim_categories
```

**Advantages:**
- Less storage (no repeated strings)
- Easier to maintain (update category name in one place)

**Disadvantages:**
- More JOINs = slower queries
- More complex for analysts

**Our choice:** We use a **star schema** in this module because it is simpler, faster for analytics, and the standard in modern cloud warehouses.

### 3. Fact Tables vs Dimension Tables

#### Fact Tables

Fact tables record **business events** -- things that happened at a point in time.

| Property | Description |
|---|---|
| Grain | One row per event (e.g., one listen, one ad impression) |
| Columns | Foreign keys to dimensions + numeric measures |
| Size | Very large (millions to billions of rows) |
| Examples | `fact_listens`, `fact_ad_events`, `fact_cdn_quality` |

**Our fact tables:**

| Table | Grain | Measures |
|---|---|---|
| `fact_listens` | One listen event | `listened_seconds`, `completion_pct` |
| `fact_ad_events` | One ad impression/click | `revenue_sar`, `ad_duration_seconds` |
| `fact_cdn_quality` | One CDN log entry | `buffer_events`, `rebuffer_ratio`, `startup_time_ms`, `bytes_transferred` |

#### Dimension Tables

Dimension tables contain **descriptive context** -- the who, what, where, when.

| Property | Description |
|---|---|
| Grain | One row per entity (e.g., one user, one podcast) |
| Columns | Natural key + descriptive attributes |
| Size | Small to medium (thousands to millions of rows) |
| Examples | `dim_users`, `dim_podcasts`, `dim_episodes`, `dim_dates` |

**Our dimension tables:**

| Table | Key | Notable columns |
|---|---|---|
| `dim_users` | `user_key` (surrogate) | `user_id`, `name`, `country`, `platform`, `signup_date`, `subscription_type`, `age`, `gender` |
| `dim_podcasts` | `podcast_key` (surrogate) | `podcast_id`, `name`, `name_en`, `category`, `language`, `host` |
| `dim_episodes` | `episode_key` (surrogate) | `episode_id`, `podcast_id`, `title`, `season`, `episode_number`, `duration_seconds` |
| `dim_dates` | `date_key` (INTEGER, YYYYMMDD) | `full_date`, `year`, `quarter`, `month`, `day_of_week`, `is_weekend` |

### 4. Surrogate Keys vs Natural Keys

- **Natural key**: The business identifier (`podcast_id = 'pod_001'`). Comes from the source system.
- **Surrogate key**: A warehouse-generated integer (`podcast_key = 1`). Used as the primary key in dimension tables and foreign key in fact tables.

**Why surrogate keys?**
- Source systems can change or reuse natural keys.
- Integer joins are faster than string joins.
- Required for SCD Type 2 (multiple rows per natural key).

### 5. Partitioning Strategies

Partitioning divides a large table into smaller physical segments so queries can skip irrelevant data.

#### Partition by Date (most common)

```sql
-- BigQuery example
CREATE TABLE fact_listens
PARTITION BY DATE(event_date);

-- DuckDB: we simulate partitioning with a date column and filtered scans
-- DuckDB automatically prunes partitions in Hive-partitioned parquet
```

When a query includes `WHERE event_date BETWEEN '2024-01-01' AND '2024-01-31'`, the engine reads only the January partition instead of scanning the entire table.

#### Partition by Category

Useful when queries always filter by a specific dimension:

```sql
-- BigQuery example
CREATE TABLE fact_listens
PARTITION BY DATE(event_date)
CLUSTER BY podcast_category;
```

### 6. Clustering / Sorting

Clustering (BigQuery) or sort keys (Redshift) order data within partitions so range scans are efficient.

```sql
-- BigQuery: CLUSTER BY sorts data within each partition
CREATE TABLE fact_listens
PARTITION BY DATE(event_date)
CLUSTER BY user_id, podcast_id;

-- Redshift: SORTKEY
CREATE TABLE fact_listens (...)
SORTKEY (event_date, user_id);

-- DuckDB: we can use ORDER BY when inserting to achieve similar physical ordering
CREATE TABLE fact_listens AS
SELECT * FROM staging_listens ORDER BY event_date, user_id;
```

### 7. Slowly Changing Dimensions (SCD)

Dimensions change over time. A podcast might rebrand, a user might switch subscription tiers. How do we handle this?

#### SCD Type 1 -- Overwrite

Simply overwrite the old value. No history is kept.

```
BEFORE: podcast_id=pod_001, name='Swalif Business'
AFTER:  podcast_id=pod_001, name='Swalif Business & Tech'
```

**Use when:** You do not care about historical values (e.g., fixing a typo).

#### SCD Type 2 -- Add a New Row (Full History)

Create a new row with the new values. The old row is marked as expired.

```
podcast_key | podcast_id | name                    | valid_from  | valid_to    | is_current
1           | pod_001    | Swalif Business         | 2019-03-15  | 2023-06-30  | false
2           | pod_001    | Swalif Business & Tech  | 2023-07-01  | 9999-12-31  | true
```

**Use when:** You need to know what the value was at the time of each event. Fact rows from before the rebrand point to `podcast_key=1`; fact rows after point to `podcast_key=2`.

**Real example with our data:**

Suppose the podcast "سوالف بزنس" (Swalif Business, pod_001) rebrands to "سوالف بزنس وتقنية" (Swalif Business & Tech) on 2023-07-01, and later changes its category from "Business" to "Business & Technology" on 2024-01-15.

The `dim_podcasts` table would have three rows for pod_001:

| podcast_key | podcast_id | name | category | valid_from | valid_to | is_current |
|---|---|---|---|---|---|---|
| 1 | pod_001 | سوالف بزنس | Business | 2019-03-15 | 2023-06-30 | false |
| 2 | pod_001 | سوالف بزنس وتقنية | Business | 2023-07-01 | 2024-01-14 | false |
| 3 | pod_001 | سوالف بزنس وتقنية | Business & Technology | 2024-01-15 | 9999-12-31 | true |

#### SCD Type 3 -- Add a Column

Keep both old and new values in the same row.

```
podcast_key | podcast_id | current_name            | previous_name       | name_changed_date
1           | pod_001    | Swalif Business & Tech  | Swalif Business     | 2023-07-01
```

**Use when:** You only need to track the most recent change (limited history).

### 8. OLAP vs OLTP

| Property | OLTP | OLAP |
|---|---|---|
| **Purpose** | Process transactions | Analyse aggregated data |
| **Operations** | INSERT, UPDATE, DELETE (single rows) | SELECT with GROUP BY, JOIN, window functions |
| **Schema** | Normalised (3NF) | Denormalised (star/snowflake) |
| **Query pattern** | Short, frequent, low-latency | Long-running, complex, batch |
| **Users** | Applications, APIs | Analysts, dashboards |
| **Examples** | PostgreSQL, MySQL | BigQuery, Snowflake, Redshift, DuckDB |

**DuckDB is an OLAP engine.** It uses columnar storage internally, which makes it excellent for analytical queries (aggregations, scans) but not for high-frequency single-row inserts that OLTP databases handle.

### 9. Cloud Data Warehouse Concepts

Even though we use DuckDB locally, it is important to understand the major cloud offerings:

#### Google BigQuery
- Serverless -- no infrastructure to manage
- Columnar storage, automatic partitioning
- Pay per query (bytes scanned) or flat-rate slots
- Supports `PARTITION BY` and `CLUSTER BY` natively

#### Snowflake
- Separates compute (virtual warehouses) from storage
- Auto-scaling, auto-suspend of compute
- Time travel (query data as of a past timestamp)
- Zero-copy cloning for dev/test environments

#### Amazon Redshift
- Cluster-based (leader + compute nodes)
- Distribution keys control how data is spread across nodes
- Sort keys control physical ordering within nodes
- Redshift Spectrum can query data in S3 without loading

#### Common Concepts Across All Three
- **Columnar storage** -- stores each column separately, enabling compression and fast scans
- **Materialized views** -- pre-computed query results that refresh periodically
- **External tables** -- query files in cloud storage (S3, GCS) without loading
- **Workload management** -- prioritise certain queries or users

### 10. DuckDB as a Local Warehouse Alternative

DuckDB is an **in-process OLAP database** (like SQLite for analytics). It is perfect for this module because:

- **Zero infrastructure** -- a single file (`warehouse.duckdb`)
- **Columnar engine** -- same query patterns as BigQuery/Snowflake
- **Direct file reading** -- `read_json()`, `read_csv()`, `read_parquet()` without loading
- **Full SQL support** -- window functions, CTEs, `QUALIFY`, lateral joins
- **Fast** -- handles millions of rows on a laptop

```python
import duckdb

# Connect to a persistent warehouse file
con = duckdb.connect('warehouse.duckdb')

# Query raw JSON directly
con.sql("SELECT * FROM read_json('data/raw/podcasts.json')")

# Create a table from a query
con.sql("""
    CREATE TABLE dim_podcasts AS
    SELECT row_number() OVER () AS podcast_key,
           podcast_id, name, name_en, category, language, host
    FROM read_json('data/raw/podcasts.json')
""")
```

---

## Module Structure

```
module-04-data-warehouse/
├── README.md              # This file -- concepts and theory
├── exercises.md           # Hands-on exercises
└── solutions/
    ├── create_warehouse.py     # Build the star schema in DuckDB
    ├── scd_type2.py            # SCD Type 2 implementation
    ├── analytics_queries.sql   # Solutions for analytics exercises
    └── window_functions.sql    # Window function exercise solutions
```

## Prerequisites

```bash
pip install duckdb
```

## Getting Started

1. Read through this README to understand the concepts.
2. Open `exercises.md` and work through each exercise.
3. Check your work against the solutions in `solutions/`.
4. Run the warehouse builder:

```bash
cd module-04-data-warehouse
python solutions/create_warehouse.py
```

This creates `warehouse.duckdb` with all dimension and fact tables loaded from `../data/raw/`.

---

## Further Reading

- [The Data Warehouse Toolkit](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/) -- Ralph Kimball's definitive guide
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Star Schema vs Snowflake Schema](https://www.guru99.com/star-snowflake-data-warehousing.html)
- [Slowly Changing Dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
