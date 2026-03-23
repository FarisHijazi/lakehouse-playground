# Module 04: Data Warehouse

## Overview

In this module, you will build a **star schema data warehouse** for NYC taxi trip analytics using **DuckDB** as a local warehouse engine. You will learn core data warehousing concepts, design dimension and fact tables, implement Slowly Changing Dimensions (SCD), and write analytical SQL queries against real NYC Taxi & Limousine Commission (TLC) data.

This pattern -- loading raw Parquet and CSV files into a structured star schema -- is exactly what Databricks uses with Delta Lake tables in their Lakehouse architecture. In production, Delta Lake provides ACID transactions and schema enforcement on top of Parquet files stored in cloud object storage (S3, ADLS, GCS). Here we replicate the same design locally with DuckDB, which has native Parquet reading that rivals Spark for single-machine workloads.

By the end of this module you will have a fully functional `warehouse.duckdb` file containing dimension tables, fact tables, and a library of analytical queries that answer real business questions about taxi trip patterns, revenue, weather impacts, and service comparisons across yellow taxis, green taxis, and for-hire vehicles.

---

## Key Concepts

### 1. Data Warehouse vs Data Lake vs Lakehouse

| Aspect | Data Warehouse | Data Lake | Lakehouse |
|---|---|---|---|
| **Data format** | Structured (tables, schemas) | Raw (JSON, CSV, Parquet, images) | Structured + semi-structured on open formats |
| **Schema** | Schema-on-write (enforced at load time) | Schema-on-read (interpreted at query time) | Schema-on-write with flexibility |
| **Query engine** | SQL-optimised (BigQuery, Redshift, Snowflake) | Spark, Presto, Hive | Unified SQL + programmatic (Delta Lake, Iceberg) |
| **Users** | Analysts, BI tools | Data scientists, ML engineers | Everyone |
| **Data quality** | High (curated, validated) | Variable (raw dumps) | High (ACID transactions, validation) |
| **Cost** | Higher per TB (compute + storage coupled) | Low storage, high compute | Balanced (open storage, elastic compute) |

**When to use what:**
- **Data Warehouse** -- You need fast, reliable SQL analytics for business reporting. Data is well-understood and changes predictably.
- **Data Lake** -- You need to store everything cheaply first, figure out the schema later. Good for ML training data, log archives.
- **Lakehouse** -- You want the best of both: cheap open-format storage with warehouse-grade query performance and ACID guarantees. This is where the industry is heading (Delta Lake, Apache Iceberg, Apache Hudi).

**How Databricks Delta Lake implements the Lakehouse pattern:**
- Raw data lands as Parquet files in cloud storage (the "lake" layer).
- Delta Lake adds a transaction log on top of those Parquet files, giving you ACID semantics, time travel, and schema enforcement (the "warehouse" layer).
- Tables are organised in a medallion architecture: Bronze (raw) -> Silver (cleaned) -> Gold (aggregated).
- In this module, our `data/raw/` Parquet and CSV files are the Bronze layer, and our DuckDB star schema is the Gold layer.

### 2. Star Schema Design for Taxi Data

The star schema is the most common data warehouse design pattern. It consists of:

- **Fact tables** at the centre -- contain measurable trip events with numeric measures (fares, distances, durations).
- **Dimension tables** radiating out -- contain descriptive attributes (zones, vendors, dates, weather).

```
                    dim_vendors
                        |
dim_date --- fact_yellow_trips --- dim_zones (pickup)
                  |                    |
          dim_payment_types     dim_zones (dropoff)
                  |
             dim_rate_codes

dim_date --- fact_green_trips --- dim_zones
                  |
             dim_vendors

dim_date --- fact_fhv_trips --- dim_zones
                  |
             dim_fhv_bases

             dim_weather (joins via date)
```

**Advantages of star schema:**
- Simple to understand and query (fewer JOINs)
- Excellent query performance (denormalized dimensions)
- BI tools (Looker, Tableau, Power BI) expect this layout

### 3. Fact Tables vs Dimension Tables

#### Fact Tables

Fact tables record **business events** -- things that happened at a point in time.

| Property | Description |
|---|---|
| Grain | One row per event (e.g., one taxi trip) |
| Columns | Foreign keys to dimensions + numeric measures |
| Size | Very large (millions to billions of rows) |
| Examples | `fact_yellow_trips`, `fact_green_trips`, `fact_fhv_trips` |

**Our fact tables:**

| Table | Grain | Measures |
|---|---|---|
| `fact_yellow_trips` | One yellow taxi trip | `fare_amount`, `tip_amount`, `total_amount`, `trip_distance`, `passenger_count` |
| `fact_green_trips` | One green taxi trip | `fare_amount`, `tip_amount`, `total_amount`, `trip_distance`, `passenger_count` |
| `fact_fhv_trips` | One for-hire vehicle trip | `trip_duration_minutes` (derived from pickup/dropoff times) |

#### Dimension Tables

Dimension tables contain **descriptive context** -- the who, what, where, when.

| Property | Description |
|---|---|
| Grain | One row per entity (e.g., one taxi zone, one vendor) |
| Columns | Natural key + descriptive attributes |
| Size | Small to medium (dozens to thousands of rows) |
| Examples | `dim_zones`, `dim_vendors`, `dim_date` |

**Our dimension tables:**

| Table | Key | Notable columns |
|---|---|---|
| `dim_zones` | `location_id` | `borough`, `zone`, `service_zone` |
| `dim_vendors` | `vendor_id` | `vendor_name` |
| `dim_rate_codes` | `rate_code_id` | `rate_code_name` |
| `dim_payment_types` | `payment_type_id` | `payment_type_name` |
| `dim_fhv_bases` | `base_number` | `base_name`, `dba`, `base_type` |
| `dim_date` | `date_key` (INTEGER, YYYYMMDD) | `full_date`, `year`, `quarter`, `month`, `day_of_week`, `is_weekend` |
| `dim_weather` | `date` | `temp_avg`, `precipitation`, `snow_depth`, `wind_speed` |

### 4. Surrogate Keys vs Natural Keys

- **Natural key**: The business identifier (`location_id = 132` for JFK Airport). Comes from the source system.
- **Surrogate key**: A warehouse-generated integer. Used as the primary key in dimension tables and foreign key in fact tables.

**Why surrogate keys?**
- Source systems can change or reuse natural keys.
- Integer joins are faster than string joins.
- Required for SCD Type 2 (multiple rows per natural key).

For this taxi dataset, many dimension tables already have clean integer natural keys (location_id, vendor_id), so we use those directly. The date dimension uses a YYYYMMDD integer key for readability and fast joins.

### 5. Slowly Changing Dimensions (SCD)

Dimensions change over time. A taxi zone boundary might shift, or a rate code might be redefined. How do we handle this?

#### SCD Type 1 -- Overwrite

Simply overwrite the old value. No history is kept.

```
BEFORE: location_id=132, zone='JFK Airport'
AFTER:  location_id=132, zone='John F. Kennedy International Airport'
```

**Use when:** You do not care about historical values (e.g., fixing a typo).

#### SCD Type 2 -- Add a New Row (Full History)

Create a new row with the new values. The old row is marked as expired.

```
zone_key | location_id | zone                | borough  | valid_from  | valid_to    | is_current
1        | 261         | Downtown Manhattan  | Manhattan| 2009-01-01  | 2023-06-30  | false
2        | 261         | World Trade Center  | Manhattan| 2023-07-01  | 9999-12-31  | true
```

**Use when:** You need to know what the value was at the time of each trip. Trips before the rename point to `zone_key=1`; trips after point to `zone_key=2`.

### 6. DuckDB as a Local Warehouse

DuckDB is an **in-process OLAP database** (like SQLite for analytics). It is perfect for this module because:

- **Zero infrastructure** -- a single file (`warehouse.duckdb`)
- **Columnar engine** -- same query patterns as BigQuery/Snowflake
- **Native Parquet reading** -- `read_parquet()` directly queries Parquet files without loading, just like Spark or Databricks
- **Full SQL support** -- window functions, CTEs, `QUALIFY`, lateral joins
- **Fast** -- handles millions of rows on a laptop

```python
import duckdb

con = duckdb.connect('warehouse.duckdb')

# Query raw Parquet directly -- just like Databricks reads Delta tables
con.sql("SELECT COUNT(*) FROM read_parquet('data/raw/yellow_taxi_trips.parquet')")

# Create a warehouse table from Parquet
con.sql("""
    CREATE TABLE fact_yellow_trips AS
    SELECT * FROM read_parquet('data/raw/yellow_taxi_trips.parquet')
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
- [Databricks Delta Lake](https://docs.databricks.com/en/delta/index.html) -- The Lakehouse pattern in production
- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) -- Source data documentation
- [Star Schema vs Snowflake Schema](https://www.guru99.com/star-snowflake-data-warehousing.html)
- [Slowly Changing Dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
