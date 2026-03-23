"""
Module 04: Create Data Warehouse
=================================
This script builds a star schema data warehouse in DuckDB from raw NYC TLC
taxi data (Parquet and CSV files).

It creates:
  - dim_date:          Calendar dimension (2018-01-01 to 2025-12-31)
  - dim_zones:         Taxi zone dimension from taxi_zones.csv
  - dim_vendors:       Vendor dimension from vendors.csv
  - dim_rate_codes:    Rate code dimension from rate_codes.csv
  - dim_payment_types: Payment type dimension from payment_types.csv
  - dim_fhv_bases:     FHV base dimension from fhv_bases.csv
  - dim_weather:       Daily weather dimension from daily_weather.csv
  - fact_yellow_trips: Yellow taxi trips from yellow_taxi_trips.parquet
  - fact_green_trips:  Green taxi trips from green_taxi_trips.parquet
  - fact_fhv_trips:    For-hire vehicle trips from fhv_trips.parquet

This pattern -- reading raw Parquet/CSV into a star schema -- mirrors what
Databricks does with Delta Lake tables in their Lakehouse architecture.
DuckDB's native Parquet reader is excellent for this workflow.

Usage:
    cd module-04-data-warehouse
    python solutions/create_warehouse.py
"""

from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
WAREHOUSE_PATH = Path(__file__).resolve().parent.parent / "warehouse.duckdb"


def create_dim_date(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the date dimension table.

    Standard calendar dimension covering 2018-01-01 to 2025-12-31.
    The date_key is an integer in YYYYMMDD format for efficient joins
    and human-readable partition pruning.
    """
    print("  Creating dim_date...")
    con.execute("DROP TABLE IF EXISTS dim_date;")
    con.execute("""
        CREATE TABLE dim_date AS
        WITH date_spine AS (
            SELECT UNNEST(generate_series(
                DATE '2018-01-01',
                DATE '2025-12-31',
                INTERVAL 1 DAY
            ))::DATE AS full_date
        )
        SELECT
            (YEAR(full_date) * 10000
             + MONTH(full_date) * 100
             + DAY(full_date))::INTEGER     AS date_key,
            full_date,
            YEAR(full_date)::INTEGER        AS year,
            QUARTER(full_date)::INTEGER     AS quarter,
            MONTH(full_date)::INTEGER       AS month,
            MONTHNAME(full_date)            AS month_name,
            DAY(full_date)::INTEGER         AS day_of_month,
            ISODOW(full_date)::INTEGER      AS day_of_week,   -- 1=Monday, 7=Sunday
            DAYNAME(full_date)              AS day_name,
            WEEKOFYEAR(full_date)::INTEGER  AS week_of_year,
            (ISODOW(full_date) >= 6)        AS is_weekend
        FROM date_spine
        ORDER BY full_date;
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
    print(f"    -> {count} rows")


def create_dim_zones(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the taxi zones dimension table.

    Loaded from taxi_zones.csv. Each row represents one of the 263 TLC
    taxi zones across NYC boroughs. Includes SCD Type 2 columns for
    tracking boundary changes over time.
    """
    print("  Creating dim_zones...")
    zones_file = DATA_RAW / "taxi_zones.csv"
    con.execute("DROP TABLE IF EXISTS dim_zones;")
    con.execute(f"""
        CREATE TABLE dim_zones AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY LocationID)::INTEGER AS zone_key,
            LocationID::INTEGER   AS location_id,
            Borough               AS borough,
            Zone                  AS zone,
            service_zone          AS service_zone,
            -- SCD Type 2 columns: initially all records are current
            DATE '2009-01-01'     AS valid_from,
            DATE '9999-12-31'     AS valid_to,
            TRUE                  AS is_current
        FROM read_csv('{zones_file}', header=true, auto_detect=true);
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_zones").fetchone()[0]
    print(f"    -> {count} rows")


def create_dim_vendors(con: duckdb.DuckDBPyConnection) -> None:
    """Create the vendor dimension table from vendors.csv."""
    print("  Creating dim_vendors...")
    vendors_file = DATA_RAW / "vendors.csv"
    con.execute("DROP TABLE IF EXISTS dim_vendors;")
    con.execute(f"""
        CREATE TABLE dim_vendors AS
        SELECT
            vendor_id::INTEGER  AS vendor_id,
            vendor_name         AS vendor_name
        FROM read_csv('{vendors_file}', header=true, auto_detect=true);
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_vendors").fetchone()[0]
    print(f"    -> {count} rows")


def create_dim_rate_codes(con: duckdb.DuckDBPyConnection) -> None:
    """Create the rate code dimension table from rate_codes.csv."""
    print("  Creating dim_rate_codes...")
    rc_file = DATA_RAW / "rate_codes.csv"
    con.execute("DROP TABLE IF EXISTS dim_rate_codes;")
    con.execute(f"""
        CREATE TABLE dim_rate_codes AS
        SELECT
            rate_code_id::INTEGER  AS rate_code_id,
            rate_code_name         AS rate_code_name
        FROM read_csv('{rc_file}', header=true, auto_detect=true);
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_rate_codes").fetchone()[0]
    print(f"    -> {count} rows")


def create_dim_payment_types(con: duckdb.DuckDBPyConnection) -> None:
    """Create the payment type dimension table from payment_types.csv."""
    print("  Creating dim_payment_types...")
    pt_file = DATA_RAW / "payment_types.csv"
    con.execute("DROP TABLE IF EXISTS dim_payment_types;")
    con.execute(f"""
        CREATE TABLE dim_payment_types AS
        SELECT
            payment_type_id::INTEGER  AS payment_type_id,
            payment_type_name         AS payment_type_name
        FROM read_csv('{pt_file}', header=true, auto_detect=true);
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_payment_types").fetchone()[0]
    print(f"    -> {count} rows")


def create_dim_fhv_bases(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the for-hire vehicle base dimension table from fhv_bases.csv.

    FHV bases include Uber, Lyft, and traditional livery/black car companies.
    The base_number is the TLC-assigned license number.
    """
    print("  Creating dim_fhv_bases...")
    bases_file = DATA_RAW / "fhv_bases.csv"
    con.execute("DROP TABLE IF EXISTS dim_fhv_bases;")
    con.execute(f"""
        CREATE TABLE dim_fhv_bases AS
        SELECT
            base_number,
            base_name,
            dba,
            base_type
        FROM read_csv('{bases_file}', header=true, auto_detect=true);
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_fhv_bases").fetchone()[0]
    print(f"    -> {count} rows")


def create_dim_weather(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the daily weather dimension table from daily_weather.csv.

    Includes a derived weather_category column for easy filtering:
    'Snow', 'Rain', or 'Clear'.
    """
    print("  Creating dim_weather...")
    weather_file = DATA_RAW / "daily_weather.csv"
    con.execute("DROP TABLE IF EXISTS dim_weather;")
    con.execute(f"""
        CREATE TABLE dim_weather AS
        SELECT
            CAST(date AS DATE)              AS date,
            temp_min::DOUBLE                AS temp_min,
            temp_max::DOUBLE                AS temp_max,
            temp_avg::DOUBLE                AS temp_avg,
            precipitation::DOUBLE           AS precipitation,
            snow_depth::DOUBLE              AS snow_depth,
            wind_speed::DOUBLE              AS wind_speed,
            -- Derived category for easy weather-impact analysis
            CASE
                WHEN snow_depth > 0 THEN 'Snow'
                WHEN precipitation > 0 THEN 'Rain'
                ELSE 'Clear'
            END AS weather_category
        FROM read_csv('{weather_file}', header=true, auto_detect=true);
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_weather").fetchone()[0]
    print(f"    -> {count} rows")


def create_fact_yellow_trips(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the yellow taxi trips fact table from Parquet.

    Grain: one row per yellow taxi trip.

    DuckDB reads Parquet natively and efficiently -- this is the same pattern
    Databricks uses to read Delta Lake tables (which are Parquet under the hood).
    """
    print("  Creating fact_yellow_trips...")
    parquet_file = DATA_RAW / "yellow_taxi_trips.parquet"
    con.execute("DROP TABLE IF EXISTS fact_yellow_trips;")
    con.execute(f"""
        CREATE TABLE fact_yellow_trips AS
        SELECT
            ROW_NUMBER() OVER ()::INTEGER                   AS trip_id,
            d.date_key                                      AS pickup_date_key,
            CAST(tpep_pickup_datetime AS TIMESTAMP)         AS pickup_datetime,
            CAST(tpep_dropoff_datetime AS TIMESTAMP)        AS dropoff_datetime,
            VendorID::INTEGER                               AS vendor_id,
            PULocationID::INTEGER                           AS pickup_location_id,
            DOLocationID::INTEGER                           AS dropoff_location_id,
            RatecodeID::INTEGER                             AS rate_code_id,
            payment_type::INTEGER                           AS payment_type_id,
            passenger_count::INTEGER                        AS passenger_count,
            trip_distance::DOUBLE                           AS trip_distance,
            -- Duration in minutes derived from timestamps
            ROUND(EXTRACT(EPOCH FROM (
                CAST(tpep_dropoff_datetime AS TIMESTAMP)
                - CAST(tpep_pickup_datetime AS TIMESTAMP)
            )) / 60.0, 2)                                   AS trip_duration_minutes,
            fare_amount::DOUBLE                             AS fare_amount,
            extra::DOUBLE                                   AS extra,
            mta_tax::DOUBLE                                 AS mta_tax,
            tip_amount::DOUBLE                              AS tip_amount,
            tolls_amount::DOUBLE                            AS tolls_amount,
            improvement_surcharge::DOUBLE                   AS improvement_surcharge,
            total_amount::DOUBLE                            AS total_amount,
            congestion_surcharge::DOUBLE                    AS congestion_surcharge,
            airport_fee::DOUBLE                             AS airport_fee,
            store_and_fwd_flag                              AS store_and_fwd_flag
        FROM read_parquet('{parquet_file}') t
        LEFT JOIN dim_date d
            ON CAST(t.tpep_pickup_datetime AS DATE) = d.full_date
        -- Filter out obviously invalid trips
        WHERE tpep_pickup_datetime IS NOT NULL
          AND tpep_dropoff_datetime IS NOT NULL
          AND tpep_dropoff_datetime > tpep_pickup_datetime
          AND trip_distance >= 0
          AND fare_amount >= 0
        ORDER BY tpep_pickup_datetime;
    """)
    count = con.execute("SELECT COUNT(*) FROM fact_yellow_trips").fetchone()[0]
    print(f"    -> {count} rows")


def create_fact_green_trips(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the green taxi trips fact table from Parquet.

    Grain: one row per green taxi trip.
    Green taxis serve the outer boroughs (no pickups in core Manhattan).
    """
    print("  Creating fact_green_trips...")
    parquet_file = DATA_RAW / "green_taxi_trips.parquet"
    con.execute("DROP TABLE IF EXISTS fact_green_trips;")
    con.execute(f"""
        CREATE TABLE fact_green_trips AS
        SELECT
            ROW_NUMBER() OVER ()::INTEGER                   AS trip_id,
            d.date_key                                      AS pickup_date_key,
            CAST(lpep_pickup_datetime AS TIMESTAMP)         AS pickup_datetime,
            CAST(lpep_dropoff_datetime AS TIMESTAMP)        AS dropoff_datetime,
            VendorID::INTEGER                               AS vendor_id,
            PULocationID::INTEGER                           AS pickup_location_id,
            DOLocationID::INTEGER                           AS dropoff_location_id,
            RatecodeID::INTEGER                             AS rate_code_id,
            payment_type::INTEGER                           AS payment_type_id,
            passenger_count::INTEGER                        AS passenger_count,
            trip_distance::DOUBLE                           AS trip_distance,
            ROUND(EXTRACT(EPOCH FROM (
                CAST(lpep_dropoff_datetime AS TIMESTAMP)
                - CAST(lpep_pickup_datetime AS TIMESTAMP)
            )) / 60.0, 2)                                   AS trip_duration_minutes,
            fare_amount::DOUBLE                             AS fare_amount,
            extra::DOUBLE                                   AS extra,
            mta_tax::DOUBLE                                 AS mta_tax,
            tip_amount::DOUBLE                              AS tip_amount,
            tolls_amount::DOUBLE                            AS tolls_amount,
            improvement_surcharge::DOUBLE                   AS improvement_surcharge,
            total_amount::DOUBLE                            AS total_amount,
            congestion_surcharge::DOUBLE                    AS congestion_surcharge,
            trip_type::INTEGER                              AS trip_type
        FROM read_parquet('{parquet_file}') t
        LEFT JOIN dim_date d
            ON CAST(t.lpep_pickup_datetime AS DATE) = d.full_date
        WHERE lpep_pickup_datetime IS NOT NULL
          AND lpep_dropoff_datetime IS NOT NULL
          AND lpep_dropoff_datetime > lpep_pickup_datetime
          AND trip_distance >= 0
          AND fare_amount >= 0
        ORDER BY lpep_pickup_datetime;
    """)
    count = con.execute("SELECT COUNT(*) FROM fact_green_trips").fetchone()[0]
    print(f"    -> {count} rows")


def create_fact_fhv_trips(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the for-hire vehicle trips fact table from Parquet.

    Grain: one row per FHV trip.
    FHV trips include Uber, Lyft, and traditional livery/black car services.
    Note: FHV data does not include fare information (not reported to TLC).
    """
    print("  Creating fact_fhv_trips...")
    parquet_file = DATA_RAW / "fhv_trips.parquet"
    con.execute("DROP TABLE IF EXISTS fact_fhv_trips;")
    con.execute(f"""
        CREATE TABLE fact_fhv_trips AS
        SELECT
            ROW_NUMBER() OVER ()::INTEGER                   AS trip_id,
            d.date_key                                      AS pickup_date_key,
            CAST(pickup_datetime AS TIMESTAMP)              AS pickup_datetime,
            CAST(dropoff_datetime AS TIMESTAMP)             AS dropoff_datetime,
            dispatching_base_num                            AS dispatching_base_num,
            PULocationID::INTEGER                           AS pickup_location_id,
            DOLocationID::INTEGER                           AS dropoff_location_id,
            COALESCE(SR_Flag, 0)::INTEGER                   AS shared_ride_flag,
            -- Duration in minutes derived from timestamps
            ROUND(EXTRACT(EPOCH FROM (
                CAST(dropoff_datetime AS TIMESTAMP)
                - CAST(pickup_datetime AS TIMESTAMP)
            )) / 60.0, 2)                                   AS trip_duration_minutes
        FROM read_parquet('{parquet_file}') t
        LEFT JOIN dim_date d
            ON CAST(t.pickup_datetime AS DATE) = d.full_date
        WHERE pickup_datetime IS NOT NULL
          AND dropoff_datetime IS NOT NULL
          AND dropoff_datetime > pickup_datetime
        ORDER BY pickup_datetime;
    """)
    count = con.execute("SELECT COUNT(*) FROM fact_fhv_trips").fetchone()[0]
    print(f"    -> {count} rows")


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Print a summary of all tables in the warehouse."""
    print("\n" + "=" * 60)
    print("WAREHOUSE SUMMARY")
    print("=" * 60)

    tables = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """).fetchall()

    for (table_name,) in tables:
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        cols = con.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = '{table_name}' AND table_schema = 'main'
        """).fetchone()[0]
        print(f"  {table_name:<25} {count:>10,} rows  x  {cols:>3} columns")

    print("=" * 60)


def main():
    # Remove old warehouse file so we start fresh
    if WAREHOUSE_PATH.exists():
        WAREHOUSE_PATH.unlink()

    print(f"Creating warehouse at: {WAREHOUSE_PATH}")
    print(f"Reading raw data from: {DATA_RAW}")
    print()

    con = duckdb.connect(str(WAREHOUSE_PATH))

    try:
        # Build dimension tables first (fact tables reference them)
        print("Building dimension tables...")
        create_dim_date(con)
        create_dim_zones(con)
        create_dim_vendors(con)
        create_dim_rate_codes(con)
        create_dim_payment_types(con)
        create_dim_fhv_bases(con)
        create_dim_weather(con)

        # Build fact tables (join to dimension keys)
        print("\nBuilding fact tables...")
        create_fact_yellow_trips(con)
        create_fact_green_trips(con)
        create_fact_fhv_trips(con)

        # Show summary
        print_summary(con)
        print(f"\nWarehouse file: {WAREHOUSE_PATH}")
        print(f"File size: {WAREHOUSE_PATH.stat().st_size / (1024*1024):.1f} MB")
        print("\nDone! You can now query the warehouse with:")
        print(f"  duckdb {WAREHOUSE_PATH}")

    finally:
        con.close()


if __name__ == '__main__':
    main()
