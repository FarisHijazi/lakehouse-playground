"""
Module 04: Create Data Warehouse
=================================
This script builds a star schema data warehouse in DuckDB from raw podcast data.

It creates:
  - dim_dates:    Calendar dimension (2018-01-01 to 2025-12-31)
  - dim_users:    User dimension from users.csv
  - dim_podcasts: Podcast dimension from podcasts.json
  - dim_episodes: Episode dimension from episodes.json
  - fact_listens: Listening events from listening_events/*.jsonl
  - fact_ad_events: Ad impression/click events from ad_events.json
  - fact_cdn_quality: CDN quality metrics from cdn_logs.csv

Usage:
    cd module-04-data-warehouse
    python solutions/create_warehouse.py
"""

import os
import duckdb


def get_data_path(filename: str) -> str:
    """Return the absolute path to a raw data file."""
    base = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw')
    return os.path.abspath(os.path.join(base, filename))


def get_warehouse_path() -> str:
    """Return the path where the warehouse.duckdb file will be created."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'warehouse.duckdb'))


def create_dim_dates(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create the date dimension table.

    This is a standard calendar dimension covering 2018-01-01 to 2025-12-31.
    The date_key is an integer in YYYYMMDD format for efficient joins and
    human-readable partition pruning.
    """
    print("  Creating dim_dates...")
    con.execute("DROP TABLE IF EXISTS dim_dates;")
    con.execute("""
        CREATE TABLE dim_dates AS
        WITH date_spine AS (
            -- Generate one row per day from 2018 through 2025
            SELECT UNNEST(generate_series(
                DATE '2018-01-01',
                DATE '2025-12-31',
                INTERVAL 1 DAY
            ))::DATE AS full_date
        )
        SELECT
            -- Surrogate key in YYYYMMDD format for readability and fast integer joins
            (YEAR(full_date) * 10000 + MONTH(full_date) * 100 + DAY(full_date))::INTEGER AS date_key,
            full_date,
            YEAR(full_date)::INTEGER        AS year,
            QUARTER(full_date)::INTEGER     AS quarter,
            MONTH(full_date)::INTEGER       AS month,
            MONTHNAME(full_date)            AS month_name,
            DAY(full_date)::INTEGER         AS day_of_month,
            ISODOW(full_date)::INTEGER      AS day_of_week,   -- 1=Monday, 7=Sunday
            DAYNAME(full_date)              AS day_name,
            WEEKOFYEAR(full_date)::INTEGER  AS week_of_year,
            -- Weekend flag: Saturday=6, Sunday=7
            (ISODOW(full_date) >= 6)        AS is_weekend
        FROM date_spine
        ORDER BY full_date;
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_dates").fetchone()[0]
    print(f"    -> {count} rows")


def create_dim_users(con: duckdb.DuckDBPyConnection, data_path: str) -> None:
    """
    Create the users dimension table.

    Handles several data quality issues in the raw CSV:
      - Multiple date formats for signup_date (YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, ISO 8601)
      - Inconsistent gender values (f, F, female, m, M, male)
      - NULL city values
    """
    print("  Creating dim_users...")
    users_file = os.path.join(data_path, 'users.csv')
    con.execute("DROP TABLE IF EXISTS dim_users;")
    con.execute(f"""
        CREATE TABLE dim_users AS
        WITH raw AS (
            SELECT * FROM read_csv('{users_file}',
                header=true,
                all_varchar=true,   -- Read everything as strings first for date parsing
                nullstr=''
            )
        )
        SELECT
            -- Surrogate key: sequential integer for warehouse joins
            ROW_NUMBER() OVER (ORDER BY user_id)::INTEGER AS user_key,
            user_id,
            name,
            email,
            country,
            city,
            platform,
            -- Parse the messy signup_date: try multiple formats
            -- DuckDB's TRY_CAST handles ISO formats; strptime handles DD/MM/YYYY
            COALESCE(
                TRY_CAST(signup_date AS DATE),
                TRY_STRPTIME(signup_date, '%d/%m/%Y')::DATE,
                TRY_STRPTIME(signup_date, '%d-%m-%Y')::DATE
            ) AS signup_date,
            subscription_type,
            TRY_CAST(age AS INTEGER) AS age,
            -- Standardise gender to 'M' or 'F'
            CASE
                WHEN LOWER(gender) IN ('m', 'male')   THEN 'M'
                WHEN LOWER(gender) IN ('f', 'female')  THEN 'F'
                ELSE gender
            END AS gender
        FROM raw;
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_users").fetchone()[0]
    print(f"    -> {count} rows")


def create_dim_podcasts(con: duckdb.DuckDBPyConnection, data_path: str) -> None:
    """
    Create the podcasts dimension table.

    Includes SCD Type 2 metadata columns (valid_from, valid_to, is_current)
    so that the table is ready for tracking historical changes. On initial
    load, all records are marked as current.
    """
    print("  Creating dim_podcasts...")
    podcasts_file = os.path.join(data_path, 'podcasts.json')
    con.execute("DROP TABLE IF EXISTS dim_podcasts;")
    con.execute(f"""
        CREATE TABLE dim_podcasts AS
        SELECT
            -- Surrogate key for warehouse joins
            ROW_NUMBER() OVER (ORDER BY podcast_id)::INTEGER AS podcast_key,
            podcast_id,
            name,
            name_en,
            category,
            language,
            host,
            -- SCD Type 2 columns: initially all records are current
            CAST(created_at AS DATE)   AS valid_from,
            DATE '9999-12-31'          AS valid_to,
            TRUE                       AS is_current
        FROM read_json('{podcasts_file}');
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_podcasts").fetchone()[0]
    print(f"    -> {count} rows")


def create_dim_episodes(con: duckdb.DuckDBPyConnection, data_path: str) -> None:
    """
    Create the episodes dimension table.

    Derives duration_minutes from duration_seconds for analyst convenience.
    Retains podcast_id as a degenerate dimension key so analysts can join
    to dim_podcasts without going through a fact table.
    """
    print("  Creating dim_episodes...")
    episodes_file = os.path.join(data_path, 'episodes.json')
    con.execute("DROP TABLE IF EXISTS dim_episodes;")
    con.execute(f"""
        CREATE TABLE dim_episodes AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY episode_id)::INTEGER AS episode_key,
            episode_id,
            podcast_id,
            title,
            CAST(published_at AS TIMESTAMP) AS published_at,
            duration_seconds::INTEGER       AS duration_seconds,
            -- Derived column: minutes is more intuitive for analysts
            ROUND(duration_seconds / 60.0, 1) AS duration_minutes,
            season::INTEGER                 AS season,
            episode_number::INTEGER         AS episode_number
        FROM read_json('{episodes_file}');
    """)
    count = con.execute("SELECT COUNT(*) FROM dim_episodes").fetchone()[0]
    print(f"    -> {count} rows")


def create_fact_listens(con: duckdb.DuckDBPyConnection, data_path: str) -> None:
    """
    Create the listening events fact table.

    Grain: one row per listening event.

    Joins to dimension tables using surrogate keys for optimal performance.
    Calculates completion_pct by dividing listened_seconds by episode duration.
    """
    print("  Creating fact_listens...")
    events_glob = os.path.join(data_path, 'listening_events', '*.jsonl')
    con.execute("DROP TABLE IF EXISTS fact_listens;")
    con.execute(f"""
        CREATE TABLE fact_listens AS
        WITH raw_events AS (
            -- DuckDB reads all JSONL files matching the glob pattern
            SELECT * FROM read_json('{events_glob}',
                format='newline_delimited',
                columns={{
                    event_id: 'VARCHAR',
                    user_id: 'VARCHAR',
                    episode_id: 'VARCHAR',
                    event_type: 'VARCHAR',
                    timestamp: 'TIMESTAMP',
                    listened_seconds: 'INTEGER',
                    platform: 'VARCHAR',
                    country: 'VARCHAR',
                    app_version: 'VARCHAR'
                }}
            )
        )
        SELECT
            e.event_id,
            -- Surrogate key lookups for dimension joins
            u.user_key,
            ep.episode_key,
            d.date_key,
            e.event_type,
            e.listened_seconds,
            -- Completion percentage: what fraction of the episode was listened to
            CASE
                WHEN ep.duration_seconds > 0
                THEN ROUND(LEAST(e.listened_seconds::DOUBLE / ep.duration_seconds, 1.0), 4)
                ELSE 0.0
            END AS completion_pct,
            e.platform,
            e.country,
            CAST(e.timestamp AS TIMESTAMP) AS event_timestamp,
            CAST(e.timestamp AS DATE)      AS event_date
        FROM raw_events e
        -- Left joins ensure we keep events even if dimension lookup fails
        LEFT JOIN dim_users    u  ON e.user_id    = u.user_id
        LEFT JOIN dim_episodes ep ON e.episode_id = ep.episode_id
        LEFT JOIN dim_dates    d  ON CAST(e.timestamp AS DATE) = d.full_date
        -- Physical ordering by date for scan efficiency (simulates sort key)
        ORDER BY e.timestamp;
    """)
    count = con.execute("SELECT COUNT(*) FROM fact_listens").fetchone()[0]
    print(f"    -> {count} rows")


def create_fact_ad_events(con: duckdb.DuckDBPyConnection, data_path: str) -> None:
    """
    Create the ad events fact table.

    Grain: one row per ad impression or click event.

    Links to listening events via event_id, enabling revenue attribution
    from ads back to specific episodes and podcasts.
    """
    print("  Creating fact_ad_events...")
    ad_file = os.path.join(data_path, 'ad_events.json')
    con.execute("DROP TABLE IF EXISTS fact_ad_events;")
    con.execute(f"""
        CREATE TABLE fact_ad_events AS
        SELECT
            a.ad_event_id,
            u.user_key,
            d.date_key,
            a.event_id,              -- Links to fact_listens for attribution
            a.ad_type,               -- pre_roll, mid_roll, post_roll
            a.action,                -- impression, click, skip
            a.advertiser,
            a.campaign_id,
            a.revenue_sar::DOUBLE    AS revenue_sar,
            a.duration_seconds::INTEGER AS ad_duration_seconds,
            CAST(a.timestamp AS TIMESTAMP) AS event_timestamp,
            CAST(a.timestamp AS DATE)      AS event_date
        FROM read_json('{ad_file}') a
        LEFT JOIN dim_users u ON a.user_id = u.user_id
        LEFT JOIN dim_dates d ON CAST(a.timestamp AS DATE) = d.full_date
        ORDER BY a.timestamp;
    """)
    count = con.execute("SELECT COUNT(*) FROM fact_ad_events").fetchone()[0]
    print(f"    -> {count} rows")


def create_fact_cdn_quality(con: duckdb.DuckDBPyConnection, data_path: str) -> None:
    """
    Create the CDN quality fact table.

    Grain: one row per CDN log entry (one streaming request).

    Tracks streaming quality metrics: buffering, startup time, errors.
    Useful for monitoring platform reliability by ISP, CDN node, and time.
    """
    print("  Creating fact_cdn_quality...")
    cdn_file = os.path.join(data_path, 'cdn_logs.csv')
    con.execute("DROP TABLE IF EXISTS fact_cdn_quality;")
    con.execute(f"""
        CREATE TABLE fact_cdn_quality AS
        SELECT
            c.log_id,
            u.user_key,
            d.date_key,
            c.event_id,              -- Links to fact_listens
            c.isp,
            c.bitrate,
            c.buffer_events::INTEGER        AS buffer_events,
            c.rebuffer_ratio::DOUBLE        AS rebuffer_ratio,
            c.startup_time_ms::INTEGER      AS startup_time_ms,
            c.error_type,
            c.cdn_node,
            c.bytes_transferred::BIGINT     AS bytes_transferred,
            CAST(c.timestamp AS TIMESTAMP)  AS event_timestamp,
            CAST(c.timestamp AS DATE)       AS event_date
        FROM read_csv('{cdn_file}', header=true, nullstr='') c
        LEFT JOIN dim_users u ON c.user_id = u.user_id
        LEFT JOIN dim_dates d ON CAST(c.timestamp AS DATE) = d.full_date
        ORDER BY c.timestamp;
    """)
    count = con.execute("SELECT COUNT(*) FROM fact_cdn_quality").fetchone()[0]
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
    warehouse_path = get_warehouse_path()
    data_path = get_data_path('')

    # Remove old warehouse file if it exists so we start fresh
    if os.path.exists(warehouse_path):
        os.remove(warehouse_path)

    print(f"Creating warehouse at: {warehouse_path}")
    print(f"Reading raw data from: {data_path}")
    print()

    # Connect to a persistent DuckDB file
    con = duckdb.connect(warehouse_path)

    try:
        # Build dimension tables first (fact tables reference them)
        print("Building dimension tables...")
        create_dim_dates(con)
        create_dim_users(con, data_path)
        create_dim_podcasts(con, data_path)
        create_dim_episodes(con, data_path)

        # Build fact tables (join to dimension surrogate keys)
        print("\nBuilding fact tables...")
        create_fact_listens(con, data_path)
        create_fact_ad_events(con, data_path)
        create_fact_cdn_quality(con, data_path)

        # Show summary
        print_summary(con)
        print(f"\nWarehouse file: {warehouse_path}")
        print(f"File size: {os.path.getsize(warehouse_path) / (1024*1024):.1f} MB")
        print("\nDone! You can now query the warehouse with:")
        print(f"  duckdb {warehouse_path}")

    finally:
        con.close()


if __name__ == '__main__':
    main()
