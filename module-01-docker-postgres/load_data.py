#!/usr/bin/env python3
"""
Load raw podcast platform data into Postgres.

This is the solution script for Module 01. It demonstrates:
  - Connecting to Postgres with psycopg2
  - Loading JSON, CSV, and JSONL files
  - Data cleaning (messy dates, inconsistent genders, missing values)
  - Bulk loading with COPY via StringIO (10-50x faster than row-by-row INSERT)
  - Proper error handling, logging, and idempotency (UPSERT with ON CONFLICT)

Usage:
    python load_data.py

Prerequisites:
    pip install psycopg2-binary
"""

import csv
import io
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "user": os.getenv("POSTGRES_USER", "lakehouse"),
    "password": os.getenv("POSTGRES_PASSWORD", "lakehouse123"),
    "dbname": os.getenv("POSTGRES_DB", "podcast_platform"),
}

# Path to raw data -- works from module-01-docker-postgres/ directory
RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------


def get_connection():
    """Create a database connection with retry logic."""
    max_retries = 5
    retry_delay = 2

    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            logger.info("Connected to Postgres at %s:%s/%s", DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries:
                logger.warning("Connection attempt %d/%d failed: %s. Retrying in %ds...", attempt, max_retries, e, retry_delay)
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect after %d attempts.", max_retries)
                raise


# ---------------------------------------------------------------------------
# Data cleaning utilities
# ---------------------------------------------------------------------------


def parse_date(value: str) -> str | None:
    """
    Parse messy date strings into ISO format (YYYY-MM-DD).

    The raw users.csv has dates in at least 4 formats:
      - 2024-09-10        (ISO)
      - 23/09/2022        (DD/MM/YYYY)
      - 2022-09-21T00:00:00  (ISO with time)
      - 03-09-2019        (DD-MM-YYYY)
    """
    if not value or not value.strip():
        return None

    value = value.strip()

    # Remove time component if present
    if "T" in value:
        value = value.split("T")[0]

    formats = [
        "%Y-%m-%d",   # 2024-09-10
        "%d/%m/%Y",   # 23/09/2022
        "%d-%m-%Y",   # 03-09-2019
        "%m-%d-%Y",   # fallback
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning("Could not parse date: '%s', returning None", value)
    return None


def normalize_gender(value: str) -> str | None:
    """Normalize gender to 'm' or 'f'."""
    if not value or not value.strip():
        return None
    v = value.strip().lower()
    if v in ("m", "male"):
        return "m"
    if v in ("f", "female"):
        return "f"
    logger.warning("Unknown gender value: '%s'", value)
    return None


def normalize_subscription(value: str) -> str:
    """Normalize subscription type to one of: free, premium, trial."""
    if not value or not value.strip():
        return "free"
    v = value.strip().lower()
    if v in ("premium", "premium_annual", "premium_monthly"):
        return "premium"
    if v in ("trial",):
        return "trial"
    return "free"


def safe_int(value) -> int | None:
    """Convert to int, return None for empty/invalid values."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def load_podcasts(conn):
    """Load podcasts.json into the podcasts table."""
    filepath = RAW_DATA_DIR / "podcasts.json"
    logger.info("Loading podcasts from %s", filepath)

    with open(filepath, "r", encoding="utf-8") as f:
        podcasts = json.load(f)

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO podcasts (podcast_id, name, name_en, category, language, host, created_at)
            VALUES %s
            ON CONFLICT (podcast_id) DO UPDATE SET
                name = EXCLUDED.name,
                name_en = EXCLUDED.name_en,
                category = EXCLUDED.category,
                language = EXCLUDED.language,
                host = EXCLUDED.host,
                created_at = EXCLUDED.created_at
            """,
            [
                (
                    p["podcast_id"],
                    p["name"],
                    p.get("name_en"),
                    p["category"],
                    p.get("language", "ar"),
                    p["host"],
                    p["created_at"],
                )
                for p in podcasts
            ],
        )
    conn.commit()
    logger.info("Loaded %d podcasts", len(podcasts))


def load_episodes(conn):
    """Load episodes.json into the episodes table."""
    filepath = RAW_DATA_DIR / "episodes.json"
    logger.info("Loading episodes from %s", filepath)

    with open(filepath, "r", encoding="utf-8") as f:
        episodes = json.load(f)

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO episodes (episode_id, podcast_id, title, published_at, duration_seconds, season, episode_number)
            VALUES %s
            ON CONFLICT (episode_id) DO UPDATE SET
                podcast_id = EXCLUDED.podcast_id,
                title = EXCLUDED.title,
                published_at = EXCLUDED.published_at,
                duration_seconds = EXCLUDED.duration_seconds,
                season = EXCLUDED.season,
                episode_number = EXCLUDED.episode_number
            """,
            [
                (
                    e["episode_id"],
                    e["podcast_id"],
                    e["title"],
                    e["published_at"],
                    e["duration_seconds"],
                    e.get("season"),
                    e.get("episode_number"),
                )
                for e in episodes
            ],
        )
    conn.commit()
    logger.info("Loaded %d episodes", len(episodes))


def load_users(conn):
    """
    Load users.csv into the users table.

    This is the messy one. The raw data has:
      - Multiple date formats
      - Inconsistent gender values (m, male, M, f, female, F)
      - Missing ages and cities (empty strings)
      - subscription_type values that need normalization (premium_annual -> premium)
    """
    filepath = RAW_DATA_DIR / "users.csv"
    logger.info("Loading users from %s", filepath)

    rows = []
    skipped = 0

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            signup_date = parse_date(row.get("signup_date", ""))
            gender = normalize_gender(row.get("gender", ""))
            subscription = normalize_subscription(row.get("subscription_type", ""))
            age = safe_int(row.get("age"))
            city = row.get("city", "").strip() or None

            rows.append((
                row["user_id"],
                row.get("name"),
                row.get("email"),
                row.get("country"),
                city,
                row.get("platform"),
                signup_date,
                subscription,
                age,
                gender,
            ))

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO users (user_id, name, email, country, city, platform, signup_date, subscription_type, age, gender)
            VALUES %s
            ON CONFLICT (user_id) DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                country = EXCLUDED.country,
                city = EXCLUDED.city,
                platform = EXCLUDED.platform,
                signup_date = EXCLUDED.signup_date,
                subscription_type = EXCLUDED.subscription_type,
                age = EXCLUDED.age,
                gender = EXCLUDED.gender
            """,
            rows,
            page_size=1000,
        )
    conn.commit()
    logger.info("Loaded %d users (%d skipped)", len(rows), skipped)


def load_listening_events(conn):
    """
    Load listening events from JSONL files using COPY for performance.

    COPY is the fastest way to bulk-load data into Postgres. We read JSONL,
    transform to TSV in memory, then stream it into the table with copy_expert.
    """
    events_dir = RAW_DATA_DIR / "listening_events"
    jsonl_files = sorted(events_dir.glob("*.jsonl"))
    logger.info("Found %d JSONL files in %s", len(jsonl_files), events_dir)

    total_loaded = 0
    start_time = time.time()

    # Process in batches of files to manage memory
    BATCH_SIZE = 100

    with conn.cursor() as cur:
        for batch_start in range(0, len(jsonl_files), BATCH_SIZE):
            batch_files = jsonl_files[batch_start:batch_start + BATCH_SIZE]
            buffer = io.StringIO()

            for filepath in batch_files:
                with open(filepath, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        event = json.loads(line)

                        # Write TSV line for COPY
                        # Column order must match the COPY statement below
                        row = "\t".join([
                            event["event_id"],
                            event["user_id"],
                            event["episode_id"],
                            event["event_type"],
                            event["timestamp"],
                            str(event.get("listened_seconds", 0)),
                            event.get("platform", ""),
                            event.get("country", ""),
                            event.get("app_version", ""),
                        ])
                        buffer.write(row + "\n")

            buffer.seek(0)
            # Use a temp table + INSERT ... ON CONFLICT for idempotency
            cur.execute("""
                CREATE TEMP TABLE tmp_events (LIKE listening_events INCLUDING DEFAULTS)
                ON COMMIT DROP
            """)
            cur.copy_expert(
                """
                COPY tmp_events (event_id, user_id, episode_id, event_type, event_timestamp,
                                 listened_seconds, platform, country, app_version)
                FROM STDIN WITH (FORMAT text, DELIMITER E'\\t')
                """,
                buffer,
            )

            cur.execute("""
                INSERT INTO listening_events
                SELECT * FROM tmp_events
                ON CONFLICT (event_id) DO NOTHING
            """)
            batch_count = cur.rowcount
            total_loaded += batch_count
            conn.commit()

            elapsed = time.time() - start_time
            logger.info(
                "Batch %d-%d: loaded %d events (total: %d, elapsed: %.1fs)",
                batch_start,
                batch_start + len(batch_files),
                batch_count,
                total_loaded,
                elapsed,
            )

    elapsed = time.time() - start_time
    logger.info("Loaded %d listening events in %.1f seconds", total_loaded, elapsed)


def load_cdn_logs(conn):
    """Load cdn_logs.csv using COPY for performance."""
    filepath = RAW_DATA_DIR / "cdn_logs.csv"
    logger.info("Loading CDN logs from %s", filepath)

    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((
                row["log_id"],
                row.get("event_id") or None,
                row.get("user_id") or None,
                row["timestamp"],
                row.get("isp"),
                row.get("bitrate"),
                safe_int(row.get("buffer_events", 0)),
                float(row.get("rebuffer_ratio", 0)) if row.get("rebuffer_ratio") else 0,
                safe_int(row.get("startup_time_ms")),
                row.get("error_type") or None,
                row.get("cdn_node"),
                int(row.get("bytes_transferred", 0)) if row.get("bytes_transferred") else 0,
            ))

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO cdn_logs (log_id, event_id, user_id, log_timestamp, isp, bitrate,
                                  buffer_events, rebuffer_ratio, startup_time_ms, error_type,
                                  cdn_node, bytes_transferred)
            VALUES %s
            ON CONFLICT (log_id) DO NOTHING
            """,
            rows,
            page_size=5000,
        )
    conn.commit()
    logger.info("Loaded %d CDN log records", len(rows))


def load_ad_events(conn):
    """Load ad_events.json into the ad_events table."""
    filepath = RAW_DATA_DIR / "ad_events.json"
    logger.info("Loading ad events from %s", filepath)

    with open(filepath, "r", encoding="utf-8") as f:
        events = json.load(f)

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO ad_events (ad_event_id, event_id, user_id, ad_timestamp, ad_type,
                                   action, advertiser, campaign_id, revenue_sar, duration_seconds)
            VALUES %s
            ON CONFLICT (ad_event_id) DO NOTHING
            """,
            [
                (
                    e["ad_event_id"],
                    e.get("event_id"),
                    e.get("user_id"),
                    e["timestamp"],
                    e["ad_type"],
                    e["action"],
                    e.get("advertiser"),
                    e.get("campaign_id"),
                    float(e.get("revenue_sar", 0)),
                    safe_int(e.get("duration_seconds")),
                )
                for e in events
            ],
            page_size=5000,
        )
    conn.commit()
    logger.info("Loaded %d ad events", len(events))


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def verify_loads(conn):
    """Print row counts for all tables to verify successful loading."""
    tables = ["podcasts", "episodes", "users", "listening_events", "cdn_logs", "ad_events"]

    logger.info("=" * 50)
    logger.info("VERIFICATION: Row counts")
    logger.info("=" * 50)

    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608 -- table names are hardcoded
            count = cur.fetchone()[0]
            logger.info("  %-20s %8d rows", table, count)

    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    """Load all raw data into Postgres in dependency order."""
    logger.info("Starting data load from %s", RAW_DATA_DIR)

    if not RAW_DATA_DIR.exists():
        logger.error("Raw data directory not found: %s", RAW_DATA_DIR)
        sys.exit(1)

    conn = get_connection()

    try:
        # Load in dependency order (podcasts before episodes, users before events)
        load_podcasts(conn)
        load_episodes(conn)
        load_users(conn)
        load_listening_events(conn)
        load_cdn_logs(conn)
        load_ad_events(conn)

        verify_loads(conn)

        logger.info("All data loaded successfully.")

    except Exception:
        logger.exception("Data load failed")
        conn.rollback()
        sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
