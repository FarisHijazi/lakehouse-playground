"""
Module 04: SCD Type 2 Implementation
=====================================
Implements Slowly Changing Dimension Type 2 for the podcasts dimension table.

SCD Type 2 preserves full history of attribute changes by:
  1. Closing the current record (setting valid_to and is_current=false)
  2. Inserting a new record with the updated values and a new surrogate key
  3. Linking fact rows to the correct version based on event date

This script processes three simulated change events:
  - 2023-07-01: pod_001 renames from "سوالف بزنس" to "سوالف بزنس وتقنية"
  - 2024-01-15: pod_001 changes category from "Business" to "Business & Technology"
  - 2024-03-01: pod_003 changes host from "سارة" to "سارة ونورة"

Usage:
    cd module-04-data-warehouse
    python solutions/create_warehouse.py   # Build the warehouse first
    python solutions/scd_type2.py          # Apply SCD Type 2 changes
"""

import os
from datetime import date
from typing import Optional

import duckdb


def get_warehouse_path() -> str:
    """Return the path to the warehouse.duckdb file."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'warehouse.duckdb'))


def apply_scd2_change(
    con: duckdb.DuckDBPyConnection,
    podcast_id: str,
    change_date: date,
    new_values: dict,
) -> None:
    """
    Apply an SCD Type 2 change to dim_podcasts.

    Steps:
      1. Find the current record for this podcast_id (is_current = true).
      2. Close that record: set valid_to = change_date - 1 day, is_current = false.
      3. Create a new record with the updated attribute(s), a new surrogate key,
         valid_from = change_date, valid_to = 9999-12-31, is_current = true.

    Parameters
    ----------
    con : DuckDB connection
    podcast_id : The natural key of the podcast to update
    change_date : The effective date of the change
    new_values : Dict of column_name -> new_value for the changed attributes
    """
    # -------------------------------------------------------------------------
    # Step 1: Fetch the current record for this podcast
    # -------------------------------------------------------------------------
    current = con.execute("""
        SELECT podcast_key, podcast_id, name, name_en, category,
               language, host, valid_from, valid_to, is_current
        FROM dim_podcasts
        WHERE podcast_id = ? AND is_current = true
    """, [podcast_id]).fetchone()

    if current is None:
        raise ValueError(f"No current record found for podcast_id={podcast_id}")

    # Unpack current values into a dict for easy manipulation
    columns = ['podcast_key', 'podcast_id', 'name', 'name_en', 'category',
               'language', 'host', 'valid_from', 'valid_to', 'is_current']
    current_dict = dict(zip(columns, current))

    print(f"\n  Processing change for {podcast_id} on {change_date}:")
    for col, new_val in new_values.items():
        old_val = current_dict[col]
        print(f"    {col}: '{old_val}' -> '{new_val}'")

    # -------------------------------------------------------------------------
    # Step 2: Close the current record
    #   Set valid_to to the day BEFORE the change takes effect.
    #   This ensures there is no overlap: the old version is valid up to
    #   change_date - 1, and the new version starts on change_date.
    # -------------------------------------------------------------------------
    close_date = date(change_date.year, change_date.month, change_date.day)
    close_date_str = str(date.fromordinal(close_date.toordinal() - 1))

    con.execute("""
        UPDATE dim_podcasts
        SET valid_to = CAST(? AS DATE),
            is_current = false
        WHERE podcast_id = ?
          AND is_current = true
    """, [close_date_str, podcast_id])

    # -------------------------------------------------------------------------
    # Step 3: Generate a new surrogate key
    #   We use MAX(podcast_key) + 1. In production, you would use a sequence
    #   or identity column, but this demonstrates the concept clearly.
    # -------------------------------------------------------------------------
    max_key = con.execute("SELECT MAX(podcast_key) FROM dim_podcasts").fetchone()[0]
    new_key = max_key + 1

    # -------------------------------------------------------------------------
    # Step 4: Build the new record
    #   Start with all values from the current record, then overwrite
    #   only the attributes that changed.
    # -------------------------------------------------------------------------
    new_record = dict(current_dict)
    new_record['podcast_key'] = new_key
    new_record['valid_from'] = change_date
    new_record['valid_to'] = date(9999, 12, 31)
    new_record['is_current'] = True

    # Apply the changed attributes
    for col, new_val in new_values.items():
        new_record[col] = new_val

    # -------------------------------------------------------------------------
    # Step 5: Insert the new record
    # -------------------------------------------------------------------------
    con.execute("""
        INSERT INTO dim_podcasts
            (podcast_key, podcast_id, name, name_en, category,
             language, host, valid_from, valid_to, is_current)
        VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?)
    """, [
        new_record['podcast_key'],
        new_record['podcast_id'],
        new_record['name'],
        new_record['name_en'],
        new_record['category'],
        new_record['language'],
        new_record['host'],
        str(new_record['valid_from']),
        str(new_record['valid_to']),
        new_record['is_current'],
    ])

    print(f"    -> Closed old record (podcast_key={current_dict['podcast_key']})")
    print(f"    -> Created new record (podcast_key={new_key})")


def verify_scd2(con: duckdb.DuckDBPyConnection) -> None:
    """
    Verify the SCD Type 2 results by running diagnostic queries.

    After processing the three changes, we expect:
      - 13 total rows (10 original + 3 new versions)
      - 10 rows where is_current = true
      - 3 rows for pod_001 (original + 2 changes)
      - 2 rows for pod_003 (original + 1 change)
    """
    print("\n" + "=" * 60)
    print("SCD TYPE 2 VERIFICATION")
    print("=" * 60)

    # Total rows
    total = con.execute("SELECT COUNT(*) FROM dim_podcasts").fetchone()[0]
    print(f"\n  Total rows in dim_podcasts: {total} (expected: 13)")

    # Current rows
    current = con.execute(
        "SELECT COUNT(*) FROM dim_podcasts WHERE is_current = true"
    ).fetchone()[0]
    print(f"  Current rows (is_current=true): {current} (expected: 10)")

    # History for pod_001
    pod001_count = con.execute(
        "SELECT COUNT(*) FROM dim_podcasts WHERE podcast_id = 'pod_001'"
    ).fetchone()[0]
    print(f"  Versions for pod_001: {pod001_count} (expected: 3)")

    # History for pod_003
    pod003_count = con.execute(
        "SELECT COUNT(*) FROM dim_podcasts WHERE podcast_id = 'pod_003'"
    ).fetchone()[0]
    print(f"  Versions for pod_003: {pod003_count} (expected: 2)")

    # Show full history for pod_001
    print("\n  Full history for pod_001 (Swalif Business):")
    print("  " + "-" * 90)
    rows = con.execute("""
        SELECT podcast_key, name, category, valid_from, valid_to, is_current
        FROM dim_podcasts
        WHERE podcast_id = 'pod_001'
        ORDER BY valid_from
    """).fetchall()
    for row in rows:
        key, name, cat, vf, vt, ic = row
        status = "CURRENT" if ic else "EXPIRED"
        print(f"    key={key:>2}  name='{name}'  category='{cat}'  "
              f"valid=[{vf} to {vt}]  {status}")

    # Show full history for pod_003
    print("\n  Full history for pod_003 (Ariika Podcast):")
    print("  " + "-" * 90)
    rows = con.execute("""
        SELECT podcast_key, name, host, valid_from, valid_to, is_current
        FROM dim_podcasts
        WHERE podcast_id = 'pod_003'
        ORDER BY valid_from
    """).fetchall()
    for row in rows:
        key, name, host, vf, vt, ic = row
        status = "CURRENT" if ic else "EXPIRED"
        print(f"    key={key:>2}  name='{name}'  host='{host}'  "
              f"valid=[{vf} to {vt}]  {status}")

    # Demonstrate point-in-time lookup
    print("\n  Point-in-time lookup: What was pod_001 called on 2023-08-15?")
    result = con.execute("""
        SELECT name, category
        FROM dim_podcasts
        WHERE podcast_id = 'pod_001'
          AND DATE '2023-08-15' BETWEEN valid_from AND valid_to
    """).fetchone()
    if result:
        print(f"    -> name='{result[0]}', category='{result[1]}'")

    print("\n  Point-in-time lookup: What was pod_001 called on 2022-12-01?")
    result = con.execute("""
        SELECT name, category
        FROM dim_podcasts
        WHERE podcast_id = 'pod_001'
          AND DATE '2022-12-01' BETWEEN valid_from AND valid_to
    """).fetchone()
    if result:
        print(f"    -> name='{result[0]}', category='{result[1]}'")

    print("\n" + "=" * 60)

    # Validate expectations
    assert total == 13, f"Expected 13 rows, got {total}"
    assert current == 10, f"Expected 10 current rows, got {current}"
    assert pod001_count == 3, f"Expected 3 versions of pod_001, got {pod001_count}"
    assert pod003_count == 2, f"Expected 2 versions of pod_003, got {pod003_count}"
    print("  All assertions passed!")


def main():
    warehouse_path = get_warehouse_path()

    if not os.path.exists(warehouse_path):
        print(f"Warehouse not found at {warehouse_path}")
        print("Run create_warehouse.py first.")
        return

    print(f"Connecting to warehouse: {warehouse_path}")
    con = duckdb.connect(warehouse_path)

    try:
        # ---------------------------------------------------------------------
        # Check if SCD changes have already been applied
        # (idempotency: avoid duplicate rows on re-run)
        # ---------------------------------------------------------------------
        row_count = con.execute("SELECT COUNT(*) FROM dim_podcasts").fetchone()[0]
        if row_count > 10:
            print("SCD Type 2 changes appear to have been applied already.")
            print("To re-run, first rebuild the warehouse with create_warehouse.py.")
            verify_scd2(con)
            return

        print("\nApplying SCD Type 2 changes...")

        # ---------------------------------------------------------------------
        # Change 1: pod_001 renames on 2023-07-01
        #   "سوالف بزنس" -> "سوالف بزنس وتقنية"
        #   "Swalif Business" -> "Swalif Business & Tech"
        # ---------------------------------------------------------------------
        apply_scd2_change(
            con,
            podcast_id='pod_001',
            change_date=date(2023, 7, 1),
            new_values={
                'name': 'سوالف بزنس وتقنية',
                'name_en': 'Swalif Business & Tech',
            },
        )

        # ---------------------------------------------------------------------
        # Change 2: pod_001 changes category on 2024-01-15
        #   "Business" -> "Business & Technology"
        # ---------------------------------------------------------------------
        apply_scd2_change(
            con,
            podcast_id='pod_001',
            change_date=date(2024, 1, 15),
            new_values={
                'category': 'Business & Technology',
            },
        )

        # ---------------------------------------------------------------------
        # Change 3: pod_003 changes host on 2024-03-01
        #   "سارة" -> "سارة ونورة"
        # ---------------------------------------------------------------------
        apply_scd2_change(
            con,
            podcast_id='pod_003',
            change_date=date(2024, 3, 1),
            new_values={
                'host': 'سارة ونورة',
            },
        )

        # Verify the results
        verify_scd2(con)

    finally:
        con.close()


if __name__ == '__main__':
    main()
