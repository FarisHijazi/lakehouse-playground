"""
Module 04: SCD Type 2 Implementation
=====================================
Implements Slowly Changing Dimension Type 2 for the taxi zones dimension table.

SCD Type 2 preserves full history of attribute changes by:
  1. Closing the current record (setting valid_to and is_current=false)
  2. Inserting a new record with the updated values and a new surrogate key
  3. Linking fact rows to the correct version based on event date

This script processes three simulated zone change events:
  - 2023-07-01: Zone 261 renames from "World Trade Center" to
                "World Trade Center / Battery Park"
  - 2024-01-15: Zone 132 (JFK Airport) changes service_zone from
                "Airports" to "Major Airports"
  - 2024-06-01: Zone 138 (LaGuardia Airport) changes borough from
                "Queens" to "Airport Authority"

In the real world, taxi zone boundaries and classifications do change over
time as the TLC updates its geographic definitions. Rate codes also evolve
as new fare structures are introduced.

Usage:
    cd module-04-data-warehouse
    python solutions/create_warehouse.py   # Build the warehouse first
    python solutions/scd_type2.py          # Apply SCD Type 2 changes
"""

from datetime import date, timedelta
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Project paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
WAREHOUSE_PATH = Path(__file__).resolve().parent.parent / "warehouse.duckdb"


def apply_scd2_change(
    con: duckdb.DuckDBPyConnection,
    table: str,
    natural_key_col: str,
    natural_key_val,
    change_date: date,
    new_values: dict,
) -> None:
    """
    Apply an SCD Type 2 change to a dimension table.

    Steps:
      1. Find the current record for the given natural key (is_current = true).
      2. Close that record: set valid_to = change_date - 1 day, is_current = false.
      3. Create a new record with the updated attribute(s), a new surrogate key,
         valid_from = change_date, valid_to = 9999-12-31, is_current = true.

    Parameters
    ----------
    con : DuckDB connection
    table : Name of the dimension table (e.g., 'dim_zones')
    natural_key_col : Column name for the natural key (e.g., 'location_id')
    natural_key_val : Value of the natural key to update
    change_date : The effective date of the change
    new_values : Dict of column_name -> new_value for the changed attributes
    """
    # -------------------------------------------------------------------------
    # Step 1: Fetch the current record
    # -------------------------------------------------------------------------
    columns_result = con.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}' AND table_schema = 'main'
        ORDER BY ordinal_position
    """).fetchall()
    all_columns = [row[0] for row in columns_result]

    current = con.execute(f"""
        SELECT {', '.join(all_columns)}
        FROM {table}
        WHERE {natural_key_col} = ? AND is_current = true
    """, [natural_key_val]).fetchone()

    if current is None:
        raise ValueError(
            f"No current record found in {table} "
            f"where {natural_key_col}={natural_key_val}"
        )

    current_dict = dict(zip(all_columns, current))

    print(f"\n  Processing change for {natural_key_col}={natural_key_val} "
          f"on {change_date}:")
    for col, new_val in new_values.items():
        old_val = current_dict[col]
        print(f"    {col}: '{old_val}' -> '{new_val}'")

    # -------------------------------------------------------------------------
    # Step 2: Close the current record
    #   Set valid_to to the day BEFORE the change takes effect.
    # -------------------------------------------------------------------------
    close_date = change_date - timedelta(days=1)

    con.execute(f"""
        UPDATE {table}
        SET valid_to = CAST(? AS DATE),
            is_current = false
        WHERE {natural_key_col} = ?
          AND is_current = true
    """, [str(close_date), natural_key_val])

    # -------------------------------------------------------------------------
    # Step 3: Generate a new surrogate key
    #   Surrogate key column is assumed to be the first column (zone_key, etc.)
    # -------------------------------------------------------------------------
    surrogate_col = all_columns[0]
    max_key = con.execute(
        f"SELECT MAX({surrogate_col}) FROM {table}"
    ).fetchone()[0]
    new_key = max_key + 1

    # -------------------------------------------------------------------------
    # Step 4: Build the new record
    # -------------------------------------------------------------------------
    new_record = dict(current_dict)
    new_record[surrogate_col] = new_key
    new_record['valid_from'] = change_date
    new_record['valid_to'] = date(9999, 12, 31)
    new_record['is_current'] = True

    for col, new_val in new_values.items():
        new_record[col] = new_val

    # -------------------------------------------------------------------------
    # Step 5: Insert the new record
    # -------------------------------------------------------------------------
    placeholders = ', '.join(['?'] * len(all_columns))
    col_list = ', '.join(all_columns)
    values = [new_record[c] for c in all_columns]

    con.execute(f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
    """, values)

    old_key = current_dict[surrogate_col]
    print(f"    -> Closed old record ({surrogate_col}={old_key})")
    print(f"    -> Created new record ({surrogate_col}={new_key})")


def verify_scd2(con: duckdb.DuckDBPyConnection) -> None:
    """
    Verify the SCD Type 2 results by running diagnostic queries.

    After processing the three zone changes, we expect:
      - original zone count + 3 new version rows
      - is_current = true count equals original zone count
      - Zone 261 has 2 rows (original + rename)
      - Zone 132 has 2 rows (original + service_zone change)
      - Zone 138 has 2 rows (original + borough change)
    """
    print("\n" + "=" * 60)
    print("SCD TYPE 2 VERIFICATION")
    print("=" * 60)

    # Total rows
    total = con.execute("SELECT COUNT(*) FROM dim_zones").fetchone()[0]
    current = con.execute(
        "SELECT COUNT(*) FROM dim_zones WHERE is_current = true"
    ).fetchone()[0]
    original_count = total - 3  # 3 changes means 3 new rows

    print(f"\n  Total rows in dim_zones: {total} "
          f"(original {original_count} + 3 new versions)")
    print(f"  Current rows (is_current=true): {current} "
          f"(expected: {original_count})")

    # History for each changed zone
    for loc_id, expected, label in [
        (261, 2, "World Trade Center -> WTC / Battery Park"),
        (132, 2, "JFK: Airports -> Major Airports"),
        (138, 2, "LaGuardia: Queens -> Airport Authority"),
    ]:
        count = con.execute(
            "SELECT COUNT(*) FROM dim_zones WHERE location_id = ?",
            [loc_id]
        ).fetchone()[0]
        print(f"  Versions for zone {loc_id} ({label}): "
              f"{count} (expected: {expected})")

    # Show full history for zone 261
    print("\n  Full history for zone 261 (World Trade Center):")
    print("  " + "-" * 80)
    rows = con.execute("""
        SELECT zone_key, zone, borough, service_zone,
               valid_from, valid_to, is_current
        FROM dim_zones
        WHERE location_id = 261
        ORDER BY valid_from
    """).fetchall()
    for row in rows:
        key, zone, borough, svc, vf, vt, ic = row
        status = "CURRENT" if ic else "EXPIRED"
        print(f"    key={key:>4}  zone='{zone}'  borough='{borough}'  "
              f"service='{svc}'  valid=[{vf} to {vt}]  {status}")

    # Show full history for zone 132
    print("\n  Full history for zone 132 (JFK Airport):")
    print("  " + "-" * 80)
    rows = con.execute("""
        SELECT zone_key, zone, service_zone,
               valid_from, valid_to, is_current
        FROM dim_zones
        WHERE location_id = 132
        ORDER BY valid_from
    """).fetchall()
    for row in rows:
        key, zone, svc, vf, vt, ic = row
        status = "CURRENT" if ic else "EXPIRED"
        print(f"    key={key:>4}  zone='{zone}'  service='{svc}'  "
              f"valid=[{vf} to {vt}]  {status}")

    # Show full history for zone 138
    print("\n  Full history for zone 138 (LaGuardia Airport):")
    print("  " + "-" * 80)
    rows = con.execute("""
        SELECT zone_key, zone, borough, service_zone,
               valid_from, valid_to, is_current
        FROM dim_zones
        WHERE location_id = 138
        ORDER BY valid_from
    """).fetchall()
    for row in rows:
        key, zone, borough, svc, vf, vt, ic = row
        status = "CURRENT" if ic else "EXPIRED"
        print(f"    key={key:>4}  zone='{zone}'  borough='{borough}'  "
              f"service='{svc}'  valid=[{vf} to {vt}]  {status}")

    # Demonstrate point-in-time lookup
    print("\n  Point-in-time lookup: What was zone 261 called on 2023-06-15?")
    result = con.execute("""
        SELECT zone, borough
        FROM dim_zones
        WHERE location_id = 261
          AND DATE '2023-06-15' BETWEEN valid_from AND valid_to
    """).fetchone()
    if result:
        print(f"    -> zone='{result[0]}', borough='{result[1]}'")

    print("  Point-in-time lookup: What was zone 261 called on 2024-01-01?")
    result = con.execute("""
        SELECT zone, borough
        FROM dim_zones
        WHERE location_id = 261
          AND DATE '2024-01-01' BETWEEN valid_from AND valid_to
    """).fetchone()
    if result:
        print(f"    -> zone='{result[0]}', borough='{result[1]}'")

    print("\n" + "=" * 60)

    # Validate expectations
    assert current == original_count, \
        f"Expected {original_count} current rows, got {current}"
    for loc_id in [261, 132, 138]:
        cnt = con.execute(
            "SELECT COUNT(*) FROM dim_zones WHERE location_id = ?",
            [loc_id]
        ).fetchone()[0]
        assert cnt == 2, \
            f"Expected 2 versions of zone {loc_id}, got {cnt}"
    print("  All assertions passed!")


def main():
    if not WAREHOUSE_PATH.exists():
        print(f"Warehouse not found at {WAREHOUSE_PATH}")
        print("Run create_warehouse.py first.")
        return

    print(f"Connecting to warehouse: {WAREHOUSE_PATH}")
    con = duckdb.connect(str(WAREHOUSE_PATH))

    try:
        # -----------------------------------------------------------------
        # Check if SCD changes have already been applied (idempotency)
        # -----------------------------------------------------------------
        zone_261_count = con.execute(
            "SELECT COUNT(*) FROM dim_zones WHERE location_id = 261"
        ).fetchone()[0]
        if zone_261_count > 1:
            print("SCD Type 2 changes appear to have been applied already.")
            print("To re-run, first rebuild the warehouse with "
                  "create_warehouse.py.")
            verify_scd2(con)
            return

        print("\nApplying SCD Type 2 changes to dim_zones...")

        # -----------------------------------------------------------------
        # Change 1: Zone 261 renames on 2023-07-01
        #   "World Trade Center" -> "World Trade Center / Battery Park"
        # -----------------------------------------------------------------
        apply_scd2_change(
            con,
            table='dim_zones',
            natural_key_col='location_id',
            natural_key_val=261,
            change_date=date(2023, 7, 1),
            new_values={
                'zone': 'World Trade Center / Battery Park',
            },
        )

        # -----------------------------------------------------------------
        # Change 2: Zone 132 (JFK) changes service_zone on 2024-01-15
        #   "Airports" -> "Major Airports"
        # -----------------------------------------------------------------
        apply_scd2_change(
            con,
            table='dim_zones',
            natural_key_col='location_id',
            natural_key_val=132,
            change_date=date(2024, 1, 15),
            new_values={
                'service_zone': 'Major Airports',
            },
        )

        # -----------------------------------------------------------------
        # Change 3: Zone 138 (LaGuardia) changes borough on 2024-06-01
        #   "Queens" -> "Airport Authority"
        # -----------------------------------------------------------------
        apply_scd2_change(
            con,
            table='dim_zones',
            natural_key_col='location_id',
            natural_key_val=138,
            change_date=date(2024, 6, 1),
            new_values={
                'borough': 'Airport Authority',
            },
        )

        # Verify the results
        verify_scd2(con)

    finally:
        con.close()


if __name__ == '__main__':
    main()
