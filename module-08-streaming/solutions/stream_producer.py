#!/usr/bin/env python3
"""
Exercise 1 Solution: Streaming Trip Producer

Reads NYC yellow taxi parquet files and simulates real-time trip arrivals:
- Peak hours during NYC rush (7-9 AM, 5-7 PM)
- Random late-arriving events (delayed meter uploads)
- Configurable trip rate
"""

import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue
from threading import Event as ThreadEvent

import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"


def load_trip_data(max_rows=50_000):
    """Load a sample of yellow taxi trips from parquet files."""
    parquet_files = sorted(DATA_DIR.glob("yellow_tripdata_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No yellow_tripdata_*.parquet files in {DATA_DIR}")

    # Read from the first available file
    table = pq.read_table(parquet_files[0])
    df = table.to_pandas()

    # Sample if too large
    if len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=42).reset_index(drop=True)

    # Normalize column names to lowercase
    df.columns = [c.lower() for c in df.columns]

    return df


def load_zone_lookup():
    """Load taxi zone lookup for zone-to-borough mapping."""
    zone_file = DATA_DIR / "taxi_zone_lookup.csv"
    if not zone_file.exists():
        return {}
    import csv
    zones = {}
    with open(zone_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            loc_id = int(row["LocationID"])
            zones[loc_id] = {
                "borough": row["Borough"],
                "zone": row["Zone"],
                "service_zone": row.get("service_zone", ""),
            }
    return zones


def generate_trip_event(trip_row, current_time=None, zones=None):
    """Convert a parquet row into a streaming trip event dict."""
    if current_time is None:
        current_time = datetime.now()

    pu_location = int(trip_row.get("pulocationid", 0)) if trip_row.get("pulocationid") else 0
    do_location = int(trip_row.get("dolocationid", 0)) if trip_row.get("dolocationid") else 0

    event = {
        "trip_id": str(uuid.uuid4()),
        "vendor_id": int(trip_row.get("vendorid", 1)) if trip_row.get("vendorid") else 1,
        "pickup_datetime": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_datetime": (current_time + timedelta(
            minutes=max(1, float(trip_row.get("trip_distance", 3)) * 3 + random.uniform(-2, 5))
        )).strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_count": int(trip_row.get("passenger_count", 1)) if trip_row.get("passenger_count") else 1,
        "trip_distance": round(float(trip_row.get("trip_distance", 0)), 2),
        "pu_location_id": pu_location,
        "do_location_id": do_location,
        "rate_code_id": int(trip_row.get("ratecodeid", 1)) if trip_row.get("ratecodeid") else 1,
        "payment_type": int(trip_row.get("payment_type", 1)) if trip_row.get("payment_type") else 1,
        "fare_amount": round(float(trip_row.get("fare_amount", 0)), 2),
        "extra": round(float(trip_row.get("extra", 0)), 2),
        "mta_tax": round(float(trip_row.get("mta_tax", 0)), 2),
        "tip_amount": round(float(trip_row.get("tip_amount", 0)), 2),
        "tolls_amount": round(float(trip_row.get("tolls_amount", 0)), 2),
        "total_amount": round(float(trip_row.get("total_amount", 0)), 2),
    }

    # Add borough info if zones available
    if zones:
        pu_info = zones.get(pu_location, {})
        do_info = zones.get(do_location, {})
        event["pu_borough"] = pu_info.get("borough", "Unknown")
        event["do_borough"] = do_info.get("borough", "Unknown")

    # 5% chance of late event (delayed meter upload -- timestamp is in the past)
    if random.random() < 0.05:
        lag_seconds = random.randint(120, 7200)  # 2 min to 2 hours late
        event["pickup_datetime"] = (current_time - timedelta(seconds=lag_seconds)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        event["_late"] = True

    return event


def get_trips_per_second(hour):
    """Return realistic trip rate based on time of day (NYC patterns)."""
    # Peak: 7-9 AM, 5-7 PM; low: 2-5 AM
    hourly_rates = {
        0: 8, 1: 5, 2: 3, 3: 2, 4: 2, 5: 3, 6: 8, 7: 20,
        8: 25, 9: 22, 10: 18, 11: 18, 12: 20, 13: 20, 14: 18,
        15: 18, 16: 22, 17: 28, 18: 30, 19: 25, 20: 20,
        21: 18, 22: 15, 23: 10,
    }
    return hourly_rates.get(hour, 10)


class StreamProducer:
    """Produces taxi trip events into a queue, simulating a Kafka topic."""

    def __init__(self, output_queue: Queue, events_per_second: int = None):
        self.queue = output_queue
        self.fixed_rate = events_per_second
        self.trip_data = load_trip_data()
        self.zones = load_zone_lookup()
        self.events_produced = 0
        self._stop = ThreadEvent()
        self._trip_index = 0

    def _next_trip_row(self):
        """Get the next trip row, cycling through the loaded data."""
        row = self.trip_data.iloc[self._trip_index % len(self.trip_data)]
        self._trip_index += 1
        return row

    def produce(self, duration_seconds=60, simulated_time=None):
        """
        Produce trip events for a given duration.

        Args:
            duration_seconds: How long to produce events (real time)
            simulated_time: If set, use this as the event timestamp base
        """
        start_time = time.time()
        current_time = simulated_time or datetime.now()

        print(f"[Producer] Starting. Rate: {self.fixed_rate or 'dynamic'} trips/sec")

        while time.time() - start_time < duration_seconds and not self._stop.is_set():
            rate = self.fixed_rate or get_trips_per_second(current_time.hour)

            for _ in range(rate):
                trip_row = self._next_trip_row()
                event = generate_trip_event(trip_row, current_time, self.zones)
                self.queue.put(event)
                self.events_produced += 1

            current_time += timedelta(seconds=1)
            time.sleep(1.0 / max(rate, 1))

        print(f"[Producer] Stopped. Total trips produced: {self.events_produced}")

    def stop(self):
        self._stop.set()


# ---- Standalone execution ----
if __name__ == "__main__":
    print("=" * 60)
    print("Streaming Trip Producer Demo")
    print("=" * 60)

    q = Queue()
    producer = StreamProducer(q, events_per_second=10)

    print("\nProducing trip events for 10 seconds at 10 trips/sec...")
    producer.produce(duration_seconds=10, simulated_time=datetime(2023, 6, 15, 17, 0, 0))

    print(f"\nTotal in queue: {q.qsize()}")
    print("\nSample trips:")
    for i in range(min(5, q.qsize())):
        event = q.get()
        late_marker = " [LATE]" if event.get("_late") else ""
        print(
            f"  ${event['fare_amount']:>7.2f} | "
            f"dist={event['trip_distance']:>5.1f}mi | "
            f"zone {event['pu_location_id']}->{event['do_location_id']} | "
            f"{event['pickup_datetime']}{late_marker}"
        )
