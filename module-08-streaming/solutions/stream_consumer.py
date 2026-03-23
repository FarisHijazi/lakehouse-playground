#!/usr/bin/env python3
"""
Exercise 2 Solution: Simple Stream Consumer

Reads taxi trip events from a queue and computes running aggregates:
- Total trips processed
- Trips by payment type and rate code
- Running average fare amount and trip distance
- Prints stats every N seconds

In production on Databricks, this would be a Structured Streaming job
reading from a Delta table or Kafka topic.
"""

import time
import threading
from collections import defaultdict
from datetime import datetime
from queue import Queue, Empty

from stream_producer import StreamProducer


PAYMENT_TYPES = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown"}


class SimpleConsumer:
    """Basic stream consumer with running aggregations for taxi trips."""

    def __init__(self, input_queue: Queue, stats_interval=5):
        self.queue = input_queue
        self.stats_interval = stats_interval
        self.total_trips = 0
        self.trips_by_payment = defaultdict(int)
        self.trips_by_rate_code = defaultdict(int)
        self.total_fare = 0.0
        self.total_tip = 0.0
        self.total_distance = 0.0
        self.trips_by_borough = defaultdict(int)
        self._stop = threading.Event()
        self._last_stats = time.time()

    def consume(self, timeout=30):
        """Consume trip events until timeout or stop signal."""
        start = time.time()
        print(f"[Consumer] Starting. Stats every {self.stats_interval}s")

        while time.time() - start < timeout and not self._stop.is_set():
            try:
                event = self.queue.get(timeout=1)
                self._process(event)
            except Empty:
                pass

            if time.time() - self._last_stats >= self.stats_interval:
                self._print_stats()
                self._last_stats = time.time()

        print("\n" + "=" * 50)
        print("FINAL STATS")
        self._print_stats()

    def _process(self, event):
        """Process a single trip event -- update running aggregates."""
        self.total_trips += 1
        self.trips_by_payment[event.get("payment_type", 0)] += 1
        self.trips_by_rate_code[event.get("rate_code_id", 0)] += 1
        self.total_fare += event.get("fare_amount", 0)
        self.total_tip += event.get("tip_amount", 0)
        self.total_distance += event.get("trip_distance", 0)
        if event.get("pu_borough"):
            self.trips_by_borough[event["pu_borough"]] += 1

    def _print_stats(self):
        """Print current aggregation stats."""
        avg_fare = (self.total_fare / self.total_trips) if self.total_trips > 0 else 0
        avg_dist = (self.total_distance / self.total_trips) if self.total_trips > 0 else 0
        avg_tip = (self.total_tip / self.total_trips) if self.total_trips > 0 else 0

        print(f"\n--- Stats at {datetime.now().strftime('%H:%M:%S')} ---")
        print(f"  Total trips:       {self.total_trips:,}")
        print(f"  Total revenue:     ${self.total_fare:,.2f}")
        print(f"  Avg fare:          ${avg_fare:.2f}")
        print(f"  Avg distance:      {avg_dist:.2f} mi")
        print(f"  Avg tip:           ${avg_tip:.2f}")
        print(f"  By payment type:")
        for ptype, count in sorted(self.trips_by_payment.items()):
            label = PAYMENT_TYPES.get(ptype, f"Type {ptype}")
            print(f"    {label:15s}: {count:,}")
        if self.trips_by_borough:
            print(f"  By borough:")
            for borough, count in sorted(self.trips_by_borough.items(),
                                         key=lambda x: x[1], reverse=True)[:5]:
                print(f"    {borough:20s}: {count:,}")

    def stop(self):
        self._stop.set()


# ---- Standalone demo ----
if __name__ == "__main__":
    print("=" * 60)
    print("Stream Consumer Demo")
    print("=" * 60)

    q = Queue()

    producer = StreamProducer(q, events_per_second=15)
    prod_thread = threading.Thread(
        target=producer.produce,
        kwargs={"duration_seconds": 20, "simulated_time": datetime(2023, 6, 15, 17, 0, 0)},
    )
    prod_thread.start()

    consumer = SimpleConsumer(q, stats_interval=5)
    consumer.consume(timeout=25)

    prod_thread.join()
    print("\nDone!")
