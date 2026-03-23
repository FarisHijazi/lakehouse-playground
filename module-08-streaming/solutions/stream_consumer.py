#!/usr/bin/env python3
"""
Exercise 2 Solution: Simple Stream Consumer

Reads events from a queue and computes running aggregates:
- Total events processed
- Events per event type
- Running average listen duration
- Prints stats every N seconds
"""

import json
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue, Empty

from stream_producer import StreamProducer


class SimpleConsumer:
    """Basic stream consumer with running aggregations."""

    def __init__(self, input_queue: Queue, stats_interval=5):
        self.queue = input_queue
        self.stats_interval = stats_interval
        self.total_events = 0
        self.events_by_type = defaultdict(int)
        self.total_listen_seconds = 0
        self.listen_count = 0
        self.events_by_platform = defaultdict(int)
        self.unique_users = set()
        self._stop = threading.Event()
        self._last_stats = time.time()

    def consume(self, timeout=30):
        """Consume events until timeout or stop signal."""
        start = time.time()
        print(f"[Consumer] Starting. Stats every {self.stats_interval}s")

        while time.time() - start < timeout and not self._stop.is_set():
            try:
                event = self.queue.get(timeout=1)
                self._process(event)
            except Empty:
                pass

            # Print stats periodically
            if time.time() - self._last_stats >= self.stats_interval:
                self._print_stats()
                self._last_stats = time.time()

        # Final stats
        print("\n" + "=" * 50)
        print("FINAL STATS")
        self._print_stats()

    def _process(self, event):
        """Process a single event - update running aggregates."""
        self.total_events += 1
        self.events_by_type[event["event_type"]] += 1
        self.events_by_platform[event.get("platform", "unknown")] += 1
        self.unique_users.add(event["user_id"])

        if event.get("listened_seconds") is not None:
            self.total_listen_seconds += event["listened_seconds"]
            self.listen_count += 1

    def _print_stats(self):
        """Print current aggregation stats."""
        avg_listen = (self.total_listen_seconds / self.listen_count) if self.listen_count > 0 else 0

        print(f"\n--- Stats at {datetime.now().strftime('%H:%M:%S')} ---")
        print(f"  Total events:     {self.total_events}")
        print(f"  Unique users:     {len(self.unique_users)}")
        print(f"  Avg listen (sec): {avg_listen:.1f}")
        print(f"  By type: {dict(self.events_by_type)}")
        print(f"  By platform: {dict(self.events_by_platform)}")

    def stop(self):
        self._stop.set()


# ---- Standalone demo ----
if __name__ == "__main__":
    print("=" * 60)
    print("Stream Consumer Demo")
    print("=" * 60)

    q = Queue()

    # Start producer in a thread
    producer = StreamProducer(q, events_per_second=15)
    prod_thread = threading.Thread(
        target=producer.produce,
        kwargs={"duration_seconds": 20, "simulated_time": datetime(2024, 11, 15, 21, 0, 0)}
    )
    prod_thread.start()

    # Start consumer
    consumer = SimpleConsumer(q, stats_interval=5)
    consumer.consume(timeout=25)

    prod_thread.join()
    print("\nDone!")
