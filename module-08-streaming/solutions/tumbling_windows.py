#!/usr/bin/env python3
"""
Exercise 3 Solution: Tumbling Window Aggregation

Implements fixed-size, non-overlapping 5-minute tumbling windows.
Per window: count trips, sum fares, compute average distance.

Tumbling Window visualization:
  Time:    |---W1---|---W2---|---W3---|
  Trips:   [t1,t2]  [t3,t4,t5] [t6]
  Each trip belongs to exactly ONE window.

On Databricks this would be:
  spark.readStream
    .groupBy(window("pickup_datetime", "5 minutes"))
    .agg(count("*"), sum("fare_amount"), avg("trip_distance"))
"""

import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Queue, Empty

from stream_producer import StreamProducer


class TumblingWindow:
    """Fixed-size, non-overlapping window for taxi trip aggregation."""

    def __init__(self, window_size_seconds=300):
        self.window_size = timedelta(seconds=window_size_seconds)
        # window_start -> {trip_count, fare_sum, distance_sum, tip_sum}
        self.windows = {}
        self.emitted_windows = set()

    def get_window_start(self, event_time: datetime) -> datetime:
        """Calculate which window a trip belongs to."""
        epoch = datetime(2023, 1, 1)
        elapsed = (event_time - epoch).total_seconds()
        window_num = int(elapsed // self.window_size.total_seconds())
        return epoch + timedelta(seconds=window_num * self.window_size.total_seconds())

    def add_event(self, event):
        """Add a trip event to its corresponding window."""
        event_time = datetime.strptime(event["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        window_start = self.get_window_start(event_time)

        if window_start not in self.windows:
            self.windows[window_start] = {
                "trip_count": 0,
                "fare_sum": 0.0,
                "distance_sum": 0.0,
                "tip_sum": 0.0,
                "total_amount_sum": 0.0,
                "passenger_sum": 0,
                "zones": defaultdict(int),
            }

        w = self.windows[window_start]
        w["trip_count"] += 1
        w["fare_sum"] += event.get("fare_amount", 0)
        w["distance_sum"] += event.get("trip_distance", 0)
        w["tip_sum"] += event.get("tip_amount", 0)
        w["total_amount_sum"] += event.get("total_amount", 0)
        w["passenger_sum"] += event.get("passenger_count", 0)
        w["zones"][event.get("pu_location_id", 0)] += 1

    def check_and_emit(self, current_time: datetime):
        """
        Check if any windows should be closed and emit results.
        A window is closed when current_time > window_start + window_size.
        """
        results = []
        for window_start in sorted(self.windows.keys()):
            window_end = window_start + self.window_size

            if current_time >= window_end and window_start not in self.emitted_windows:
                w = self.windows[window_start]
                trip_count = w["trip_count"]
                avg_fare = w["fare_sum"] / trip_count if trip_count > 0 else 0
                avg_dist = w["distance_sum"] / trip_count if trip_count > 0 else 0
                top_zones = sorted(w["zones"].items(), key=lambda x: x[1], reverse=True)[:3]

                result = {
                    "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "trip_count": trip_count,
                    "total_fare": round(w["fare_sum"], 2),
                    "total_revenue": round(w["total_amount_sum"], 2),
                    "avg_fare": round(avg_fare, 2),
                    "avg_distance": round(avg_dist, 2),
                    "total_tips": round(w["tip_sum"], 2),
                    "top_zones": top_zones,
                }

                results.append(result)
                self.emitted_windows.add(window_start)
                del self.windows[window_start]

        return results


def run_tumbling_window_demo(duration=20, window_size=10):
    """Run the tumbling window demo with simulated taxi trip streaming."""
    print("=" * 60)
    print(f"Tumbling Window Demo (window_size={window_size}s)")
    print("=" * 60)

    q = Queue()
    window = TumblingWindow(window_size_seconds=window_size)

    producer = StreamProducer(q, events_per_second=20)
    sim_start = datetime(2023, 6, 15, 17, 0, 0)
    prod_thread = threading.Thread(
        target=producer.produce,
        kwargs={"duration_seconds": duration, "simulated_time": sim_start},
    )
    prod_thread.start()

    start = time.time()
    events_processed = 0
    sim_time = sim_start

    while time.time() - start < duration + 5:
        try:
            event = q.get(timeout=1)
            window.add_event(event)
            events_processed += 1

            event_time = datetime.strptime(event["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
            sim_time = max(sim_time, event_time)

            results = window.check_and_emit(sim_time)
            for result in results:
                print(f"\n  Window [{result['window_start']} -> {result['window_end']}]")
                print(f"    Trips: {result['trip_count']:,}  |  "
                      f"Revenue: ${result['total_revenue']:,.2f}  |  "
                      f"Avg fare: ${result['avg_fare']:.2f}  |  "
                      f"Avg dist: {result['avg_distance']:.1f}mi")
                if result["top_zones"]:
                    zones_str = ", ".join(f"zone {z}: {c}" for z, c in result["top_zones"])
                    print(f"    Top pickup zones: {zones_str}")

        except Empty:
            if not prod_thread.is_alive():
                break

    # Emit remaining windows
    final_results = window.check_and_emit(sim_time + timedelta(minutes=10))
    for result in final_results:
        print(f"\n  Window [{result['window_start']} -> {result['window_end']}] (final flush)")
        print(f"    Trips: {result['trip_count']:,}  |  Revenue: ${result['total_revenue']:,.2f}")

    prod_thread.join()
    print(f"\nTotal trips processed: {events_processed}")
    print(f"Windows emitted: {len(window.emitted_windows)}")


if __name__ == "__main__":
    run_tumbling_window_demo(duration=15, window_size=5)
