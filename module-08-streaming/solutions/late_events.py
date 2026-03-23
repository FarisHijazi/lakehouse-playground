#!/usr/bin/env python3
"""
Exercise 5 Solution: Late Event Handling with Watermarks

Demonstrates how to handle taxi trips that arrive after their window has closed.
Real-world cause: delayed meter uploads, connectivity issues, batch uploads from
taxi meters that were offline.

Watermark = max_event_time - allowed_lateness
- Trips with event_time >= watermark: processed normally
- Trips with event_time < watermark: sent to late trips sink

On Databricks this maps directly to:
  spark.readStream
    .withWatermark("pickup_datetime", "10 minutes")
    .groupBy(window("pickup_datetime", "5 minutes"))
    .agg(count("*"), sum("fare_amount"))
"""

import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Queue, Empty

from stream_producer import StreamProducer


class WatermarkedWindowProcessor:
    """Tumbling windows with watermark-based late trip handling."""

    def __init__(self, window_size_seconds=60, allowed_lateness_seconds=30):
        self.window_size = timedelta(seconds=window_size_seconds)
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.max_event_time = datetime.min
        self.windows = {}  # window_start -> {trip_count, fare_sum, distance_sum}
        self.emitted = set()

        # Metrics
        self.on_time_events = 0
        self.late_accepted_events = 0
        self.late_dropped_events = 0
        self.late_events_log = []

    @property
    def watermark(self):
        """Current watermark position."""
        return self.max_event_time - self.allowed_lateness

    def get_window_start(self, event_time):
        epoch = datetime(2023, 1, 1)
        elapsed = (event_time - epoch).total_seconds()
        window_num = int(elapsed // self.window_size.total_seconds())
        return epoch + timedelta(seconds=window_num * self.window_size.total_seconds())

    def process_event(self, event):
        """Process a trip event with watermark checking."""
        event_time = datetime.strptime(event["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        window_start = self.get_window_start(event_time)

        # Update max event time (watermark advances)
        self.max_event_time = max(self.max_event_time, event_time)

        # Check if trip is too late
        if event_time < self.watermark:
            self.late_dropped_events += 1
            self.late_events_log.append({
                "trip_id": event.get("trip_id", "unknown"),
                "event_time": event["pickup_datetime"],
                "watermark": self.watermark.strftime("%Y-%m-%d %H:%M:%S"),
                "lag_seconds": (self.watermark - event_time).total_seconds(),
                "fare_amount": event.get("fare_amount", 0),
            })
            return "dropped"

        elif window_start in self.emitted:
            # Window already emitted, but trip is within watermark -- accept as update
            self.late_accepted_events += 1
            self._add_to_window(window_start, event)
            return "late_accepted"

        else:
            # Normal on-time trip
            self.on_time_events += 1
            self._add_to_window(window_start, event)
            return "on_time"

    def _add_to_window(self, window_start, event):
        if window_start not in self.windows:
            self.windows[window_start] = {
                "trip_count": 0,
                "fare_sum": 0.0,
                "distance_sum": 0.0,
                "tip_sum": 0.0,
            }
        w = self.windows[window_start]
        w["trip_count"] += 1
        w["fare_sum"] += event.get("fare_amount", 0)
        w["distance_sum"] += event.get("trip_distance", 0)
        w["tip_sum"] += event.get("tip_amount", 0)

    def emit_completed_windows(self):
        """Emit windows that are complete (watermark has passed window end)."""
        results = []
        for window_start in sorted(list(self.windows.keys())):
            window_end = window_start + self.window_size
            if self.watermark >= window_end and window_start not in self.emitted:
                w = self.windows[window_start]
                result = {
                    "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "trip_count": w["trip_count"],
                    "total_fare": round(w["fare_sum"], 2),
                    "avg_distance": round(
                        w["distance_sum"] / w["trip_count"], 2
                    ) if w["trip_count"] > 0 else 0,
                }
                results.append(result)
                self.emitted.add(window_start)
        return results


def run_late_events_demo(duration=15):
    """Demo watermark and late taxi trip handling."""
    print("=" * 60)
    print("Late Trip Handling with Watermarks Demo")
    print("=" * 60)
    print("  Window size: 10 seconds")
    print("  Allowed lateness: 5 seconds")

    q = Queue()
    processor = WatermarkedWindowProcessor(
        window_size_seconds=10,
        allowed_lateness_seconds=5,
    )

    producer = StreamProducer(q, events_per_second=20)
    sim_start = datetime(2023, 6, 15, 17, 0, 0)
    prod_thread = threading.Thread(
        target=producer.produce,
        kwargs={"duration_seconds": duration, "simulated_time": sim_start},
    )
    prod_thread.start()

    start = time.time()
    events_processed = 0

    while time.time() - start < duration + 5:
        try:
            event = q.get(timeout=1)
            status = processor.process_event(event)
            events_processed += 1

            if status == "dropped" and processor.late_dropped_events <= 3:
                print(
                    f"\n  DROPPED: trip at {event['pickup_datetime']} "
                    f"(watermark at {processor.watermark.strftime('%H:%M:%S')}) "
                    f"fare=${event.get('fare_amount', 0):.2f}"
                )
            elif status == "late_accepted" and processor.late_accepted_events <= 3:
                print(f"\n  LATE ACCEPTED: trip at {event['pickup_datetime']}")

            results = processor.emit_completed_windows()
            for r in results:
                print(
                    f"\n  WINDOW COMPLETE: [{r['window_start']} -> {r['window_end']}] "
                    f"trips={r['trip_count']}, fare=${r['total_fare']:,.2f}, "
                    f"avg_dist={r['avg_distance']:.1f}mi"
                )

        except Empty:
            if not prod_thread.is_alive():
                break

    prod_thread.join()

    print(f"\n{'=' * 50}")
    print("WATERMARK RESULTS")
    print(f"{'=' * 50}")
    print(f"  Total events:        {events_processed}")
    print(f"  On-time trips:       {processor.on_time_events}")
    print(f"  Late (accepted):     {processor.late_accepted_events}")
    print(f"  Late (dropped):      {processor.late_dropped_events}")
    print(f"  Windows emitted:     {len(processor.emitted)}")

    if processor.late_events_log:
        dropped_fare = sum(e["fare_amount"] for e in processor.late_events_log)
        print(f"\n  Revenue in dropped trips: ${dropped_fare:,.2f}")
        print(f"\n  Sample dropped late trips:")
        for late in processor.late_events_log[:5]:
            print(
                f"    Trip time: {late['event_time']}, "
                f"Watermark: {late['watermark']}, "
                f"Lag: {late['lag_seconds']:.0f}s, "
                f"Fare: ${late['fare_amount']:.2f}"
            )


if __name__ == "__main__":
    run_late_events_demo(duration=15)
