#!/usr/bin/env python3
"""
Exercise 6 Solution: Late Event Handling with Watermarks

Demonstrates how to handle events that arrive after their window has closed.
Uses watermarks to define the boundary between "acceptably late" and "too late".

Watermark = max_event_time - allowed_lateness
- Events with event_time >= watermark: processed normally
- Events with event_time < watermark: sent to late events sink
"""

import json
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Queue, Empty

from stream_producer import StreamProducer


class WatermarkedWindowProcessor:
    """Tumbling windows with watermark-based late event handling."""

    def __init__(self, window_size_seconds=60, allowed_lateness_seconds=30):
        self.window_size = timedelta(seconds=window_size_seconds)
        self.allowed_lateness = timedelta(seconds=allowed_lateness_seconds)
        self.max_event_time = datetime.min
        self.windows = {}  # window_start -> {episode_id -> count}
        self.emitted = set()

        # Metrics
        self.on_time_events = 0
        self.late_accepted_events = 0  # Late but within watermark
        self.late_dropped_events = 0   # Too late, beyond watermark
        self.late_events_log = []

    @property
    def watermark(self):
        """Current watermark position."""
        return self.max_event_time - self.allowed_lateness

    def get_window_start(self, event_time):
        epoch = datetime(2024, 1, 1)
        elapsed = (event_time - epoch).total_seconds()
        window_num = int(elapsed // self.window_size.total_seconds())
        return epoch + timedelta(seconds=window_num * self.window_size.total_seconds())

    def process_event(self, event):
        """Process an event with watermark checking."""
        event_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
        window_start = self.get_window_start(event_time)
        window_end = window_start + self.window_size

        # Update max event time (watermark advances)
        self.max_event_time = max(self.max_event_time, event_time)

        # Check if event is too late
        if event_time < self.watermark:
            # Event is beyond the watermark — drop it
            self.late_dropped_events += 1
            self.late_events_log.append({
                "event_id": event["event_id"],
                "event_time": event["timestamp"],
                "watermark": self.watermark.strftime("%Y-%m-%d %H:%M:%S"),
                "lag_seconds": (self.watermark - event_time).total_seconds(),
            })
            return "dropped"

        elif window_start in self.emitted:
            # Window already emitted, but event is within watermark — accept as update
            self.late_accepted_events += 1
            if window_start not in self.windows:
                self.windows[window_start] = defaultdict(int)
            self.windows[window_start][event["episode_id"]] += 1
            return "late_accepted"

        else:
            # Normal on-time event
            self.on_time_events += 1
            if window_start not in self.windows:
                self.windows[window_start] = defaultdict(int)
            self.windows[window_start][event["episode_id"]] += 1
            return "on_time"

    def emit_completed_windows(self):
        """Emit windows that are complete (watermark has passed window end)."""
        results = []
        for window_start in sorted(list(self.windows.keys())):
            window_end = window_start + self.window_size
            if self.watermark >= window_end and window_start not in self.emitted:
                result = {
                    "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "total_events": sum(self.windows[window_start].values()),
                    "unique_episodes": len(self.windows[window_start]),
                }
                results.append(result)
                self.emitted.add(window_start)
        return results


def run_late_events_demo(duration=15):
    """Demo watermark and late event handling."""
    print("=" * 60)
    print("Late Event Handling with Watermarks Demo")
    print("=" * 60)
    print(f"  Window size: 10 seconds")
    print(f"  Allowed lateness: 5 seconds")

    q = Queue()
    processor = WatermarkedWindowProcessor(
        window_size_seconds=10,
        allowed_lateness_seconds=5
    )

    # Start producer (generates some late events automatically)
    producer = StreamProducer(q, events_per_second=20)
    sim_start = datetime(2024, 11, 15, 21, 0, 0)
    prod_thread = threading.Thread(
        target=producer.produce,
        kwargs={"duration_seconds": duration, "simulated_time": sim_start}
    )
    prod_thread.start()

    # Process
    start = time.time()
    events_processed = 0

    while time.time() - start < duration + 5:
        try:
            event = q.get(timeout=1)
            status = processor.process_event(event)
            events_processed += 1

            # Log interesting cases
            if status == "dropped" and processor.late_dropped_events <= 3:
                print(f"\n  DROPPED: event at {event['timestamp']} "
                      f"(watermark at {processor.watermark.strftime('%H:%M:%S')})")
            elif status == "late_accepted" and processor.late_accepted_events <= 3:
                print(f"\n  LATE ACCEPTED: event at {event['timestamp']}")

            # Emit completed windows
            results = processor.emit_completed_windows()
            for r in results:
                print(f"\n  WINDOW COMPLETE: [{r['window_start']} → {r['window_end']}] "
                      f"events={r['total_events']}, episodes={r['unique_episodes']}")

        except Empty:
            if not prod_thread.is_alive():
                break

    prod_thread.join()

    # Summary
    print(f"\n{'=' * 50}")
    print("WATERMARK RESULTS")
    print(f"{'=' * 50}")
    print(f"  Total events:        {events_processed}")
    print(f"  On-time events:      {processor.on_time_events}")
    print(f"  Late (accepted):     {processor.late_accepted_events}")
    print(f"  Late (dropped):      {processor.late_dropped_events}")
    print(f"  Windows emitted:     {len(processor.emitted)}")

    if processor.late_events_log:
        print(f"\n  Sample dropped late events:")
        for late in processor.late_events_log[:5]:
            print(f"    Event time: {late['event_time']}, "
                  f"Watermark: {late['watermark']}, "
                  f"Lag: {late['lag_seconds']:.0f}s")


if __name__ == "__main__":
    run_late_events_demo(duration=15)
