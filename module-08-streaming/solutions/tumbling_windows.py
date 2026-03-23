#!/usr/bin/env python3
"""
Exercise 3 Solution: Tumbling Window Aggregation

Implements fixed-size, non-overlapping tumbling windows.
Counts unique listeners per episode in 5-minute windows.

Tumbling Window visualization:
  Time:    |---W1---|---W2---|---W3---|
  Events:  [1,2,3]  [4,5]    [6,7,8,9]
  Each event belongs to exactly ONE window.
"""

import json
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Queue, Empty

from stream_producer import StreamProducer


class TumblingWindow:
    """Fixed-size, non-overlapping window."""

    def __init__(self, window_size_seconds=300):
        self.window_size = timedelta(seconds=window_size_seconds)
        self.windows = {}  # window_start -> {episode_id -> set(user_ids)}
        self.emitted_windows = set()

    def get_window_start(self, event_time: datetime) -> datetime:
        """Calculate which window an event belongs to."""
        # Floor the time to the nearest window boundary
        epoch = datetime(2024, 1, 1)
        elapsed = (event_time - epoch).total_seconds()
        window_num = int(elapsed // self.window_size.total_seconds())
        return epoch + timedelta(seconds=window_num * self.window_size.total_seconds())

    def add_event(self, event):
        """Add an event to its corresponding window."""
        event_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
        window_start = self.get_window_start(event_time)

        if window_start not in self.windows:
            self.windows[window_start] = defaultdict(set)

        self.windows[window_start][event["episode_id"]].add(event["user_id"])

    def check_and_emit(self, current_time: datetime):
        """
        Check if any windows should be closed and emit results.
        A window is closed when current_time > window_start + window_size.
        """
        results = []
        for window_start in sorted(self.windows.keys()):
            window_end = window_start + self.window_size

            if current_time >= window_end and window_start not in self.emitted_windows:
                # Window is complete — emit results
                window_data = self.windows[window_start]
                result = {
                    "window_start": window_start.strftime("%Y-%m-%d %H:%M:%S"),
                    "window_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "episodes": {}
                }
                for episode_id, users in window_data.items():
                    result["episodes"][episode_id] = {
                        "unique_listeners": len(users),
                        "total_events": len(users),  # simplified
                    }

                results.append(result)
                self.emitted_windows.add(window_start)

                # Clean up old window data to free memory
                del self.windows[window_start]

        return results


def run_tumbling_window_demo(duration=20, window_size=10):
    """Run the tumbling window demo with simulated streaming."""
    print("=" * 60)
    print(f"Tumbling Window Demo (window_size={window_size}s)")
    print("=" * 60)

    q = Queue()
    window = TumblingWindow(window_size_seconds=window_size)

    # Start producer
    producer = StreamProducer(q, events_per_second=20)
    sim_start = datetime(2024, 11, 15, 21, 0, 0)
    prod_thread = threading.Thread(
        target=producer.produce,
        kwargs={"duration_seconds": duration, "simulated_time": sim_start}
    )
    prod_thread.start()

    # Process events
    start = time.time()
    events_processed = 0
    sim_time = sim_start

    while time.time() - start < duration + 5:
        try:
            event = q.get(timeout=1)
            window.add_event(event)
            events_processed += 1

            # Advance simulated clock
            event_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
            sim_time = max(sim_time, event_time)

            # Check for completed windows
            results = window.check_and_emit(sim_time)
            for result in results:
                top_episodes = sorted(
                    result["episodes"].items(),
                    key=lambda x: x[1]["unique_listeners"],
                    reverse=True
                )[:3]
                print(f"\n  Window [{result['window_start']} → {result['window_end']}]")
                print(f"    Total episodes with activity: {len(result['episodes'])}")
                for ep_id, stats in top_episodes:
                    print(f"    {ep_id}: {stats['unique_listeners']} unique listeners")

        except Empty:
            if not prod_thread.is_alive():
                break

    # Emit any remaining windows
    final_results = window.check_and_emit(sim_time + timedelta(minutes=10))
    for result in final_results:
        print(f"\n  Window [{result['window_start']} → {result['window_end']}] (final flush)")
        print(f"    Episodes: {len(result['episodes'])}")

    prod_thread.join()
    print(f"\nTotal events processed: {events_processed}")
    print(f"Windows emitted: {len(window.emitted_windows)}")


if __name__ == "__main__":
    run_tumbling_window_demo(duration=15, window_size=5)
