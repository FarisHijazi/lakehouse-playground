#!/usr/bin/env python3
"""
Exercise 1 Solution: Streaming Event Producer

Simulates a real-time stream of podcast listening events with realistic patterns:
- Peak hours in Saudi Arabia (9-11 PM AST)
- Random late-arriving events
- Configurable event rate
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue
from threading import Event as ThreadEvent

# Load reference data for realistic events
DATA_DIR = Path(__file__).parent.parent.parent / "data" / "raw"


def load_reference_data():
    """Load episode and user IDs for realistic event generation."""
    with open(DATA_DIR / "episodes.json") as f:
        episodes = json.load(f)
    episode_ids = [e["episode_id"] for e in episodes]
    episode_durations = {e["episode_id"]: e["duration_seconds"] for e in episodes}

    # Read a sample of user IDs from the CSV
    user_ids = []
    with open(DATA_DIR / "users.csv") as f:
        next(f)  # skip header
        for i, line in enumerate(f):
            if i >= 1000:
                break
            user_ids.append(line.split(",")[0])

    return user_ids, episode_ids, episode_durations


def generate_event(user_ids, episode_ids, episode_durations, current_time=None):
    """Generate a single realistic listening event."""
    if current_time is None:
        current_time = datetime.now()

    user_id = random.choice(user_ids)
    episode_id = random.choice(episode_ids)
    duration = episode_durations.get(episode_id, 2700)
    completion_rate = random.betavariate(2, 3)

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "episode_id": episode_id,
        "event_type": random.choices(
            ["play", "pause", "resume", "complete", "skip", "seek"],
            weights=[40, 15, 10, 20, 10, 5],
            k=1
        )[0],
        "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        "listened_seconds": int(duration * completion_rate),
        "platform": random.choice(["ios", "android", "web", "smart_speaker", "car_play"]),
        "country": random.choice(["SA", "AE", "KW", "BH", "QA", "EG", "JO"]),
        "app_version": f"{random.randint(2,5)}.{random.randint(0,15)}.{random.randint(0,30)}",
    }

    # 5% chance of late event (timestamp is in the past)
    if random.random() < 0.05:
        lag_seconds = random.randint(60, 7200)  # 1 min to 2 hours late
        event["timestamp"] = (current_time - timedelta(seconds=lag_seconds)).strftime("%Y-%m-%d %H:%M:%S")
        event["_late"] = True

    return event


def get_events_per_second(hour):
    """Return realistic event rate based on time of day (Saudi Arabia patterns)."""
    # Peak: 9-11 PM (21-23), secondary peak: 8-10 AM, low: 2-6 AM
    hourly_rates = {
        0: 5, 1: 3, 2: 2, 3: 1, 4: 1, 5: 2, 6: 3, 7: 5,
        8: 10, 9: 12, 10: 10, 11: 8, 12: 6, 13: 6, 14: 7,
        15: 8, 16: 9, 17: 10, 18: 12, 19: 15, 20: 18,
        21: 20, 22: 18, 23: 12,
    }
    return hourly_rates.get(hour, 5)


class StreamProducer:
    """Produces events into a queue, simulating a Kafka topic."""

    def __init__(self, output_queue: Queue, events_per_second: int = None):
        self.queue = output_queue
        self.fixed_rate = events_per_second
        self.user_ids, self.episode_ids, self.episode_durations = load_reference_data()
        self.events_produced = 0
        self._stop = ThreadEvent()

    def produce(self, duration_seconds=60, simulated_time=None):
        """
        Produce events for a given duration.

        Args:
            duration_seconds: How long to produce events (real time)
            simulated_time: If set, use this as the event timestamp base
        """
        start_time = time.time()
        current_time = simulated_time or datetime.now()

        print(f"[Producer] Starting. Rate: {self.fixed_rate or 'dynamic'} events/sec")

        while time.time() - start_time < duration_seconds and not self._stop.is_set():
            # Determine rate
            rate = self.fixed_rate or get_events_per_second(current_time.hour)

            # Generate a batch of events
            for _ in range(rate):
                event = generate_event(
                    self.user_ids, self.episode_ids,
                    self.episode_durations, current_time
                )
                self.queue.put(event)
                self.events_produced += 1

            # Advance simulated time by 1 second
            current_time += timedelta(seconds=1)
            time.sleep(1.0 / max(rate, 1))  # Real-time pacing

        print(f"[Producer] Stopped. Total events produced: {self.events_produced}")

    def stop(self):
        self._stop.set()


# ---- Standalone execution ----
if __name__ == "__main__":
    print("=" * 60)
    print("Streaming Event Producer Demo")
    print("=" * 60)

    q = Queue()
    producer = StreamProducer(q, events_per_second=10)

    print("\nProducing events for 10 seconds at 10 events/sec...")
    producer.produce(duration_seconds=10, simulated_time=datetime(2024, 11, 15, 21, 0, 0))

    # Drain and show some events
    print(f"\nTotal in queue: {q.qsize()}")
    print("\nSample events:")
    for i in range(min(5, q.qsize())):
        event = q.get()
        print(f"  {event['event_type']:10s} | user={event['user_id']} | ep={event['episode_id']} | {event['timestamp']}")
        if event.get("_late"):
            print(f"             ^^^ LATE EVENT")
