#!/usr/bin/env python3
"""
Exercise 4 Solution: Session Windows

Groups trips by pickup zone into "busy periods" based on activity gaps.
A busy period ends after N minutes of no pickups in that zone.

Session Window visualization:
  Zone 132: [trip...trip...trip.....gap.....trip...trip]
            |---- busy period 1 ----|      |-- period 2 --|

This is a per-key (per-zone) windowing strategy.
"""

import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Queue, Empty

from stream_producer import StreamProducer


class ZoneBusyPeriod:
    """Represents a single busy period for a pickup zone."""

    def __init__(self, zone_id, first_event):
        self.zone_id = zone_id
        self.start_time = datetime.strptime(first_event["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        self.last_activity = self.start_time
        self.events = [first_event]
        self.total_fare = first_event.get("fare_amount", 0)
        self.total_tips = first_event.get("tip_amount", 0)
        self.total_distance = first_event.get("trip_distance", 0)
        self.total_passengers = first_event.get("passenger_count", 0)

    def add_event(self, event):
        self.events.append(event)
        event_time = datetime.strptime(event["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
        self.last_activity = max(self.last_activity, event_time)
        self.total_fare += event.get("fare_amount", 0)
        self.total_tips += event.get("tip_amount", 0)
        self.total_distance += event.get("trip_distance", 0)
        self.total_passengers += event.get("passenger_count", 0)

    @property
    def duration_seconds(self):
        return (self.last_activity - self.start_time).total_seconds()

    @property
    def trips_per_minute(self):
        mins = max(self.duration_seconds / 60, 1)
        return len(self.events) / mins

    @property
    def is_surge(self):
        """A surge period has more than 5 trips per minute."""
        return self.trips_per_minute > 5

    def to_dict(self):
        return {
            "zone_id": self.zone_id,
            "period_start": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "period_end": self.last_activity.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": self.duration_seconds,
            "trip_count": len(self.events),
            "trips_per_minute": round(self.trips_per_minute, 1),
            "total_fare": round(self.total_fare, 2),
            "total_tips": round(self.total_tips, 2),
            "avg_fare": round(self.total_fare / len(self.events), 2),
            "total_passengers": self.total_passengers,
            "is_surge": self.is_surge,
        }


class SessionWindowProcessor:
    """Manages session windows (busy periods) per pickup zone."""

    def __init__(self, gap_seconds=300):
        """
        Args:
            gap_seconds: Inactivity gap that closes a busy period (default: 5 minutes)
        """
        self.gap = timedelta(seconds=gap_seconds)
        self.active_sessions = {}  # zone_id -> ZoneBusyPeriod
        self.completed_sessions = []

    def process_event(self, event):
        """Process a single trip event, assigning it to a zone busy period."""
        zone_id = event.get("pu_location_id", 0)
        event_time = datetime.strptime(event["pickup_datetime"], "%Y-%m-%d %H:%M:%S")

        if zone_id in self.active_sessions:
            session = self.active_sessions[zone_id]

            if event_time - session.last_activity <= self.gap:
                session.add_event(event)
            else:
                # Gap exceeded -- close old period, start new one
                self.completed_sessions.append(session)
                self.active_sessions[zone_id] = ZoneBusyPeriod(zone_id, event)
        else:
            self.active_sessions[zone_id] = ZoneBusyPeriod(zone_id, event)

    def check_expired(self, current_time: datetime):
        """Check for busy periods that have expired (no activity within gap)."""
        expired = []
        for zone_id, session in list(self.active_sessions.items()):
            if current_time - session.last_activity > self.gap:
                self.completed_sessions.append(session)
                expired.append(zone_id)
                del self.active_sessions[zone_id]
        return expired

    def flush_all(self):
        """Close all active sessions (end of stream)."""
        for session in self.active_sessions.values():
            self.completed_sessions.append(session)
        self.active_sessions.clear()

    def get_stats(self):
        """Get summary statistics about completed busy periods."""
        if not self.completed_sessions:
            return {"total_periods": 0}

        durations = [s.duration_seconds for s in self.completed_sessions]
        fares = [s.total_fare for s in self.completed_sessions]
        return {
            "total_periods": len(self.completed_sessions),
            "active_periods": len(self.active_sessions),
            "avg_duration_sec": sum(durations) / len(durations),
            "max_duration_sec": max(durations),
            "surge_periods": sum(1 for s in self.completed_sessions if s.is_surge),
            "avg_trips_per_period": sum(len(s.events) for s in self.completed_sessions) / len(
                self.completed_sessions
            ),
            "total_revenue": sum(fares),
        }


def run_session_window_demo(duration=20):
    """Run the session window demo for taxi zones."""
    print("=" * 60)
    print("Session Window Demo -- Zone Busy Periods (gap=30s for demo)")
    print("=" * 60)

    q = Queue()
    processor = SessionWindowProcessor(gap_seconds=30)  # 30s gap for demo speed

    producer = StreamProducer(q, events_per_second=25)
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
            processor.process_event(event)
            events_processed += 1

            event_time = datetime.strptime(event["pickup_datetime"], "%Y-%m-%d %H:%M:%S")
            sim_time = max(sim_time, event_time)

            if events_processed % 50 == 0:
                expired = processor.check_expired(sim_time)
                if expired:
                    print(f"\n  Expired {len(expired)} zone busy periods at {sim_time}")

        except Empty:
            if not prod_thread.is_alive():
                break

    processor.flush_all()
    prod_thread.join()

    stats = processor.get_stats()
    print(f"\n{'=' * 50}")
    print("SESSION WINDOW RESULTS -- Zone Busy Periods")
    print(f"{'=' * 50}")
    print(f"  Trips processed:        {events_processed}")
    print(f"  Total busy periods:     {stats['total_periods']}")
    print(f"  Avg period duration:    {stats.get('avg_duration_sec', 0):.1f}s")
    print(f"  Max period duration:    {stats.get('max_duration_sec', 0):.1f}s")
    print(f"  Surge periods:          {stats.get('surge_periods', 0)}")
    print(f"  Avg trips per period:   {stats.get('avg_trips_per_period', 0):.1f}")
    print(f"  Total revenue:          ${stats.get('total_revenue', 0):,.2f}")

    # Show busiest zone periods
    print(f"\nBusiest zone periods (by trip count):")
    sorted_sessions = sorted(
        processor.completed_sessions, key=lambda s: len(s.events), reverse=True
    )
    for session in sorted_sessions[:8]:
        s = session.to_dict()
        surge = " [SURGE]" if s["is_surge"] else ""
        print(
            f"  Zone {s['zone_id']:>3}: {s['trip_count']:>3} trips, "
            f"{s['duration_seconds']:.0f}s, "
            f"${s['total_fare']:,.2f} revenue, "
            f"{s['trips_per_minute']:.1f} trips/min{surge}"
        )


if __name__ == "__main__":
    run_session_window_demo(duration=15)
