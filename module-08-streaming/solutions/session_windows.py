#!/usr/bin/env python3
"""
Exercise 5 Solution: Session Windows

Groups user events into listening sessions based on activity gaps.
A session ends after 5 minutes of inactivity from the same user.

Session Window visualization:
  User A: [play...pause.....play...complete]  → 1 session (gap < 5 min)
  User A: [play...pause...........play]       → 2 sessions (gap > 5 min)

This is a per-key (per-user) windowing strategy.
"""

import json
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Queue, Empty

from stream_producer import StreamProducer


class UserSession:
    """Represents a single user listening session."""

    def __init__(self, user_id, first_event):
        self.user_id = user_id
        self.start_time = datetime.strptime(first_event["timestamp"], "%Y-%m-%d %H:%M:%S")
        self.last_activity = self.start_time
        self.events = [first_event]
        self.episodes = set()
        self.total_listen_seconds = 0
        self.episodes.add(first_event["episode_id"])
        if first_event.get("listened_seconds"):
            self.total_listen_seconds += first_event["listened_seconds"]

    def add_event(self, event):
        self.events.append(event)
        event_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
        self.last_activity = max(self.last_activity, event_time)
        self.episodes.add(event["episode_id"])
        if event.get("listened_seconds"):
            self.total_listen_seconds += event["listened_seconds"]

    @property
    def duration_seconds(self):
        return (self.last_activity - self.start_time).total_seconds()

    @property
    def is_binge(self):
        """A binge session is > 1 hour."""
        return self.duration_seconds > 3600

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "session_start": self.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "session_end": self.last_activity.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": self.duration_seconds,
            "event_count": len(self.events),
            "unique_episodes": len(self.episodes),
            "total_listen_seconds": self.total_listen_seconds,
            "is_binge": self.is_binge,
        }


class SessionWindowProcessor:
    """Manages session windows per user."""

    def __init__(self, gap_seconds=300):
        """
        Args:
            gap_seconds: Inactivity gap that closes a session (default: 5 minutes)
        """
        self.gap = timedelta(seconds=gap_seconds)
        self.active_sessions = {}  # user_id -> UserSession
        self.completed_sessions = []

    def process_event(self, event):
        """Process a single event, assigning it to a session."""
        user_id = event["user_id"]
        event_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")

        if user_id in self.active_sessions:
            session = self.active_sessions[user_id]

            # Check if this event is within the session gap
            if event_time - session.last_activity <= self.gap:
                session.add_event(event)
            else:
                # Gap exceeded → close old session, start new one
                self.completed_sessions.append(session)
                self.active_sessions[user_id] = UserSession(user_id, event)
        else:
            # New session for this user
            self.active_sessions[user_id] = UserSession(user_id, event)

    def check_expired(self, current_time: datetime):
        """Check for sessions that have expired (no activity within gap)."""
        expired = []
        for user_id, session in list(self.active_sessions.items()):
            if current_time - session.last_activity > self.gap:
                self.completed_sessions.append(session)
                expired.append(user_id)
                del self.active_sessions[user_id]
        return expired

    def flush_all(self):
        """Close all active sessions (end of stream)."""
        for session in self.active_sessions.values():
            self.completed_sessions.append(session)
        self.active_sessions.clear()

    def get_stats(self):
        """Get summary statistics about completed sessions."""
        if not self.completed_sessions:
            return {"total_sessions": 0}

        durations = [s.duration_seconds for s in self.completed_sessions]
        return {
            "total_sessions": len(self.completed_sessions),
            "active_sessions": len(self.active_sessions),
            "avg_duration_sec": sum(durations) / len(durations),
            "max_duration_sec": max(durations),
            "binge_sessions": sum(1 for s in self.completed_sessions if s.is_binge),
            "avg_episodes_per_session": sum(len(s.episodes) for s in self.completed_sessions) / len(self.completed_sessions),
        }


def run_session_window_demo(duration=20):
    """Run the session window demo."""
    print("=" * 60)
    print("Session Window Demo (gap=30s for demo speed)")
    print("=" * 60)

    q = Queue()
    processor = SessionWindowProcessor(gap_seconds=30)  # 30s gap for demo

    # Start producer
    producer = StreamProducer(q, events_per_second=25)
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
            processor.process_event(event)
            events_processed += 1

            event_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
            sim_time = max(sim_time, event_time)

            # Periodically check for expired sessions
            if events_processed % 50 == 0:
                expired = processor.check_expired(sim_time)
                if expired:
                    print(f"\n  Expired {len(expired)} sessions at {sim_time}")

        except Empty:
            if not prod_thread.is_alive():
                break

    # Flush remaining sessions
    processor.flush_all()
    prod_thread.join()

    # Print results
    stats = processor.get_stats()
    print(f"\n{'=' * 50}")
    print("SESSION WINDOW RESULTS")
    print(f"{'=' * 50}")
    print(f"  Events processed:       {events_processed}")
    print(f"  Total sessions:         {stats['total_sessions']}")
    print(f"  Avg session duration:   {stats.get('avg_duration_sec', 0):.1f}s")
    print(f"  Max session duration:   {stats.get('max_duration_sec', 0):.1f}s")
    print(f"  Binge sessions (>1hr):  {stats.get('binge_sessions', 0)}")
    print(f"  Avg episodes/session:   {stats.get('avg_episodes_per_session', 0):.1f}")

    # Show some sample sessions
    print(f"\nSample completed sessions:")
    for session in processor.completed_sessions[:5]:
        s = session.to_dict()
        print(f"  User {s['user_id']}: {s['event_count']} events, "
              f"{s['duration_seconds']:.0f}s, {s['unique_episodes']} episodes"
              f"{' [BINGE]' if s['is_binge'] else ''}")


if __name__ == "__main__":
    run_session_window_demo(duration=15)
