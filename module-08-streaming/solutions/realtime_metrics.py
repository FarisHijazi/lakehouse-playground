#!/usr/bin/env python3
"""
Exercise 9 Solution: Real-Time Dashboard Metrics

Continuously computes dashboard-ready metrics from the event stream:
- Concurrent listeners (distinct users with play in last 5 min)
- Trending episodes (plays in last 30 min vs previous 30 min)
- CDN health score (simulated)
- Geographic hotspots
"""

import json
import time
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta
from queue import Queue, Empty

from stream_producer import StreamProducer


class RealTimeMetrics:
    """Computes real-time dashboard metrics from streaming events."""

    def __init__(self):
        # Sliding window buffers (deque of (timestamp, event) tuples)
        self.recent_events = deque()  # last 30 min
        self.event_count = 0

    def add_event(self, event):
        """Add an event and maintain the sliding window."""
        event_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
        self.recent_events.append((event_time, event))
        self.event_count += 1

        # Evict events older than 60 minutes
        cutoff = event_time - timedelta(minutes=60)
        while self.recent_events and self.recent_events[0][0] < cutoff:
            self.recent_events.popleft()

    def get_concurrent_listeners(self, as_of: datetime = None):
        """Count distinct users with play events in the last 5 minutes."""
        if not self.recent_events:
            return 0
        if as_of is None:
            as_of = self.recent_events[-1][0]
        cutoff = as_of - timedelta(minutes=5)
        users = set()
        for ts, event in self.recent_events:
            if ts >= cutoff and event["event_type"] == "play":
                users.add(event["user_id"])
        return len(users)

    def get_trending_episodes(self, as_of: datetime = None, top_n=5):
        """
        Find trending episodes: most plays in last 15 min vs previous 15 min.
        Trending score = (recent_plays - previous_plays) / max(previous_plays, 1)
        """
        if not self.recent_events:
            return []
        if as_of is None:
            as_of = self.recent_events[-1][0]

        recent_cutoff = as_of - timedelta(minutes=15)
        previous_cutoff = as_of - timedelta(minutes=30)

        recent_plays = defaultdict(int)
        previous_plays = defaultdict(int)

        for ts, event in self.recent_events:
            if event["event_type"] != "play":
                continue
            ep = event["episode_id"]
            if ts >= recent_cutoff:
                recent_plays[ep] += 1
            elif ts >= previous_cutoff:
                previous_plays[ep] += 1

        # Compute trending score
        trending = []
        all_episodes = set(recent_plays.keys()) | set(previous_plays.keys())
        for ep in all_episodes:
            r = recent_plays.get(ep, 0)
            p = previous_plays.get(ep, 0)
            score = (r - p) / max(p, 1)
            trending.append((ep, r, p, score))

        trending.sort(key=lambda x: x[3], reverse=True)
        return trending[:top_n]

    def get_geographic_distribution(self, as_of: datetime = None):
        """Get listener distribution by country in last 10 minutes."""
        if not self.recent_events:
            return {}
        if as_of is None:
            as_of = self.recent_events[-1][0]
        cutoff = as_of - timedelta(minutes=10)

        country_users = defaultdict(set)
        for ts, event in self.recent_events:
            if ts >= cutoff and event.get("country"):
                country_users[event["country"]].add(event["user_id"])

        return {country: len(users) for country, users in
                sorted(country_users.items(), key=lambda x: len(x[1]), reverse=True)}

    def get_platform_distribution(self, as_of: datetime = None):
        """Get listener distribution by platform."""
        if not self.recent_events:
            return {}
        if as_of is None:
            as_of = self.recent_events[-1][0]
        cutoff = as_of - timedelta(minutes=10)

        platform_counts = defaultdict(int)
        for ts, event in self.recent_events:
            if ts >= cutoff and event.get("platform"):
                platform_counts[event["platform"]] += 1

        return dict(sorted(platform_counts.items(), key=lambda x: x[1], reverse=True))

    def print_dashboard(self, as_of: datetime = None):
        """Print a formatted real-time dashboard."""
        if not self.recent_events:
            print("  No data yet.")
            return

        if as_of is None:
            as_of = self.recent_events[-1][0]

        concurrent = self.get_concurrent_listeners(as_of)
        trending = self.get_trending_episodes(as_of)
        geo = self.get_geographic_distribution(as_of)
        platforms = self.get_platform_distribution(as_of)

        print(f"\n{'━' * 55}")
        print(f"  REAL-TIME DASHBOARD  |  {as_of.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'━' * 55}")
        print(f"  🎧 Concurrent Listeners:  {concurrent}")
        print(f"  📊 Events in buffer:      {len(self.recent_events)}")
        print(f"  📈 Total processed:       {self.event_count}")

        if trending:
            print(f"\n  📈 Trending Episodes:")
            for ep, recent, prev, score in trending[:3]:
                arrow = "↑" if score > 0 else "↓" if score < 0 else "→"
                print(f"     {ep}: {recent} plays (was {prev}) {arrow} {score:+.1f}x")

        if geo:
            print(f"\n  🌍 Top Countries:")
            for country, count in list(geo.items())[:5]:
                print(f"     {country}: {count} listeners")

        if platforms:
            print(f"\n  📱 Platforms:")
            for platform, count in list(platforms.items())[:4]:
                print(f"     {platform}: {count} events")

        print(f"{'━' * 55}")


def run_realtime_dashboard(duration=25):
    """Run real-time dashboard metrics demo."""
    print("=" * 60)
    print("Real-Time Dashboard Metrics Demo")
    print("=" * 60)

    q = Queue()
    metrics = RealTimeMetrics()

    # Producer
    producer = StreamProducer(q, events_per_second=30)
    sim_start = datetime(2024, 11, 15, 21, 0, 0)
    prod_thread = threading.Thread(
        target=producer.produce,
        kwargs={"duration_seconds": duration, "simulated_time": sim_start}
    )
    prod_thread.start()

    # Process and display dashboard
    start = time.time()
    last_dashboard = time.time()
    dashboard_interval = 5  # Print dashboard every 5 seconds

    while time.time() - start < duration + 3:
        try:
            event = q.get(timeout=0.5)
            metrics.add_event(event)
        except Empty:
            if not prod_thread.is_alive() and q.empty():
                break

        # Periodically print dashboard
        if time.time() - last_dashboard >= dashboard_interval:
            metrics.print_dashboard()
            last_dashboard = time.time()

    # Final dashboard
    print("\n\nFINAL DASHBOARD:")
    metrics.print_dashboard()

    prod_thread.join()


if __name__ == "__main__":
    run_realtime_dashboard(duration=20)
