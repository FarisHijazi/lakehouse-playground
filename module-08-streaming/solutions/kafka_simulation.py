#!/usr/bin/env python3
"""
Exercise 8 Solution: Simulated Kafka System

Full simulation of Kafka concepts using Python threading and queues:
- Topics with partitions
- Producers with key-based partitioning
- Consumer groups with partition assignment
- Offset tracking and commit
- Rebalancing demonstration

This teaches you Kafka internals without requiring a Kafka installation.
"""

import hashlib
import json
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from queue import Queue, Empty
from typing import Callable, Dict, List, Optional


class Partition:
    """A single partition within a topic. Stores messages with sequential offsets."""

    def __init__(self, partition_id: int):
        self.partition_id = partition_id
        self.messages = []  # List of (offset, key, value, timestamp)
        self.lock = threading.Lock()

    def append(self, key, value):
        """Append a message and return its offset."""
        with self.lock:
            offset = len(self.messages)
            self.messages.append((offset, key, value, datetime.now()))
            return offset

    def read(self, start_offset: int, max_records: int = 10):
        """Read messages starting from offset."""
        with self.lock:
            end = min(start_offset + max_records, len(self.messages))
            return self.messages[start_offset:end]

    @property
    def latest_offset(self):
        return len(self.messages)


class Topic:
    """A Kafka-like topic with multiple partitions."""

    def __init__(self, name: str, num_partitions: int = 3):
        self.name = name
        self.num_partitions = num_partitions
        self.partitions = {i: Partition(i) for i in range(num_partitions)}
        print(f"  [Topic] Created '{name}' with {num_partitions} partitions")

    def get_partition_for_key(self, key: str) -> int:
        """Determine partition using consistent hashing on the key."""
        if key is None:
            # Round-robin for null keys
            return hash(time.time()) % self.num_partitions
        key_hash = int(hashlib.md5(key.encode()).hexdigest(), 16)
        return key_hash % self.num_partitions


class Producer:
    """Publishes messages to a topic with key-based partitioning."""

    def __init__(self, topic: Topic):
        self.topic = topic
        self.messages_sent = 0

    def send(self, key: str, value: dict):
        """Send a message to the topic."""
        partition_id = self.topic.get_partition_for_key(key)
        partition = self.topic.partitions[partition_id]
        offset = partition.append(key, value)
        self.messages_sent += 1
        return partition_id, offset


class Consumer:
    """Reads messages from assigned partitions, tracking offsets."""

    def __init__(self, consumer_id: str):
        self.consumer_id = consumer_id
        self.assigned_partitions: Dict[int, Partition] = {}
        self.committed_offsets: Dict[int, int] = {}  # partition_id -> offset
        self.messages_consumed = 0

    def assign(self, partitions: Dict[int, Partition]):
        """Assign partitions to this consumer (called during rebalance)."""
        self.assigned_partitions = partitions
        for pid in partitions:
            if pid not in self.committed_offsets:
                self.committed_offsets[pid] = 0
        print(f"    [Consumer {self.consumer_id}] Assigned partitions: {list(partitions.keys())}")

    def poll(self, max_records=10):
        """Poll for new messages from assigned partitions."""
        messages = []
        for pid, partition in self.assigned_partitions.items():
            offset = self.committed_offsets.get(pid, 0)
            records = partition.read(offset, max_records)
            for record in records:
                messages.append({
                    "partition": pid,
                    "offset": record[0],
                    "key": record[1],
                    "value": record[2],
                    "timestamp": record[3],
                })
            # Auto-advance offset (at-least-once)
            if records:
                self.committed_offsets[pid] = records[-1][0] + 1
                self.messages_consumed += len(records)
        return messages

    def commit(self):
        """Commit current offsets (in real Kafka, this goes to __consumer_offsets topic)."""
        pass  # Already tracked in self.committed_offsets


class ConsumerGroup:
    """
    Manages a group of consumers that share partitions of a topic.
    Each partition is assigned to exactly one consumer in the group.
    """

    def __init__(self, group_id: str, topic: Topic):
        self.group_id = group_id
        self.topic = topic
        self.consumers: List[Consumer] = []
        print(f"  [ConsumerGroup] Created '{group_id}' for topic '{topic.name}'")

    def add_consumer(self, consumer: Consumer):
        """Add a consumer and trigger rebalance."""
        self.consumers.append(consumer)
        print(f"  [ConsumerGroup] Consumer '{consumer.consumer_id}' joined → rebalancing...")
        self._rebalance()

    def remove_consumer(self, consumer: Consumer):
        """Remove a consumer and trigger rebalance."""
        self.consumers.remove(consumer)
        print(f"  [ConsumerGroup] Consumer '{consumer.consumer_id}' left → rebalancing...")
        self._rebalance()

    def _rebalance(self):
        """
        Reassign partitions to consumers using range assignment.
        In real Kafka, this is done by the Group Coordinator.
        """
        if not self.consumers:
            return

        partitions = list(self.topic.partitions.items())
        num_consumers = len(self.consumers)

        # Clear all assignments
        for consumer in self.consumers:
            consumer.assigned_partitions = {}

        # Range assignment: divide partitions evenly
        for i, (pid, partition) in enumerate(partitions):
            consumer_idx = i % num_consumers
            consumer = self.consumers[consumer_idx]
            consumer.assigned_partitions[pid] = partition
            if pid not in consumer.committed_offsets:
                consumer.committed_offsets[pid] = 0

        # Print new assignments
        for consumer in self.consumers:
            pids = list(consumer.assigned_partitions.keys())
            print(f"    [{consumer.consumer_id}] → partitions {pids}")


# ========================================
# Demo: Full Kafka Simulation
# ========================================

def run_kafka_simulation():
    print("=" * 60)
    print("Kafka Simulation Demo")
    print("=" * 60)

    # 1. Create a topic with 4 partitions
    print("\n--- Step 1: Create Topic ---")
    topic = Topic("listening_events", num_partitions=4)

    # 2. Create a producer
    print("\n--- Step 2: Produce Messages ---")
    producer = Producer(topic)

    # Produce some events, keyed by user_id
    users = ["usr_000001", "usr_000002", "usr_000003", "usr_000004", "usr_000005"]
    for i in range(20):
        user = users[i % len(users)]
        event = {
            "event_id": f"evt_{i:04d}",
            "user_id": user,
            "event_type": "play",
            "episode_id": f"ep_{(i % 3) + 1:04d}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        pid, offset = producer.send(key=user, value=event)
        if i < 5:
            print(f"  Sent event {event['event_id']} → partition {pid}, offset {offset}")

    print(f"  ... sent {producer.messages_sent} messages total")

    # Show partition distribution
    print("\n  Partition distribution:")
    for pid, partition in topic.partitions.items():
        print(f"    Partition {pid}: {partition.latest_offset} messages")

    # 3. Create consumer group with 2 consumers
    print("\n--- Step 3: Consumer Group (2 consumers) ---")
    group = ConsumerGroup("analytics_group", topic)

    consumer_a = Consumer("consumer-A")
    consumer_b = Consumer("consumer-B")
    group.add_consumer(consumer_a)
    group.add_consumer(consumer_b)

    # 4. Consume messages
    print("\n--- Step 4: Consume Messages ---")
    msgs_a = consumer_a.poll(max_records=100)
    msgs_b = consumer_b.poll(max_records=100)
    print(f"  Consumer A read: {len(msgs_a)} messages")
    print(f"  Consumer B read: {len(msgs_b)} messages")
    print(f"  Total: {len(msgs_a) + len(msgs_b)} messages (should = {producer.messages_sent})")

    # Show messages are ordered per partition per consumer
    if msgs_a:
        print(f"\n  Consumer A sample (from partitions {list(consumer_a.assigned_partitions.keys())}):")
        for msg in msgs_a[:3]:
            print(f"    partition={msg['partition']}, offset={msg['offset']}, user={msg['value']['user_id']}")

    # 5. Demonstrate rebalancing
    print("\n--- Step 5: Rebalancing (add consumer C) ---")
    consumer_c = Consumer("consumer-C")
    group.add_consumer(consumer_c)

    # 6. Produce more messages and consume
    print("\n--- Step 6: Produce more, consume with 3 consumers ---")
    for i in range(12):
        user = users[i % len(users)]
        producer.send(key=user, value={"event_id": f"evt_{20+i:04d}", "user_id": user, "event_type": "play"})

    msgs_a2 = consumer_a.poll(max_records=100)
    msgs_b2 = consumer_b.poll(max_records=100)
    msgs_c2 = consumer_c.poll(max_records=100)
    print(f"  Consumer A: {len(msgs_a2)} new messages")
    print(f"  Consumer B: {len(msgs_b2)} new messages")
    print(f"  Consumer C: {len(msgs_c2)} new messages")

    # 7. Demonstrate independent consumer groups
    print("\n--- Step 7: Independent Consumer Groups ---")
    print("  Creating a second consumer group (same topic)...")
    group2 = ConsumerGroup("dashboard_group", topic)
    dashboard_consumer = Consumer("dashboard-1")
    group2.add_consumer(dashboard_consumer)

    # This consumer reads from the beginning (offset 0)
    dashboard_msgs = dashboard_consumer.poll(max_records=100)
    print(f"  Dashboard consumer read: {len(dashboard_msgs)} messages (reads ALL from beginning)")
    print(f"  Analytics group total: {consumer_a.messages_consumed + consumer_b.messages_consumed + consumer_c.messages_consumed} messages")
    print("\n  Key insight: Different consumer groups read independently!")

    # 8. Show key-based ordering guarantee
    print("\n--- Step 8: Ordering Guarantee ---")
    print("  Same key always goes to same partition → ordered per user:")
    for user in users[:3]:
        pid = topic.get_partition_for_key(user)
        print(f"    {user} → always partition {pid}")

    print("\nDone! This simulation covers:")
    print("  - Topics and partitions")
    print("  - Key-based partitioning (ordering guarantee)")
    print("  - Consumer groups and partition assignment")
    print("  - Rebalancing when consumers join/leave")
    print("  - Independent consumer groups reading same topic")
    print("  - Offset tracking")


if __name__ == "__main__":
    run_kafka_simulation()
