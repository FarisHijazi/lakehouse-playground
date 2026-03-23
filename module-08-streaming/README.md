# Module 08: Streaming Data Processing

## Overview

At Thmanyah, real-time data is critical: live listener counts, trending episode detection, CDN quality monitoring, and real-time ad serving. This module teaches you stream processing concepts using simulated Kafka-like patterns.

## Key Concepts

### Batch vs Stream Processing

| Aspect | Batch | Stream |
|--------|-------|--------|
| Latency | Minutes to hours | Milliseconds to seconds |
| Data | Bounded (finite) | Unbounded (infinite) |
| Processing | Full dataset at once | Event by event or micro-batch |
| Use case | Historical analysis, ETL | Real-time dashboards, alerts |
| Tools | Spark, dbt, Airflow | Kafka, Flink, Spark Streaming |

### Apache Kafka Core Concepts

**Topics**: Named feeds of messages. Example: `listening_events`, `cdn_logs`, `ad_impressions`.

**Partitions**: Topics are split into partitions for parallelism. Events with the same key (e.g., user_id) go to the same partition, ensuring ordering per user.

**Producers**: Applications that publish events to topics.

**Consumers**: Applications that subscribe to topics and process events.

**Consumer Groups**: Multiple consumers that share the work of reading a topic. Each partition is assigned to exactly one consumer in the group.

**Offsets**: Sequential IDs for messages within a partition. Consumers track their position (offset) to know what they've processed.

```
Producer → Topic (Partition 0) → Consumer Group A (Consumer 1)
                (Partition 1) → Consumer Group A (Consumer 2)
                (Partition 2) → Consumer Group A (Consumer 3)
```

### Delivery Guarantees

| Guarantee | Meaning | Trade-off |
|-----------|---------|-----------|
| At-most-once | Messages may be lost, never duplicated | Fastest, least safe |
| At-least-once | Messages never lost, may be duplicated | Safe, needs dedup |
| Exactly-once | Messages processed exactly once | Slowest, most complex |

**At Thmanyah**: Use at-least-once for listening events (dedup in Silver layer), exactly-once for ad billing.

### Event Time vs Processing Time

- **Event time**: When the event actually happened (user pressed play at 9:15 PM)
- **Processing time**: When your system processes it (arrived at 9:15:03 PM, or 2 hours late)
- **Watermarks**: "I believe I've seen all events up to time T". Events arriving after the watermark are *late events*.

### Windowing Strategies

```
Events: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]  (arriving over time)

Tumbling Window (5 min, no overlap):
  Window 1: [1, 2, 3]
  Window 2: [4, 5, 6]
  Window 3: [7, 8, 9, 10]

Sliding Window (5 min window, 2 min slide):
  Window 1: [1, 2, 3]
  Window 2: [2, 3, 4, 5]
  Window 3: [4, 5, 6, 7]

Session Window (gap = 3 min):
  Session 1: [1, 2, 3]     (user active)
  Session 2: [6, 7, 8, 9]  (user returned after gap)
```

### Lambda vs Kappa Architecture

**Lambda**: Separate batch and speed layers. Batch recomputes truth periodically, speed layer handles real-time. Results merged at serving layer.
- Pro: Batch layer corrects stream errors
- Con: Two codebases to maintain

**Kappa**: Everything is a stream. Reprocessing = replay the stream from the beginning.
- Pro: Single codebase
- Con: Reprocessing can be slow

**Modern approach**: Most companies use Kappa with Delta Lake/Iceberg for "stream + table" duality.

### Stream-Table Duality

A **stream** is a changelog of a **table**, and a **table** is a materialized view of a **stream**.

```
Stream: INSERT user_1, INSERT user_2, UPDATE user_1, DELETE user_2
Table:  {user_1: updated_data}  (current state)
```

This is the foundation of Kafka's KSQL and Flink SQL.

## Real-World Streaming at a Podcast Platform

| Use Case | Source | Processing | Output |
|----------|--------|-----------|--------|
| Live listener count | Play/pause events | Count distinct users per episode per minute | Real-time dashboard |
| Trending episodes | Play events | Sliding window top-N by plays | Homepage ranking |
| CDN monitoring | Quality logs | Tumbling window avg rebuffer rate | Alert if > threshold |
| Ad serving | User context events | Enrich with user profile, select ad | Real-time ad decision |
| Recommendations | Listen history | Session window, collaborative filtering | "Up next" suggestions |

## Exercises

See [exercises.md](exercises.md) for hands-on practice.

## Key Takeaway

> Stream processing is just batch processing with the time dimension made explicit. If you understand SQL GROUP BY with a WHERE on timestamp, you understand windowed aggregations — streaming just does it continuously.
