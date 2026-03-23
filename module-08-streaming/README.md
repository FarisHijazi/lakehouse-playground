# Module 08: Streaming Data Processing

## Overview

NYC taxi trips arrive continuously -- thousands per minute across boroughs. This module teaches stream processing concepts by simulating real-time taxi trip ingestion, windowed aggregations, and late-event handling. In production on Databricks, you would use **Structured Streaming** with Delta Lake as both source and sink.

## Key Concepts

### Batch vs Stream Processing

| Aspect | Batch | Stream |
|--------|-------|--------|
| Latency | Minutes to hours | Milliseconds to seconds |
| Data | Bounded (finite) | Unbounded (infinite) |
| Processing | Full dataset at once | Event by event or micro-batch |
| Use case | Historical analysis, ETL | Real-time dashboards, alerts |
| Tools | Spark, dbt, Airflow | Kafka, Flink, Spark Structured Streaming |

### Databricks Structured Streaming

Databricks uses Spark Structured Streaming with Delta Lake for unified batch and streaming:

```python
# Read a stream of new taxi trips landing as Parquet files
trips_stream = (
    spark.readStream
    .format("cloudFiles")          # Auto Loader
    .option("cloudFiles.format", "parquet")
    .load("/data/raw/yellow_tripdata/")
)

# Write to a Delta table with a checkpoint
(
    trips_stream.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/yellow_trips")
    .outputMode("append")
    .toTable("bronze.yellow_trips")
)
```

### Apache Kafka Core Concepts

**Topics**: Named feeds of messages. Example: `taxi_trips`, `zone_demand`, `fare_alerts`.

**Partitions**: Topics are split into partitions for parallelism. Events with the same key (e.g., PULocationID) go to the same partition, ensuring ordering per zone.

**Producers**: Applications that publish events to topics.

**Consumers**: Applications that subscribe to topics and process events.

**Consumer Groups**: Multiple consumers that share the work of reading a topic. Each partition is assigned to exactly one consumer in the group.

**Offsets**: Sequential IDs for messages within a partition. Consumers track their position (offset) to know what they have processed.

```
Producer -> Topic (Partition 0) -> Consumer Group A (Consumer 1)
                 (Partition 1) -> Consumer Group A (Consumer 2)
                 (Partition 2) -> Consumer Group A (Consumer 3)
```

### Delivery Guarantees

| Guarantee | Meaning | Trade-off |
|-----------|---------|-----------|
| At-most-once | Messages may be lost, never duplicated | Fastest, least safe |
| At-least-once | Messages never lost, may be duplicated | Safe, needs dedup |
| Exactly-once | Messages processed exactly once | Slowest, most complex |

**For taxi data**: Use at-least-once for trip ingestion (dedup by trip surrogate key in Silver layer), exactly-once for fare reconciliation.

### Event Time vs Processing Time

- **Event time**: When the trip actually started (pickup at 9:15 PM)
- **Processing time**: When your system processes the record (arrived at 9:17 PM, or 2 hours late for delayed meter uploads)
- **Watermarks**: "I believe I have seen all trips up to time T". Trips arriving after the watermark are *late events*.

### Windowing Strategies

```
Trips: [t1, t2, t3, t4, t5, t6, t7, t8, t9, t10]  (arriving over time)

Tumbling Window (5 min, no overlap):
  Window 1: [t1, t2, t3]
  Window 2: [t4, t5, t6]
  Window 3: [t7, t8, t9, t10]

Sliding Window (5 min window, 2 min slide):
  Window 1: [t1, t2, t3]
  Window 2: [t2, t3, t4, t5]
  Window 3: [t4, t5, t6, t7]

Session Window (gap = 3 min):
  Session 1: [t1, t2, t3]     (zone busy)
  Session 2: [t6, t7, t8, t9] (zone busy again after gap)
```

### Stream-Table Duality

A **stream** is a changelog of a **table**, and a **table** is a materialized view of a **stream**.

```
Stream: trip_started(zone=132), trip_started(zone=79), trip_ended(zone=132)
Table:  {zone_132: 0 active trips, zone_79: 1 active trip}  (current state)
```

This is the foundation of Delta Lake's streaming capabilities -- a Delta table can be both a batch table and a streaming source/sink simultaneously.

## Real-World Streaming for NYC Taxi Data

| Use Case | Source | Processing | Output |
|----------|--------|-----------|--------|
| Trip volume monitoring | New trip records | Count trips per minute per borough | Real-time dashboard |
| Surge detection | Trip fare data | Sliding window avg fare vs baseline | Pricing alert |
| Zone demand | Pickup events | Tumbling window trip count per zone | Driver dispatch |
| Revenue tracking | Completed trips | Running sum of fares, tips | Revenue dashboard |
| Late meter uploads | Trip records with old timestamps | Watermark-based handling | Data quality report |

## Exercises

See [exercises.md](exercises.md) for hands-on practice.

## Key Takeaway

> Stream processing is just batch processing with the time dimension made explicit. If you understand SQL GROUP BY with a WHERE on timestamp, you understand windowed aggregations -- streaming just does it continuously. On Databricks, Structured Streaming + Delta Lake gives you exactly-once semantics and unified batch/streaming pipelines.
