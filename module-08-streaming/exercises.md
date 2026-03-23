# Module 08: Streaming Exercises

## Exercise 1: Build a Streaming Event Producer

Create a Python script that simulates a real-time stream of podcast listening events.

**Requirements:**
- Generate events at realistic rates (more in evening hours, less at night)
- Events should follow the same schema as our listening events data
- Add realistic latency (some events arrive late)
- Support configurable event rate (events per second)

**Concepts:** Event generation, timestamps, realistic traffic patterns

---

## Exercise 2: Build a Simple Stream Consumer

Create a consumer that reads events from the producer and computes running aggregates.

**Requirements:**
- Count total events processed
- Track events per event type (play, pause, skip, complete)
- Compute running average listen duration
- Print stats every 5 seconds

**Concepts:** Consumer loop, running aggregations, state management

---

## Exercise 3: Tumbling Window Aggregation

Implement tumbling (fixed) windows to count listeners per episode in 5-minute windows.

**Requirements:**
- Each window is exactly 5 minutes, no overlap
- Count unique listeners per episode per window
- Emit results when the window closes
- Handle events arriving within the window

**Concepts:** Tumbling windows, windowed aggregation, window lifecycle

---

## Exercise 4: Sliding Window Aggregation

Implement sliding windows for moving averages.

**Requirements:**
- 10-minute window, sliding every 2 minutes
- Compute moving average of concurrent listeners
- Compare with tumbling window results

**Concepts:** Sliding windows, overlapping computation

---

## Exercise 5: Session Windows

Group user events into listening sessions.

**Requirements:**
- A session starts with a "play" event
- A session ends after 5 minutes of inactivity from the same user
- Track: session duration, episodes listened, total listen time
- Identify "binge listeners" (sessions > 1 hour)

**Concepts:** Session windows, per-key state, gap detection

---

## Exercise 6: Late Event Handling with Watermarks

Handle events that arrive after their window has closed.

**Requirements:**
- Set a watermark at max_event_time - 30 seconds
- Events within the watermark update the window
- Events beyond the watermark go to a "late events" side output
- Count and report how many late events were dropped vs. processed

**Concepts:** Watermarks, late data, allowed lateness

---

## Exercise 7: Stream-to-File Sink

Write windowed aggregation results to Parquet files.

**Requirements:**
- Every completed window writes a Parquet file
- Files are partitioned by window_start date
- Support append mode (don't overwrite previous windows)
- Include metadata: window_start, window_end, record_count

**Concepts:** Sinks, file output, exactly-once file writes

---

## Exercise 8: Simulated Kafka System

Build a complete simulated Kafka system using Python threading.

**Requirements:**
- Implement Topic class with partitions
- Implement Producer with partitioning by key
- Implement Consumer and ConsumerGroup with offset tracking
- Support multiple consumer groups reading the same topic independently
- Demonstrate rebalancing when a consumer joins/leaves

**Concepts:** Kafka internals, partitioning, consumer groups, offsets

---

## Exercise 9: Real-Time Dashboard Metrics

Build a system that continuously computes dashboard metrics.

**Requirements:**
- Concurrent listeners right now (count distinct users with play events in last 5 min)
- Trending episodes (most plays in last 30 minutes, compared to previous 30 min)
- CDN health score (avg rebuffer rate in last 10 minutes, alert if > 5%)
- Geographic hotspots (top countries by active listeners)

**Concepts:** Complex event processing, multi-metric computation, alerting

---

## Solutions

All solutions are in the `solutions/` directory. Try each exercise before peeking!
