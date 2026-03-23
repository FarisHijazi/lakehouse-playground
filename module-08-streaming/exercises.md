# Module 08: Streaming Exercises

## Exercise 1: Build a Streaming Trip Producer

Create a Python script that simulates real-time taxi trip arrivals by reading from
parquet files and emitting trips row by row.

**Requirements:**
- Read yellow taxi trip parquet files from `data/raw/`
- Emit trips at realistic rates (more during rush hours, less at night)
- Add realistic late-arriving events (delayed meter uploads)
- Support configurable trip rate (trips per second)

**Concepts:** Event generation, timestamps, realistic traffic patterns

---

## Exercise 2: Build a Simple Stream Consumer

Create a consumer that reads trip events from the producer and computes running aggregates.

**Requirements:**
- Count total trips processed
- Track trips by payment type and rate code
- Compute running average fare amount and trip distance
- Print stats every 5 seconds

**Concepts:** Consumer loop, running aggregations, state management

---

## Exercise 3: Tumbling Window Aggregation

Implement 5-minute tumbling windows to aggregate taxi trip metrics.

**Requirements:**
- Each window is exactly 5 minutes, no overlap
- Count trips, sum fares, compute average distance per window
- Emit results when the window closes
- Handle trips arriving within the window

**Concepts:** Tumbling windows, windowed aggregation, window lifecycle

---

## Exercise 4: Session Windows

Group trips by pickup zone into "busy periods" based on activity gaps.

**Requirements:**
- A busy period starts when a trip is picked up in a zone
- A busy period ends after N minutes of no pickups in that zone
- Track: busy period duration, total trips, total revenue per zone
- Identify "surge zones" (busy periods with many trips in a short time)

**Concepts:** Session windows, per-key state, gap detection

---

## Exercise 5: Late Event Handling with Watermarks

Handle taxi trips that arrive after their window has closed (delayed meter uploads).

**Requirements:**
- Set a watermark at max_event_time - allowed_lateness
- Trips within the watermark update the window
- Trips beyond the watermark go to a "late trips" side output
- Count and report how many late trips were dropped vs processed

**Concepts:** Watermarks, late data, allowed lateness

---

## Exercise 6: Simulated Kafka System

Build a complete simulated Kafka system for taxi trip events using Python threading.

**Requirements:**
- Implement Topic class with partitions
- Implement Producer with partitioning by zone (PULocationID)
- Implement Consumer and ConsumerGroup with offset tracking
- Support multiple consumer groups reading the same topic independently
- Demonstrate rebalancing when a consumer joins/leaves

**Concepts:** Kafka internals, partitioning, consumer groups, offsets

---

## Exercise 7: Real-Time Dashboard Metrics

Build a system that continuously computes dashboard-ready metrics from the trip stream.

**Requirements:**
- Trips per minute by borough (Manhattan, Brooklyn, Queens, Bronx, Staten Island)
- Running revenue totals (fares + tips) by payment type
- Surge detection: flag zones where average fare exceeds 2x the rolling average
- Airport trip monitoring (JFK, LaGuardia, Newark zones)

**Concepts:** Complex event processing, multi-metric computation, alerting

---

## Solutions

All solutions are in the `solutions/` directory. Try each exercise before peeking!
