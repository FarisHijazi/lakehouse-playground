# Module 04: Exercises -- Data Warehouse

Work through these exercises in order. Each builds on the previous one. Use DuckDB as your warehouse engine.

---

## Exercise 1: Design a Star Schema

**Goal:** Draw (on paper or in a diagram tool) the star schema for the NYC taxi trip analytics warehouse.

**Tasks:**

1. Identify the **business processes** we want to analyse:
   - Yellow taxi trips (fare revenue, distances, tip behaviour)
   - Green taxi trips (outer-borough service patterns)
   - For-hire vehicle trips (Uber, Lyft, livery car usage)
   - Weather impact on transportation

2. For each business process, identify:
   - The **grain** (one row = one ___?)
   - The **dimensions** (who, what, where, when)
   - The **measures** (numeric facts to aggregate)

3. Design these tables:

   **Dimension Tables:**
   - `dim_zones` -- taxi zone attributes (borough, zone name, service zone)
   - `dim_vendors` -- yellow/green taxi vendor companies
   - `dim_rate_codes` -- rate code descriptions (standard, JFK, Newark, etc.)
   - `dim_payment_types` -- payment method descriptions
   - `dim_fhv_bases` -- for-hire vehicle base companies (Uber, Lyft, etc.)
   - `dim_date` -- calendar dimension (one row per day)
   - `dim_weather` -- daily weather conditions

   **Fact Tables:**
   - `fact_yellow_trips` -- one row per yellow taxi trip
   - `fact_green_trips` -- one row per green taxi trip
   - `fact_fhv_trips` -- one row per for-hire vehicle trip

4. For each table, list the columns, data types, and key relationships.

**Hints:**
- Look at the raw Parquet and CSV files to understand available columns.
- Fact tables should contain foreign keys (to dimensions) and numeric measures.
- Push descriptive attributes into dimension tables.

---

## Exercise 2: Create Dimension Tables

**Goal:** Write SQL (using DuckDB) to create the dimension tables from raw data.

### 2a: dim_date

Create a date dimension covering 2018-01-01 to 2025-12-31. Include:

| Column | Type | Description |
|---|---|---|
| `date_key` | INTEGER | YYYYMMDD format (e.g., 20240115) |
| `full_date` | DATE | The actual date |
| `year` | INTEGER | 2024 |
| `quarter` | INTEGER | 1-4 |
| `month` | INTEGER | 1-12 |
| `month_name` | VARCHAR | 'January', 'February', ... |
| `day_of_month` | INTEGER | 1-31 |
| `day_of_week` | INTEGER | 1 (Monday) to 7 (Sunday) |
| `day_name` | VARCHAR | 'Monday', 'Tuesday', ... |
| `week_of_year` | INTEGER | 1-53 |
| `is_weekend` | BOOLEAN | true for Saturday/Sunday |

**Hint:** Use DuckDB's `generate_series` to create a range of dates, then extract parts.

```sql
SELECT UNNEST(generate_series(DATE '2018-01-01', DATE '2025-12-31', INTERVAL 1 DAY)) AS full_date
```

### 2b: dim_zones

Load from `taxi_zones.csv`. Include:
- `location_id` (the TLC zone ID, used as the key)
- `borough`, `zone`, `service_zone`

### 2c: dim_vendors

Load from `vendors.csv`. Include:
- `vendor_id`, `vendor_name`

### 2d: dim_rate_codes

Load from `rate_codes.csv`. Include:
- `rate_code_id`, `rate_code_name`

### 2e: dim_payment_types

Load from `payment_types.csv`. Include:
- `payment_type_id`, `payment_type_name`

### 2f: dim_fhv_bases

Load from `fhv_bases.csv`. Include:
- `base_number`, `base_name`, `dba` (doing business as), `base_type`

### 2g: dim_weather

Load from `daily_weather.csv`. Include:
- `date` as the key
- Temperature, precipitation, snow, wind speed columns
- Derive a `weather_category` column: 'Snow', 'Rain', 'Clear'

---

## Exercise 3: Create Fact Tables

**Goal:** Create the three fact tables from raw Parquet files, joining to dimension keys.

### 3a: fact_yellow_trips

Load from `yellow_taxi_trips.parquet`. For each trip:
- Map pickup/dropoff location IDs to `dim_zones`
- Map `VendorID` to `dim_vendors`
- Map `RatecodeID` to `dim_rate_codes`
- Map `payment_type` to `dim_payment_types`
- Look up `date_key` from `dim_date`
- Calculate `trip_duration_minutes` from pickup/dropoff timestamps
- Keep all fare-related measures: `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`, `trip_distance`, `passenger_count`

**Hint:** DuckDB reads Parquet natively and efficiently:
```sql
SELECT * FROM read_parquet('data/raw/yellow_taxi_trips.parquet')
```

### 3b: fact_green_trips

Load from `green_taxi_trips.parquet`. Similar structure to yellow trips.

### 3c: fact_fhv_trips

Load from `fhv_trips.parquet`. For each for-hire vehicle trip:
- Map pickup/dropoff location IDs to `dim_zones`
- Map `dispatching_base_num` to `dim_fhv_bases`
- Calculate `trip_duration_minutes` from pickup/dropoff timestamps
- Note: FHV trips have no fare data (Uber/Lyft do not report fares to TLC)

---

## Exercise 4: Implement SCD Type 2 for Taxi Zones

**Goal:** Implement Slowly Changing Dimension Type 2 to track taxi zone boundary changes.

**Scenario:** The following changes happened to taxi zones over time:

1. **2023-07-01**: Zone 261 renames from "World Trade Center" to "World Trade Center / Battery Park"
2. **2024-01-15**: Zone 132 (JFK Airport) changes service_zone from "Airports" to "Major Airports"
3. **2024-06-01**: Zone 138 (LaGuardia Airport) changes borough classification from "Queens" to "Airport Authority"

**Tasks:**

1. Add SCD Type 2 columns to `dim_zones`:
   - `valid_from` (DATE)
   - `valid_to` (DATE, use '9999-12-31' for current records)
   - `is_current` (BOOLEAN)

2. Write a Python function that processes a "change event" and:
   - Closes the current record (set `valid_to` and `is_current = false`)
   - Inserts a new record with the updated values
   - Assigns a new surrogate key

3. After processing all three changes, verify that the history is preserved correctly.

---

## Exercise 5: Analytical Queries

Write SQL queries to answer these business questions. Use the warehouse tables you built.

### 5a: Revenue by Borough and Zone

For each borough, find the top 10 pickup zones by total fare revenue from yellow taxi trips. Show:
- Borough, zone name, total revenue, trip count, average fare

### 5b: Trip Patterns by Hour and Day

Create a heatmap-ready dataset showing trip volume by hour of day and day of week:
- `day_of_week`, `day_name`, `hour_of_day`, `total_trips`, `avg_fare`

### 5c: Weather Impact on Trip Volume

Join trips to weather data and analyse:
- How do rainy/snowy days affect trip counts vs clear days?
- What is the average fare on bad-weather days vs good-weather days?
- Does tipping behaviour change with weather?

### 5d: Uber vs Lyft vs Taxi Comparison

Compare for-hire vehicles (grouped by base company) with yellow/green taxis:
- Total trips by service type
- Average trip duration
- Most popular pickup zones by service type

### 5e: Tip Analysis by Payment Type

Analyse tipping behaviour:
- Average tip percentage by payment type
- Tip percentage distribution (what fraction of fare goes to tips?)
- Does trip distance affect tip percentage?

### 5f: Airport Trip Analysis

Analyse trips to/from the three major airports (JFK zone 132, LaGuardia zone 138, Newark zone 1):
- Trip volume by airport and direction (to/from)
- Average fare by airport
- Peak hours for airport trips
- Most common origin/destination zones for each airport

---

## Exercise 6: Implement Partitioning by Date

**Goal:** Demonstrate date-based partitioning using DuckDB's Hive-partitioned Parquet export.

**Tasks:**

1. Export `fact_yellow_trips` to Hive-partitioned Parquet files:
```sql
COPY (
    SELECT *, year(pickup_datetime) AS year, month(pickup_datetime) AS month
    FROM fact_yellow_trips
)
TO 'output/yellow_trips_partitioned'
(FORMAT PARQUET, PARTITION_BY (year, month));
```

2. Query the partitioned data and observe that DuckDB only reads relevant partitions:
```sql
SELECT count(*) FROM parquet_scan('output/yellow_trips_partitioned/**/*.parquet', hive_partitioning=true)
WHERE year = 2023 AND month = 6;
```

3. Compare query performance between the partitioned Parquet and the DuckDB table.

---

## Exercise 7: Window Functions

Write queries using window functions. Each query should be a single SQL statement.

### 7a: Running Total of Daily Revenue

For each borough, calculate the cumulative daily revenue from yellow taxi trips:
- `pickup_date`, `borough`, `daily_revenue`, `running_total_revenue`

### 7b: Rank Zones by Trip Volume

Rank pickup zones by total trip count. Use:
- `RANK()` -- allows gaps (1, 2, 2, 4)
- `DENSE_RANK()` -- no gaps (1, 2, 2, 3)
- `ROW_NUMBER()` -- unique (1, 2, 3, 4)

Show all three rankings side by side, partitioned by borough.

### 7c: Moving Average of Trip Distances

Calculate a 7-day rolling average of trip distance per day for yellow taxis.

### 7d: Month-over-Month Growth Rate

For each borough, calculate month-over-month growth in trip count.
Use `LAG()` to reference the previous month.

### 7e: Percent of Total Calculations

For each zone, calculate:
- What percentage of the borough's total trips does this zone represent?
- What percentage of the city's total trips does this zone represent?

Use `SUM() OVER (PARTITION BY ...)` for the denominator.

---

## Exercise 8: Advanced Analytics with CTEs

Write each query using Common Table Expressions (CTEs) for readability.

### 8a: Peak Hour Analysis by Borough

Find the busiest hour of day for each borough, comparing weekdays vs weekends.

### 8b: Cross-Borough Trip Analysis

Analyse trips where pickup and dropoff are in different boroughs:
- Most common borough-to-borough routes
- Average fare for cross-borough trips
- How do cross-borough trips compare in distance and duration?

### 8c: Weather-Revenue Correlation

Build a daily summary combining trip revenue and weather, then:
- Calculate correlation between temperature and daily revenue
- Find the revenue impact of each additional inch of precipitation
- Identify the worst-weather days and their revenue impact
