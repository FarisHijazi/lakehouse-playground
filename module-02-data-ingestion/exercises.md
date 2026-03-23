# Module 02: Exercises

Work through these exercises in order. Each one builds on concepts from the previous.

---

## Exercise 1: Profile the Raw Data

**Goal**: Before writing any transforms, understand what you are working with.

Write a script (`profile_data.py`) that reads every raw data source and prints a
report covering:

- Row counts and column names
- Data types (as inferred by pandas or embedded in Parquet)
- Null counts and null percentages per column
- Unique value counts for categorical columns
- Sample values for each column
- Min/max for numeric and date columns
- Duplicate detection (full-row and key-based)

**Data sources to profile:**
- `data/raw/yellow_tripdata_2023-01.parquet` (and any other yellow months)
- `data/raw/green_tripdata_2023-01.parquet` (if downloaded)
- `data/raw/fhvhv_tripdata_2023-01.parquet` (if downloaded)
- `data/raw/taxi_zone_lookup.csv` (265 zones)
- `data/raw/vendors.csv`, `rate_codes.csv`, `payment_types.csv`
- `data/raw/nyc_weather_2023.csv` (365 days)

**Questions to answer:**
1. What percentage of yellow taxi trips have null `passenger_count`?
2. How many trips have negative `fare_amount`? What do they look like?
3. How many trips have `trip_distance = 0` but `fare_amount > 0`?
4. What are all the `rate_code_id` values? Does 99 (unknown) appear?
5. What is the date range of trips? Are there any trips outside the expected month?

**Hint**: Use `pd.read_parquet()` for Parquet files. For very large files, read
only a sample or specific columns with the `columns` parameter.

---

## Exercise 2: Convert Parquet to CSV/JSON (Format Tradeoffs)

**Goal**: Convert taxi trip Parquet to CSV and JSON to understand format tradeoffs.

Write a script (`convert_formats.py`) that:

1. Reads a yellow taxi Parquet file.
2. Writes the same data as CSV, JSON (line-delimited), and Parquet with different
   compression (Snappy, Gzip, Zstd, uncompressed).
3. Also converts the CSV dimension files (zones, vendors, etc.) to Parquet.
4. Prints the original file size vs each output format size.

**Key concepts to practice:**
- Defining PyArrow schemas with `pa.schema([pa.field(...), ...])`
- Choosing appropriate types: `pa.string()`, `pa.int32()`, `pa.float64()`, `pa.timestamp()`
- Setting compression: `pq.write_table(..., compression='snappy')`
- Understanding why taxi data ships as Parquet (not CSV) from the TLC

**Expected output structure:**
```
data/processed/bronze/
├── yellow_tripdata_2023-01.parquet   (re-written with explicit schema)
├── taxi_zones.parquet
├── vendors.parquet
├── rate_codes.parquet
├── payment_types.parquet
└── nyc_weather.parquet
```

---

## Exercise 3: Clean the Messy Trip Data

**Goal**: Handle real-world data quality issues in the yellow taxi trips.

Write a script (`clean_trips.py`) that:

1. **Handles null values**:
   - `passenger_count`: fill nulls with 1 (single rider assumption).
   - `rate_code_id`: replace 99 and nulls with 1 (standard rate).
   - `store_and_fwd_flag`: fill nulls with `'N'`.
   - `congestion_surcharge`, `airport_fee`: fill nulls with 0.0.

2. **Removes bad records**:
   - Negative `fare_amount` (unless very small, these are errors).
   - `trip_distance < 0` (impossible).
   - `passenger_count > 9` (taxi max is ~6, be generous).
   - Trips where `tpep_dropoff_datetime < tpep_pickup_datetime` (time travel).

3. **Caps outliers**:
   - `trip_distance > 200` miles (NYC is 35 miles long).
   - `fare_amount > 1000` (even JFK trips rarely exceed $100).
   - `tip_amount > 500`.

4. **Adds derived columns**:
   - `trip_duration_min`: dropoff minus pickup in minutes.
   - `pickup_date`: date extracted from pickup datetime.

5. Writes the cleaned data to `data/processed/silver/yellow_trips_clean.parquet`.

**Validation**: After cleaning, assert:
- No null values in `passenger_count`, `rate_code_id`.
- No negative `fare_amount` values.
- No trips with dropoff before pickup.

---

## Exercise 4: Deduplicate Taxi Trips

**Goal**: Remove duplicate trips from the yellow taxi data.

Write a script (`deduplicate_events.py`) that:

1. Reads yellow taxi trip data (raw Parquet).
2. Identifies exact row duplicates (the TLC data genuinely has them).
3. Identifies semantic duplicates using a composite key:
   `(vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id,
   do_location_id, trip_distance, fare_amount)`.
4. Reports how many duplicates were found at each level.
5. Writes deduplicated data to `data/processed/silver/yellow_trips_deduped.parquet`.

**Think about:**
- Why do duplicates happen in taxi data? (system resubmissions, meter resets, vendor bugs)
- What is the difference between exact duplicates and semantic duplicates?
- Why use a composite key instead of a single ID? (TLC data has no unique trip ID)

---

## Exercise 5: Partition Taxi Trips by Date and Borough

**Goal**: Organize trips into a partitioned directory structure for efficient querying.

Write a script (`partition_events.py`) that:

1. Reads yellow taxi trip data (cleaned or raw).
2. Joins with `taxi_zone_lookup.csv` to get the pickup borough name.
3. Extracts year and month from `tpep_pickup_datetime`.
4. Writes Parquet files partitioned by `pickup_borough` and `pickup_month`:
   ```
   data/processed/silver/yellow_trips_partitioned/
   ├── pickup_borough=Manhattan/
   │   ├── pickup_month=2023-01/
   │   │   └── data.parquet
   │   ├── pickup_month=2023-02/
   │   │   └── data.parquet
   │   └── ...
   ├── pickup_borough=Brooklyn/
   │   └── ...
   └── ...
   ```
5. Reports the number of records per partition.

**Key concepts:**
- Partitioning reduces I/O by allowing query engines to skip irrelevant files.
- Borough-level partitioning is ideal for NYC taxi data (5 boroughs + EWR + Unknown).
- PyArrow's `pq.write_to_dataset()` handles partitioning automatically.
- Joining with dimension tables during ingestion is a common enrichment pattern.

---

## Exercise 6: Compare File Sizes and Read Performance

**Goal**: See the real-world impact of file format and compression choices on taxi data.

Write a script (`compare_formats.py`) that takes a yellow taxi Parquet file and:

1. Writes it in multiple formats:
   - CSV (uncompressed)
   - JSON (line-delimited)
   - Parquet with Snappy compression
   - Parquet with Gzip compression
   - Parquet with Zstd compression
   - Parquet uncompressed
2. Measures and reports:
   - File size on disk for each format
   - Write time for each format
   - Read time for each format (full scan)
   - Read time for a filtered query (single column, predicate filter)
3. Prints a comparison table.

**Expected insight**: Parquet with Snappy should be 5-10x smaller than CSV and
significantly faster to read for analytical queries due to column pruning.

---

## Exercise 7: Incremental Ingestion

**Goal**: Build a pipeline that only processes new monthly taxi files, avoiding reprocessing.

Write a script (`incremental_ingest.py`) that:

1. Maintains a checkpoint file (`data/processed/checkpoints/taxi_ingest_checkpoint.json`)
   tracking which source Parquet files have already been processed.
2. On each run:
   - Scans `data/raw/` for all `yellow_tripdata_*.parquet` files.
   - Compares against the checkpoint to find new (unprocessed) files.
   - Reads only the new files.
   - Applies basic cleaning (from Exercise 3).
   - Appends the cleaned data to the output directory.
   - Updates the checkpoint.
3. On the first run, processes everything. On subsequent runs, processes nothing
   (since all files are already tracked).

**Key concepts:**
- Incremental ingestion avoids the O(n) cost of reprocessing all historical data.
- Checkpointing must be atomic: update the checkpoint only after data is successfully written.
- In production, tools like Apache Airflow or Dagster manage this automatically.

**Test it**: Run the script twice. The first run should process all files. The second
run should report "No new files to process."
