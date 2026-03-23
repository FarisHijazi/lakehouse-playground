# Module 02: Exercises

Work through these exercises in order. Each one builds on concepts from the previous.

---

## Exercise 1: Profile the Raw Data

**Goal**: Before writing any transforms, understand what you are working with.

Write a script (`profile_data.py`) that reads every raw data source and prints a
report covering:

- Row counts and column names
- Data types (as inferred by pandas)
- Null counts and null percentages per column
- Unique value counts for categorical columns
- Sample values for each column
- Min/max for numeric and date columns
- Duplicate detection (full-row and key-based)

**Data sources to profile:**
- `data/raw/users.csv` (5,000 users)
- `data/raw/podcasts.json` (10 podcasts)
- `data/raw/episodes.json` (784 episodes)
- `data/raw/listening_events/events_*.jsonl` (~200k+ events across ~2,500 daily files)
- `data/raw/cdn_logs.csv` (50,000 CDN log entries)
- `data/raw/ad_events.json` (18,071 ad events)

**Questions to answer:**
1. How many distinct date formats appear in `users.csv.signup_date`?
2. What are all the gender values in `users.csv`? How would you normalize them?
3. How many duplicate `event_id` values exist across listening events?
4. What percentage of user records have null `city` values?
5. What is the date range of listening events?

**Hint**: Use `pd.read_json(..., lines=True)` for JSONL files. To read all JSONL files,
use `glob.glob()` and `pd.concat()`.

---

## Exercise 2: Convert CSV/JSON to Parquet

**Goal**: Convert raw files to Parquet with explicit schemas and compression.

Write a script (`convert_formats.py`) that:

1. Reads each raw data source.
2. Defines an explicit PyArrow schema for each dataset (do not rely on pandas inference).
3. Writes Parquet files with Snappy compression to `data/processed/bronze/`.
4. Prints the original file size vs Parquet file size for each dataset.

**Key concepts to practice:**
- Defining PyArrow schemas with `pa.schema([pa.field(...), ...])`
- Choosing appropriate types: `pa.string()`, `pa.int32()`, `pa.float64()`, `pa.timestamp()`
- Setting compression: `pq.write_table(..., compression='snappy')`

**Expected output structure:**
```
data/processed/bronze/
├── podcasts.parquet
├── episodes.parquet
├── users.parquet
├── listening_events.parquet
├── cdn_logs.parquet
└── ad_events.parquet
```

---

## Exercise 3: Clean the Messy User Data

**Goal**: Handle real-world data quality issues in `users.csv`.

Write a script (`clean_users.py`) that:

1. **Parses mixed date formats** in `signup_date`:
   - ISO format: `2024-09-10`
   - ISO with time: `2022-09-21T00:00:00`
   - Day/Month/Year: `23/09/2022`
   - Month-Day-Year: `03-09-2019`, `07-21-2021`
   - Use `pd.to_datetime(..., format='mixed', dayfirst=False)` or write a custom parser.

2. **Normalizes gender values** to a standard set (`male`, `female`, `unknown`):
   - Input values include: `m`, `M`, `male`, `Male`, `f`, `F`, `female`, `Female`, `""` (empty)
   - Map all variations to `male`, `female`, or `unknown`.

3. **Handles null values**:
   - Fill missing `city` with `"Unknown"`.
   - Fill missing `age` with the median age.
   - Fill missing `gender` with `"unknown"`.
   - Fill missing `email` with a placeholder.

4. **Detects and removes full duplicates** (users appearing more than once).

5. Writes the cleaned data to `data/processed/silver/users_clean.parquet`.

**Validation**: After cleaning, assert:
- No null values in `signup_date`, `gender`, `city`.
- All gender values are in `{'male', 'female', 'unknown'}`.
- No duplicate `user_id` values.

---

## Exercise 4: Deduplicate Listening Events

**Goal**: Remove duplicate events from the listening event stream.

Write a script (`deduplicate_events.py`) that:

1. Reads all JSONL files from `data/raw/listening_events/`.
2. Identifies duplicates by `event_id` (exact duplicates from retries).
3. For duplicate `event_id` values, keeps the record with the latest timestamp
   (the most recent version is most likely to be correct).
4. Reports how many duplicates were found and removed.
5. Writes deduplicated data to `data/processed/silver/listening_events_deduped.parquet`.

**Think about:**
- Why do duplicates happen in event streams? (at-least-once delivery, retries, client bugs)
- What is the difference between exact duplicates and semantic duplicates?
- When would you deduplicate on a composite key (user_id + episode_id + timestamp)
  instead of event_id?

---

## Exercise 5: Partition Listening Events by Date

**Goal**: Organize events into a partitioned directory structure for efficient querying.

Write a script (`partition_events.py`) that:

1. Reads the deduplicated events from Exercise 4 (or raw events if not done yet).
2. Extracts the date from the timestamp column.
3. Writes Parquet files partitioned by `year` and `month`:
   ```
   data/processed/silver/listening_events_partitioned/
   ├── year=2018/
   │   ├── month=01/
   │   │   └── data.parquet
   │   ├── month=02/
   │   │   └── data.parquet
   │   └── ...
   ├── year=2019/
   │   └── ...
   └── ...
   ```
4. Reports the number of records per partition.

**Key concepts:**
- Partitioning reduces I/O by allowing query engines to skip irrelevant files.
- Over-partitioning (too many small files) hurts performance. Partition by coarse
  granularity (year/month) rather than fine (year/month/day) for this dataset size.
- PyArrow's `pq.write_to_dataset()` handles partitioning automatically.

---

## Exercise 6: Compare File Sizes and Read Performance

**Goal**: See the real-world impact of file format and compression choices.

Write a script (`compare_formats.py`) that takes the listening events data and:

1. Writes it in multiple formats:
   - CSV (uncompressed)
   - JSON
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

**Goal**: Build a pipeline that only processes new files, avoiding reprocessing.

Write a script (`incremental_ingest.py`) that:

1. Maintains a checkpoint file (`data/processed/checkpoints/listening_events_checkpoint.json`)
   tracking which source files have already been processed.
2. On each run:
   - Scans `data/raw/listening_events/` for all JSONL files.
   - Compares against the checkpoint to find new (unprocessed) files.
   - Reads only the new files.
   - Deduplicates the new events (within the batch and against previously processed event IDs).
   - Appends the new events to the partitioned output.
   - Updates the checkpoint.
3. On the first run, processes everything. On subsequent runs, processes nothing
   (since all files are already tracked).

**Key concepts:**
- Incremental ingestion avoids the O(n) cost of reprocessing all historical data.
- Checkpointing must be atomic: update the checkpoint only after data is successfully written.
- In production, tools like Apache Airflow or Dagster manage this automatically.

**Test it**: Run the script twice. The first run should process all files. The second
run should report "No new files to process."
