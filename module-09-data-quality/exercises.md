# Module 09: Data Quality -- Exercises

Work through these exercises in order. Each one builds on concepts from the previous.
All solutions use pandas and DuckDB. Run them from the repository root:

```bash
pip install pandas duckdb pyyaml pyarrow
python module-09-data-quality/solutions/<script>.py
```

The raw data at `data/raw/` includes NYC TLC taxi trip parquet files and reference CSV
files. Real taxi data has real quality issues: null passenger counts, zero-distance trips,
negative fares, impossible location IDs, and more. Your job is to find them.

---

## Exercise 1: Profile All Raw Datasets

**Goal**: Understand the shape and quality of every dataset before writing any checks.

**Tasks**:
1. Load every raw dataset (yellow_tripdata parquet files, green_tripdata parquet files,
   taxi_zone_lookup.csv, vendors.csv, rate_codes.csv, payment_types.csv, fhv_bases.csv,
   and nyc_weather_2023.csv).
2. For each dataset, compute:
   - Row count and column count
   - Null counts and null percentages per column
   - Duplicate row counts
   - Unique value counts per column
   - Basic statistics (min, max, mean, std) for numeric columns
   - Sample values for string columns to spot inconsistencies
3. Document every quality issue you find (null passenger counts, negative fares,
   impossible distances, invalid location IDs, etc.).

**Expected output**: A printed report for each dataset summarizing shape, nulls,
duplicates, and specific issues discovered.

**Solution**: `solutions/profile_all_data.py`

---

## Exercise 2: Build Custom Data Quality Checks

**Goal**: Build a reusable framework for running quality checks on any dataframe.

**Tasks**:
1. Implement these check types:
   - **Completeness**: Column should have no nulls (or null rate below a threshold).
   - **Uniqueness**: Column values should be unique (no duplicates).
   - **Range**: Numeric column values should fall within [min, max].
   - **Pattern**: String column values should match a regex pattern.
   - **Accepted values**: Column values should be in an allowed set.
2. Each check should return a result with: check name, column, passed (bool),
   total rows, failing rows, and failure percentage.
3. Run checks against yellow_tripdata and taxi_zone_lookup to validate:
   - fare_amount is within a reasonable range (e.g., -50 to 5000)
   - trip_distance is non-negative and below 500 miles
   - passenger_count is between 0 and 9
   - PULocationID and DOLocationID are between 1 and 265
   - payment_type is in the valid set {1, 2, 3, 4, 5, 6}
   - LocationID in zones is unique and not null

**Expected output**: A table of check results showing pass/fail status and failure counts.

**Solution**: `solutions/quality_checks.py`

---

## Exercise 3: Referential Integrity Checks

**Goal**: Validate that foreign key relationships hold across tables.

**Tasks**:
1. Check that every `PULocationID` in yellow trips exists in taxi_zone_lookup.LocationID.
2. Check that every `DOLocationID` in yellow trips exists in taxi_zone_lookup.LocationID.
3. Check that every `PULocationID` in green trips exists in taxi_zone_lookup.LocationID.
4. Check that every `DOLocationID` in green trips exists in taxi_zone_lookup.LocationID.
5. Check that every `VendorID` in yellow trips exists in vendors.vendor_id.
6. Check that every `payment_type` in yellow trips exists in payment_types.payment_type_id.
7. Check that every `RatecodeID` in yellow trips exists in rate_codes.rate_code_id.
8. For each check, report the number and percentage of orphaned records.

**Expected output**: A referential integrity report showing which relationships hold
and which have orphaned foreign keys.

**Solution**: `solutions/referential_integrity.py`

---

## Exercise 4: Freshness Checks

**Goal**: Detect stale data -- datasets where the most recent record is older than expected.

**Tasks**:
1. For each dataset with a timestamp column, find the most recent timestamp.
2. Calculate the "freshness" as the time gap between now and the latest record.
3. Define freshness SLOs (e.g., yellow trip data should be < 90 days old for a
   production system).
4. Flag datasets that violate their freshness SLO.
5. Check for gaps in the monthly trip data files -- are there missing months
   in the sequence?

**Expected output**: A freshness report showing the latest timestamp per dataset,
the freshness gap, and whether the SLO is met. A list of missing monthly files.

**Solution**: `solutions/freshness_checks.py`

---

## Exercise 5: Volume Anomaly Detection

**Goal**: Detect months with unusually high or low trip counts.

**Tasks**:
1. Count trips per month across all yellow trip parquet files.
2. Compute rolling statistics across available months.
3. Flag months where the count deviates significantly from the rolling mean.
4. Also flag months where the count drops more than 50% compared to the previous month.
5. Compare yellow vs green trip volumes for consistency.

**Expected output**: A table of monthly counts with rolling stats, anomaly flags, and
a summary of anomalous months found.

**Solution**: `solutions/volume_anomaly.py`

---

## Exercise 6: Quality Scoring System

**Goal**: Assign a 0-100 quality score to each dataset based on multiple dimensions.

**Tasks**:
1. Define scoring dimensions with weights:
   - Completeness (30%): Percentage of non-null values in required columns.
   - Uniqueness (20%): Percentage of unique values in key columns.
   - Validity (25%): Percentage of values passing range/pattern checks.
   - Consistency (15%): Percentage of values matching expected formats.
   - Timeliness (10%): Whether the data meets its freshness SLO.
2. Compute each dimension score for every dataset.
3. Calculate the weighted overall score.
4. Classify each dataset: EXCELLENT (>= 95), GOOD (>= 85), FAIR (>= 70),
   POOR (>= 50), CRITICAL (< 50).

**Expected output**: A scorecard for each dataset showing dimension scores, the
weighted overall score, and the classification.

**Solution**: `solutions/quality_scorer.py`

---

## Exercise 7: Circuit Breaker

**Goal**: Implement a mechanism that halts pipeline execution when data quality
drops below an acceptable threshold.

**Tasks**:
1. Define a `CircuitBreaker` class with configurable thresholds:
   - CRITICAL threshold (default 50): halt and page on-call.
   - WARNING threshold (default 80): log warning, proceed with caution.
   - PASSING threshold (default 90): normal operation.
2. The circuit breaker should accept a quality score and decide: OPEN (halt),
   HALF-OPEN (warn), or CLOSED (proceed).
3. Log all decisions with timestamps and reasons.
4. Simulate a pipeline run: compute quality scores for all datasets, then pass
   each through the circuit breaker.
5. Show which datasets would halt the pipeline and which would proceed.

**Expected output**: A pipeline simulation log showing circuit breaker decisions
for each dataset.

**Solution**: `solutions/circuit_breaker.py`

---

## Exercise 8: Quality Report Dashboard

**Goal**: Generate a comprehensive data quality report combining all checks.

**Tasks**:
1. Run all quality checks (profiling, completeness, uniqueness, validity,
   referential integrity, freshness, volume anomalies).
2. Compile results into a single report with sections:
   - Executive summary (overall health, critical issues count)
   - Per-dataset scorecards
   - Referential integrity results
   - Freshness status
   - Volume anomaly alerts
   - Top 10 most critical issues with recommended actions
3. Print the report in a readable, formatted layout.

**Expected output**: A multi-section quality report printed to the console.

**Solution**: `solutions/quality_report.py`

---

## Exercise 9: Data Contracts

**Goal**: Define data contracts as YAML configuration and validate datasets against them.

**Tasks**:
1. Study the example contracts in `contracts/yellow_trips_contract.yml` and
   `contracts/zones_contract.yml`.
2. Implement a contract validator that:
   - Loads a contract YAML file.
   - Validates schema: checks that expected columns exist with correct types.
   - Validates nullability: checks that non-nullable columns have no nulls.
   - Validates uniqueness: checks that unique columns have no duplicates.
   - Validates quality rules: evaluates rule expressions against the data.
3. Run the validator against the raw data.
4. Report which contract clauses pass and which fail.

**Expected output**: A contract validation report showing pass/fail per clause.

**Solution**: `solutions/data_contracts.py`
