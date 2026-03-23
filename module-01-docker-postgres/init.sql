-- =============================================================================
-- NYC Taxi & Limousine Commission — Analytics Schema
-- =============================================================================
-- Real-world data from the NYC TLC public dataset. This schema supports
-- yellow taxi, green taxi, and for-hire vehicle (Uber/Lyft) trip records
-- along with dimension tables for zones, vendors, rates, and weather.
--
-- The raw data is naturally messy:
--   - Null passenger counts, negative fares, zero-distance trips
--   - Outlier tip amounts, impossible timestamps
--   - Schema differences between yellow, green, and FHV data
--   - Rate code 99 = unknown (data quality issue in source)
--   - Store-and-forward flag inconsistencies across vendors
--
-- Design principles:
--   1. TEXT over VARCHAR — Postgres stores them identically.
--   2. TIMESTAMPTZ over TIMESTAMP — trips span timezone boundaries.
--   3. Minimal constraints on fact tables — the raw data IS messy,
--      and we want to load it as-is for cleaning in later modules.
--   4. BIGSERIAL PKs on trip tables — no natural key in TLC data.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- taxi_zones: Pickup/dropoff location lookup
-- ---------------------------------------------------------------------------
-- 265 taxi zones across NYC's 5 boroughs plus EWR and unknown.
-- Source: TLC taxi_zone_lookup.csv
CREATE TABLE taxi_zones (
    location_id   INTEGER PRIMARY KEY,
    borough       TEXT NOT NULL,
    zone          TEXT NOT NULL,
    service_zone  TEXT NOT NULL
);

COMMENT ON TABLE taxi_zones IS 'NYC taxi zone lookup — 265 zones across 5 boroughs + EWR.';
COMMENT ON COLUMN taxi_zones.service_zone IS 'Boro Zone, Yellow Zone, Airports, EWR, or N/A.';


-- ---------------------------------------------------------------------------
-- vendors: Taxi technology vendors
-- ---------------------------------------------------------------------------
CREATE TABLE vendors (
    vendor_id    INTEGER PRIMARY KEY,
    vendor_name  TEXT NOT NULL
);

COMMENT ON TABLE vendors IS 'Yellow/green taxi technology providers (CMT, VeriFone).';


-- ---------------------------------------------------------------------------
-- rate_codes: Trip rate classification
-- ---------------------------------------------------------------------------
CREATE TABLE rate_codes (
    rate_code_id    INTEGER PRIMARY KEY,
    rate_code_name  TEXT NOT NULL
);

COMMENT ON TABLE rate_codes IS 'Rate codes: standard, JFK, Newark, etc. Code 99 = unknown (data quality issue).';


-- ---------------------------------------------------------------------------
-- payment_types: How the fare was paid
-- ---------------------------------------------------------------------------
CREATE TABLE payment_types (
    payment_type_id    INTEGER PRIMARY KEY,
    payment_type_name  TEXT NOT NULL
);

COMMENT ON TABLE payment_types IS 'Payment methods: credit card, cash, no charge, dispute, etc.';


-- ---------------------------------------------------------------------------
-- fhv_bases: For-hire vehicle base/app companies
-- ---------------------------------------------------------------------------
CREATE TABLE fhv_bases (
    base_license_num  TEXT PRIMARY KEY,
    base_name         TEXT NOT NULL,
    app_company       TEXT NOT NULL
);

COMMENT ON TABLE fhv_bases IS 'FHV dispatching bases — Uber (HV0003), Lyft (HV0005), Via, Juno.';


-- ---------------------------------------------------------------------------
-- yellow_taxi_trips: The main fact table
-- ---------------------------------------------------------------------------
-- Millions of rows per month. This is the core analytical table.
-- Columns match the TLC data dictionary exactly.
-- Data quality issues in the wild:
--   - passenger_count is FLOAT in the source (yes, really)
--   - Negative fare_amount, total_amount (refunds? errors?)
--   - trip_distance = 0 with non-zero fare
--   - Pickup datetime after dropoff datetime
--   - rate_code_id = 99 (unknown)
--   - vendor_id values changed meaning across years
CREATE TABLE yellow_taxi_trips (
    trip_id                 BIGSERIAL PRIMARY KEY,
    vendor_id               SMALLINT,
    tpep_pickup_datetime    TIMESTAMPTZ,
    tpep_dropoff_datetime   TIMESTAMPTZ,
    passenger_count         DOUBLE PRECISION,  -- float in source data
    trip_distance           DOUBLE PRECISION,
    rate_code_id            DOUBLE PRECISION,  -- float in source (contains NaN)
    store_and_fwd_flag      TEXT,              -- 'Y' or 'N' (or null)
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    payment_type            BIGINT,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    airport_fee             DOUBLE PRECISION
);

-- The most common analytical queries:
--   1. "Trips from/to zone X" → index on pickup/dropoff location
--   2. "Daily trip volume" → index on pickup datetime
--   3. "Revenue by vendor" → index on vendor
CREATE INDEX idx_yellow_pickup_dt ON yellow_taxi_trips(tpep_pickup_datetime);
CREATE INDEX idx_yellow_pu_location ON yellow_taxi_trips(pu_location_id);
CREATE INDEX idx_yellow_do_location ON yellow_taxi_trips(do_location_id);
CREATE INDEX idx_yellow_vendor ON yellow_taxi_trips(vendor_id);

COMMENT ON TABLE yellow_taxi_trips IS 'NYC yellow taxi trip records. Millions of rows. Source: TLC.';
COMMENT ON COLUMN yellow_taxi_trips.passenger_count IS 'Float in source — can be 0, null, or fractional (data quality issue).';
COMMENT ON COLUMN yellow_taxi_trips.rate_code_id IS 'Float in source — contains NaN. 99 = unknown.';


-- ---------------------------------------------------------------------------
-- green_taxi_trips: Borough taxis (outer boroughs + north Manhattan)
-- ---------------------------------------------------------------------------
-- Similar to yellow but with extra columns (ehail_fee, trip_type).
-- Different pickup datetime column name (lpep_ vs tpep_).
CREATE TABLE green_taxi_trips (
    trip_id                 BIGSERIAL PRIMARY KEY,
    vendor_id               SMALLINT,
    lpep_pickup_datetime    TIMESTAMPTZ,
    lpep_dropoff_datetime   TIMESTAMPTZ,
    passenger_count         DOUBLE PRECISION,
    trip_distance           DOUBLE PRECISION,
    rate_code_id            DOUBLE PRECISION,
    store_and_fwd_flag      TEXT,
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    payment_type            BIGINT,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    ehail_fee               DOUBLE PRECISION,  -- green-taxi-only field
    trip_type               DOUBLE PRECISION   -- 1=street-hail, 2=dispatch
);

CREATE INDEX idx_green_pickup_dt ON green_taxi_trips(lpep_pickup_datetime);
CREATE INDEX idx_green_pu_location ON green_taxi_trips(pu_location_id);
CREATE INDEX idx_green_do_location ON green_taxi_trips(do_location_id);

COMMENT ON TABLE green_taxi_trips IS 'NYC green (boro) taxi trips. Different schema from yellow.';
COMMENT ON COLUMN green_taxi_trips.trip_type IS '1 = street-hail, 2 = dispatch. Float in source.';


-- ---------------------------------------------------------------------------
-- fhv_trips: For-hire vehicle high-volume (Uber, Lyft, Via, Juno)
-- ---------------------------------------------------------------------------
-- Completely different schema from yellow/green. Massive volume.
-- No fare breakdown — just base_passenger_fare and component charges.
CREATE TABLE fhv_trips (
    trip_id                 BIGSERIAL PRIMARY KEY,
    hvfhs_license_num       TEXT,              -- HV0003 = Uber, HV0005 = Lyft
    dispatching_base_num    TEXT,
    originating_base_num    TEXT,
    request_datetime        TIMESTAMPTZ,
    on_scene_datetime       TIMESTAMPTZ,
    pickup_datetime         TIMESTAMPTZ,
    dropoff_datetime        TIMESTAMPTZ,
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    trip_miles              DOUBLE PRECISION,
    trip_time               BIGINT,            -- seconds
    base_passenger_fare     DOUBLE PRECISION,
    tolls                   DOUBLE PRECISION,
    bcf                     DOUBLE PRECISION,  -- Black Car Fund
    sales_tax               DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    airport_fee             DOUBLE PRECISION,
    tips                    DOUBLE PRECISION,
    driver_pay              DOUBLE PRECISION,
    shared_request_flag     TEXT,              -- 'Y' or 'N'
    shared_match_flag       TEXT,
    access_a_ride_flag      TEXT,
    wav_request_flag        TEXT,              -- wheelchair accessible
    wav_match_flag          TEXT
);

CREATE INDEX idx_fhv_pickup_dt ON fhv_trips(pickup_datetime);
CREATE INDEX idx_fhv_pu_location ON fhv_trips(pu_location_id);
CREATE INDEX idx_fhv_do_location ON fhv_trips(do_location_id);
CREATE INDEX idx_fhv_license ON fhv_trips(hvfhs_license_num);

COMMENT ON TABLE fhv_trips IS 'For-hire vehicle (Uber/Lyft) trip records. Largest table.';
COMMENT ON COLUMN fhv_trips.hvfhs_license_num IS 'HV0002=Juno, HV0003=Uber, HV0004=Via, HV0005=Lyft.';
COMMENT ON COLUMN fhv_trips.trip_time IS 'Trip duration in seconds.';


-- ---------------------------------------------------------------------------
-- daily_weather: NYC Central Park weather station (NOAA)
-- ---------------------------------------------------------------------------
-- For enrichment joins: "How does weather affect taxi demand?"
CREATE TABLE daily_weather (
    date               DATE PRIMARY KEY,
    temp_max_f         DOUBLE PRECISION,
    temp_min_f         DOUBLE PRECISION,
    temp_avg_f         DOUBLE PRECISION,
    precipitation_in   DOUBLE PRECISION,
    snowfall_in        DOUBLE PRECISION,
    snow_depth_in      DOUBLE PRECISION,
    wind_speed_mph     DOUBLE PRECISION
);

COMMENT ON TABLE daily_weather IS 'Daily NYC weather from Central Park station. For enrichment joins.';
COMMENT ON COLUMN daily_weather.precipitation_in IS 'Total precipitation in inches (rain + melted snow).';
