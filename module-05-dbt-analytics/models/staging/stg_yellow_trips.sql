-- Staging model for yellow taxi trip data.
-- Reads bronze parquet files and performs light cleaning:
--   - Renames columns to snake_case conventions
--   - Casts timestamps and numeric types
--   - Filters out rows with null pickup timestamps

with source as (

    select * from read_parquet('../data/bronze/yellow_tripdata_*.parquet', union_by_name=true)

),

cleaned as (

    select
        -- Trip identifiers
        row_number() over () as trip_id,
        'yellow' as taxi_type,

        -- Vendor
        cast(VendorID as integer) as vendor_id,

        -- Timestamps
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,

        -- Trip details
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as double) as trip_distance,
        cast(RatecodeID as integer) as rate_code_id,
        cast(store_and_fwd_flag as varchar) as store_and_fwd_flag,

        -- Locations
        cast(PULocationID as integer) as pickup_location_id,
        cast(DOLocationID as integer) as dropoff_location_id,

        -- Payment
        cast(payment_type as integer) as payment_type_id,

        -- Fare breakdown
        cast(fare_amount as double) as fare_amount,
        cast(extra as double) as extra,
        cast(mta_tax as double) as mta_tax,
        cast(tip_amount as double) as tip_amount,
        cast(tolls_amount as double) as tolls_amount,
        cast(improvement_surcharge as double) as improvement_surcharge,
        cast(total_amount as double) as total_amount,
        cast(congestion_surcharge as double) as congestion_surcharge,
        cast(airport_fee as double) as airport_fee

    from source
    where tpep_pickup_datetime is not null

)

select * from cleaned
