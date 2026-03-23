-- Staging model for green taxi trip data.
-- Green taxis serve areas outside Manhattan core.
-- Schema is similar to yellow but uses lpep_ prefix for timestamps
-- and includes an ehail_fee column.

with source as (

    select * from read_parquet('../data/bronze/green_tripdata_*.parquet', union_by_name=true)

),

cleaned as (

    select
        row_number() over () as trip_id,
        'green' as taxi_type,

        -- Vendor
        cast(VendorID as integer) as vendor_id,

        -- Timestamps (green uses lpep_ prefix)
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,

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
        cast(coalesce(ehail_fee, 0) as double) as ehail_fee,
        cast(trip_type as integer) as trip_type

    from source
    where lpep_pickup_datetime is not null

)

select * from cleaned
