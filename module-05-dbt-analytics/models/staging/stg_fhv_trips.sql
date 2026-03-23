-- Staging model for high-volume for-hire vehicle (FHV) trip data.
-- This covers Uber, Lyft, Via, and Juno rides.
-- Schema differs from yellow/green: no fare breakdown, uses
-- hvfhs_license_num and dispatching_base_num for company identification.

with source as (

    select * from read_parquet('../data/bronze/fhvhv_tripdata_*.parquet', union_by_name=true)

),

cleaned as (

    select
        row_number() over () as trip_id,
        'fhv' as taxi_type,

        -- Company identification
        cast(hvfhs_license_num as varchar) as hvfhs_license_num,
        cast(dispatching_base_num as varchar) as dispatching_base_num,
        cast(originating_base_num as varchar) as originating_base_num,

        -- Timestamps
        pickup_datetime,
        dropoff_datetime,
        request_datetime,
        on_scene_datetime,

        -- Locations
        cast(PULocationID as integer) as pickup_location_id,
        cast(DOLocationID as integer) as dropoff_location_id,

        -- Trip details
        cast(trip_miles as double) as trip_distance,
        cast(trip_time as integer) as trip_time_seconds,

        -- Fare
        cast(base_passenger_fare as double) as base_passenger_fare,
        cast(tolls as double) as tolls_amount,
        cast(bcf as double) as black_car_fund,
        cast(sales_tax as double) as sales_tax,
        cast(congestion_surcharge as double) as congestion_surcharge,
        cast(airport_fee as double) as airport_fee,
        cast(tips as double) as tip_amount,
        cast(driver_pay as double) as driver_pay,

        -- Flags
        cast(shared_request_flag as varchar) as shared_request_flag,
        cast(shared_match_flag as varchar) as shared_match_flag,
        cast(access_a_ride_flag as varchar) as access_a_ride_flag,
        cast(wav_request_flag as varchar) as wav_request_flag,
        cast(wav_match_flag as varchar) as wav_match_flag

    from source
    where pickup_datetime is not null

)

select * from cleaned
