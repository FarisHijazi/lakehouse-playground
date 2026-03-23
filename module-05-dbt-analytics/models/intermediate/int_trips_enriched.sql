-- Enrich taxi trips with zone names, borough info, and derived metrics.
-- Unions yellow and green taxi trips (FHV has a different fare structure
-- and is handled separately in marts that need it).

with yellow as (

    select
        trip_id,
        taxi_type,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge
    from {{ ref('stg_yellow_trips') }}

),

green as (

    select
        trip_id,
        taxi_type,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        payment_type_id,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge
    from {{ ref('stg_green_trips') }}

),

trips_unioned as (

    select * from yellow
    union all
    select * from green

),

pickup_zones as (

    select * from {{ ref('stg_zones') }}

),

dropoff_zones as (

    select * from {{ ref('stg_zones') }}

),

enriched as (

    select
        -- Use taxi_type + trip_id to ensure uniqueness across union
        t.taxi_type || '-' || cast(t.trip_id as varchar) as trip_key,
        t.trip_id,
        t.taxi_type,
        t.vendor_id,

        -- Timestamps
        t.pickup_datetime,
        t.dropoff_datetime,
        cast(t.pickup_datetime as date) as trip_date,
        extract(hour from t.pickup_datetime) as pickup_hour,
        extract(dow from t.pickup_datetime) as pickup_dow,

        -- Trip details
        t.passenger_count,
        t.trip_distance,
        t.rate_code_id,
        t.payment_type_id,

        -- Pickup zone
        t.pickup_location_id,
        pz.zone_name as pickup_zone,
        pz.borough as pickup_borough,
        pz.service_zone as pickup_service_zone,

        -- Dropoff zone
        t.dropoff_location_id,
        dz.zone_name as dropoff_zone,
        dz.borough as dropoff_borough,
        dz.service_zone as dropoff_service_zone,

        -- Fare
        t.fare_amount,
        t.tip_amount,
        t.tolls_amount,
        t.total_amount,
        t.congestion_surcharge,

        -- Derived: trip duration in minutes
        round(
            epoch(t.dropoff_datetime - t.pickup_datetime) / 60.0, 2
        ) as trip_duration_minutes,

        -- Derived: speed in mph (avoid divide by zero)
        case
            when epoch(t.dropoff_datetime - t.pickup_datetime) > 0
                then round(
                    t.trip_distance / (epoch(t.dropoff_datetime - t.pickup_datetime) / 3600.0),
                    2
                )
            else null
        end as speed_mph,

        -- Derived: tip percentage
        case
            when t.fare_amount > 0
                then round(t.tip_amount / t.fare_amount * 100, 2)
            else 0
        end as tip_pct,

        -- Derived: is airport trip (JFK zone 132, LaGuardia zone 138, Newark zone 1)
        case
            when t.pickup_location_id in (132, 138, 1)
                or t.dropoff_location_id in (132, 138, 1)
                then true
            else false
        end as is_airport_trip

    from trips_unioned t
    left join pickup_zones pz on t.pickup_location_id = pz.location_id
    left join dropoff_zones dz on t.dropoff_location_id = dz.location_id

)

select * from enriched
