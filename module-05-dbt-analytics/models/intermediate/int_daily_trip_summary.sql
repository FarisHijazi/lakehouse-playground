-- Daily aggregation of trips by zone and taxi type.
-- Provides a summary used by several mart models.

with trips as (

    select * from {{ ref('int_trips_enriched') }}

),

daily_summary as (

    select
        trip_date,
        pickup_location_id,
        pickup_zone,
        pickup_borough,
        taxi_type,

        count(*) as trip_count,
        count(distinct dropoff_location_id) as unique_dropoff_zones,
        sum(passenger_count) as total_passengers,
        round(avg(passenger_count), 2) as avg_passengers,
        round(sum(trip_distance), 2) as total_distance_miles,
        round(avg(trip_distance), 2) as avg_distance_miles,
        round(sum(trip_duration_minutes), 2) as total_duration_minutes,
        round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
        round(avg(speed_mph), 2) as avg_speed_mph,

        -- Revenue
        round(sum(fare_amount), 2) as total_fare,
        round(sum(tip_amount), 2) as total_tips,
        round(sum(tolls_amount), 2) as total_tolls,
        round(sum(total_amount), 2) as total_revenue,
        round(avg(total_amount), 2) as avg_revenue_per_trip,
        round(avg(tip_pct), 2) as avg_tip_pct,

        -- Airport trips
        count(case when is_airport_trip then 1 end) as airport_trip_count

    from trips
    where trip_date is not null
    group by
        trip_date,
        pickup_location_id,
        pickup_zone,
        pickup_borough,
        taxi_type

)

select * from daily_summary
