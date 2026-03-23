-- Hour-of-day patterns for taxi trips.
-- Answers: when are taxis busiest? How do fares and tips vary by hour?

with trips as (

    select * from {{ ref('int_trips_enriched') }}

),

hourly as (

    select
        pickup_hour,
        pickup_dow,

        -- Day type for easier analysis
        case
            when pickup_dow in (0, 6) then 'weekend'
            else 'weekday'
        end as day_type,

        count(*) as trip_count,
        round(avg(trip_distance), 2) as avg_distance_miles,
        round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
        round(avg(speed_mph), 2) as avg_speed_mph,
        round(avg(fare_amount), 2) as avg_fare,
        round(avg(tip_amount), 2) as avg_tip,
        round(avg(total_amount), 2) as avg_total,
        round(avg(tip_pct), 2) as avg_tip_pct,
        round(avg(passenger_count), 2) as avg_passengers,
        count(case when is_airport_trip then 1 end) as airport_trips,

        -- Percentage of trips that are airport trips
        round(
            count(case when is_airport_trip then 1 end) * 100.0 / count(*),
            2
        ) as airport_trip_pct

    from trips
    where trip_date is not null
    group by pickup_hour, pickup_dow

)

select * from hourly
order by pickup_dow, pickup_hour
