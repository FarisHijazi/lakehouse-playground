-- Daily trip and revenue metrics across all taxi types.
-- Use this for time-series dashboards and trend analysis.

with daily as (

    select * from {{ ref('int_daily_trip_summary') }}

),

aggregated as (

    select
        trip_date,

        sum(trip_count) as total_trips,
        sum(total_passengers) as total_passengers,
        round(sum(total_revenue), 2) as total_revenue,
        round(sum(total_fare), 2) as total_fares,
        round(sum(total_tips), 2) as total_tips,
        round(sum(total_distance_miles), 2) as total_distance_miles,
        round(avg(avg_distance_miles), 2) as avg_distance_miles,
        round(avg(avg_duration_minutes), 2) as avg_duration_minutes,
        round(avg(avg_tip_pct), 2) as avg_tip_pct,
        round(avg(avg_speed_mph), 2) as avg_speed_mph,
        sum(airport_trip_count) as airport_trips,

        -- Breakdown by taxi type
        sum(case when taxi_type = 'yellow' then trip_count else 0 end) as yellow_trips,
        sum(case when taxi_type = 'green' then trip_count else 0 end) as green_trips,

        -- Revenue per trip
        case
            when sum(trip_count) > 0
                then round(sum(total_revenue) / sum(trip_count), 2)
            else 0
        end as avg_revenue_per_trip

    from daily
    group by trip_date

)

select * from aggregated
order by trip_date desc
