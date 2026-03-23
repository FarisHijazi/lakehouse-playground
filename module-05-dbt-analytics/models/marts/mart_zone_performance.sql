-- Zone-level performance metrics. One row per zone.
-- Answers: which zones generate the most trips, revenue, and tips?

with daily as (

    select * from {{ ref('int_daily_trip_summary') }}

),

zones as (

    select * from {{ ref('stg_zones') }}

),

zone_metrics as (

    select
        d.pickup_location_id as location_id,
        z.zone_name,
        z.borough,
        z.service_zone,

        count(distinct d.trip_date) as active_days,
        sum(d.trip_count) as total_trips,
        round(avg(d.trip_count), 1) as avg_daily_trips,
        sum(d.total_passengers) as total_passengers,
        round(sum(d.total_distance_miles), 2) as total_distance_miles,
        round(avg(d.avg_distance_miles), 2) as avg_distance_miles,
        round(avg(d.avg_duration_minutes), 2) as avg_duration_minutes,
        round(avg(d.avg_speed_mph), 2) as avg_speed_mph,

        -- Revenue
        round(sum(d.total_revenue), 2) as total_revenue,
        round(avg(d.avg_revenue_per_trip), 2) as avg_revenue_per_trip,
        round(sum(d.total_tips), 2) as total_tips,
        round(avg(d.avg_tip_pct), 2) as avg_tip_pct,

        -- Airport
        sum(d.airport_trip_count) as airport_trips,
        round(
            sum(d.airport_trip_count) * 100.0 / nullif(sum(d.trip_count), 0),
            2
        ) as airport_trip_pct,

        -- Rank by total trips
        rank() over (order by sum(d.trip_count) desc) as trip_volume_rank

    from daily d
    left join zones z on d.pickup_location_id = z.location_id
    group by
        d.pickup_location_id,
        z.zone_name,
        z.borough,
        z.service_zone

)

select * from zone_metrics
order by total_trips desc
