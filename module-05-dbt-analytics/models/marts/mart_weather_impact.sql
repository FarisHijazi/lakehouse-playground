-- Analyze the impact of weather on taxi ridership and revenue.
-- Joins daily trip metrics with weather data.

with daily_metrics as (

    select * from {{ ref('mart_daily_metrics') }}

),

weather as (

    select * from {{ ref('stg_weather') }}

),

joined as (

    select
        d.trip_date,

        -- Weather conditions
        w.temp_max_f,
        w.temp_min_f,
        w.temp_avg_f,
        w.precipitation_in,
        w.snowfall_in,
        w.snow_depth_in,
        w.wind_speed_mph,

        -- Weather classification
        case
            when w.snowfall_in > 1 then 'snow'
            when w.precipitation_in > 0.5 then 'heavy_rain'
            when w.precipitation_in > 0 then 'light_rain'
            when w.temp_max_f > 90 then 'extreme_heat'
            when w.temp_min_f < 20 then 'extreme_cold'
            else 'clear'
        end as weather_category,

        -- Trip metrics
        d.total_trips,
        d.total_revenue,
        d.avg_revenue_per_trip,
        d.avg_distance_miles,
        d.avg_duration_minutes,
        d.avg_tip_pct,
        d.avg_speed_mph,
        d.airport_trips,
        d.yellow_trips,
        d.green_trips

    from daily_metrics d
    inner join weather w on d.trip_date = w.weather_date

)

select * from joined
order by trip_date
