-- Staging model for NYC daily weather data (Central Park station).
-- Used to analyze the impact of weather on taxi ridership.

with source as (

    select * from read_csv_auto('../data/raw/nyc_weather_2023.csv')

),

cleaned as (

    select
        cast(date as date) as weather_date,
        cast(temp_max_f as double) as temp_max_f,
        cast(temp_min_f as double) as temp_min_f,
        cast(temp_avg_f as double) as temp_avg_f,
        cast(precipitation_in as double) as precipitation_in,
        cast(snowfall_in as double) as snowfall_in,
        cast(snow_depth_in as double) as snow_depth_in,
        cast(wind_speed_mph as double) as wind_speed_mph

    from source

)

select * from cleaned
