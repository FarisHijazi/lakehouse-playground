-- Staging model for NYC taxi zone lookup data.
-- Each row represents one of the 265 taxi zones across the 5 boroughs.

with source as (

    select * from read_csv_auto('../data/raw/taxi_zone_lookup.csv')

),

cleaned as (

    select
        cast(LocationID as integer) as location_id,
        cast(Borough as varchar) as borough,
        cast(Zone as varchar) as zone_name,
        cast(service_zone as varchar) as service_zone

    from source

)

select * from cleaned
