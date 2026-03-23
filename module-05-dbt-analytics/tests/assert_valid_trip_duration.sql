-- This test fails if any trip has a dropoff time before its pickup time.
-- Such records indicate data quality issues in the source.

select
    trip_key,
    pickup_datetime,
    dropoff_datetime
from {{ ref('int_trips_enriched') }}
where dropoff_datetime < pickup_datetime
