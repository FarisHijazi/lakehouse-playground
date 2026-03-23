-- This test fails if there are trips with pickup_location_id values
-- that do not exist in the zones reference table.

select
    t.trip_key,
    t.pickup_location_id
from {{ ref('int_trips_enriched') }} t
left join {{ ref('stg_zones') }} z on t.pickup_location_id = z.location_id
where z.location_id is null
