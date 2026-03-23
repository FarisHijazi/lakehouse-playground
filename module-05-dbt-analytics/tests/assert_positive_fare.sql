-- This test fails if any trip in the enriched model has a negative fare.
-- The staging layer should have cleaned negative fares, but this acts
-- as a safety net.

select
    trip_key,
    fare_amount
from {{ ref('int_trips_enriched') }}
where fare_amount < 0
