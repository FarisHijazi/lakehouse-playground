-- This test fails if any listening event has a negative duration after cleaning.
-- We expect stg_listening_events to floor negatives at 0.

select
    event_id,
    listened_seconds
from {{ ref('stg_listening_events') }}
where listened_seconds < 0
