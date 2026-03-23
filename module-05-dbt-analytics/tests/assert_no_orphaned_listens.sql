-- This test fails if there are listening events referencing episodes
-- that do not exist in the episodes source.

select
    le.event_id,
    le.episode_id
from {{ ref('stg_listening_events') }} le
left join {{ ref('stg_episodes') }} e on le.episode_id = e.episode_id
where e.episode_id is null
