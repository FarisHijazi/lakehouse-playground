-- Sessionize listening events per user.
-- A new session starts when there is a gap of 30+ minutes between events.

with events as (

    select
        user_id,
        event_id,
        episode_id,
        event_timestamp,
        listened_seconds,
        event_type,
        lag(event_timestamp) over (
            partition by user_id order by event_timestamp
        ) as prev_event_timestamp
    from {{ ref('stg_listening_events') }}

),

with_session_boundary as (

    select
        *,
        case
            when prev_event_timestamp is null then 1
            when epoch(event_timestamp) - epoch(prev_event_timestamp) > 1800 then 1
            else 0
        end as is_new_session
    from events

),

with_session_id as (

    select
        *,
        sum(is_new_session) over (
            partition by user_id
            order by event_timestamp
            rows between unbounded preceding and current row
        ) as session_num
    from with_session_boundary

),

sessions as (

    select
        user_id,
        session_num,
        user_id || '-' || cast(session_num as varchar) as session_id,
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        count(*) as events_in_session,
        count(distinct episode_id) as episodes_in_session,
        sum(listened_seconds) as total_listened_seconds,
        epoch(max(event_timestamp)) - epoch(min(event_timestamp)) as session_duration_seconds

    from with_session_id
    group by user_id, session_num

)

select * from sessions
