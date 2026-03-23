-- Enrich listening events with user, episode, and podcast metadata.
-- This is the core fact table that most mart models build on.

with listens as (

    select * from {{ ref('stg_listening_events') }}

),

users as (

    select * from {{ ref('stg_users') }}

),

episodes as (

    select * from {{ ref('stg_episodes') }}

),

podcasts as (

    select * from {{ ref('stg_podcasts') }}

),

enriched as (

    select
        -- Event dimensions
        l.event_id,
        l.event_type,
        l.event_timestamp,
        cast(l.event_timestamp as date) as event_date,
        l.listened_seconds,
        l.platform as listen_platform,
        l.country as listen_country,
        l.app_version,

        -- User dimensions
        l.user_id,
        u.name as user_name,
        u.country as user_country,
        u.signup_date,
        u.subscription_type,
        u.age,
        u.gender,

        -- Episode dimensions
        l.episode_id,
        e.title as episode_title,
        e.published_at as episode_published_at,
        e.duration_seconds as episode_duration_seconds,
        e.season,
        e.episode_number,

        -- Podcast dimensions
        p.podcast_id,
        p.name as podcast_name,
        p.name_en as podcast_name_en,
        p.category as podcast_category,
        p.language as podcast_language,
        p.host as podcast_host,

        -- Derived metrics
        case
            when e.duration_seconds > 0
                then round(l.listened_seconds * 100.0 / e.duration_seconds, 2)
            else 0
        end as listen_pct

    from listens l
    left join users u on l.user_id = u.user_id
    left join episodes e on l.episode_id = e.episode_id
    left join podcasts p on e.podcast_id = p.podcast_id

)

select * from enriched
