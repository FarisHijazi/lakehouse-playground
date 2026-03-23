-- Podcast-level performance summary.
-- One row per podcast with lifetime and recent metrics.

with enriched as (

    select * from {{ ref('int_listens_enriched') }}

),

episodes as (

    select * from {{ ref('stg_episodes') }}

),

podcast_stats as (

    select
        podcast_id,
        podcast_name,
        podcast_name_en,
        podcast_category,
        podcast_language,
        podcast_host,

        count(*) as total_events,
        count(distinct user_id) as total_unique_listeners,
        count(distinct episode_id) as episodes_listened,
        count(distinct event_date) as active_days,

        sum(listened_seconds) as total_listened_seconds,
        round(sum(listened_seconds) / 3600.0, 1) as total_listened_hours,
        round(avg(listened_seconds), 1) as avg_listened_seconds,
        round(avg(listen_pct), 2) as avg_listen_pct,

        count(case when event_type = 'complete' then 1 end) as total_completions,
        round(
            count(case when event_type = 'complete' then 1 end) * 100.0
            / nullif(count(*), 0),
            2
        ) as completion_rate,

        min(event_date) as first_listen_date,
        max(event_date) as last_listen_date,

        -- Recent 30-day metrics
        count(case when event_date >= current_date - interval '30 days' then 1 end) as events_last_30d,
        count(distinct case when event_date >= current_date - interval '30 days' then user_id end) as listeners_last_30d

    from enriched
    group by
        podcast_id,
        podcast_name,
        podcast_name_en,
        podcast_category,
        podcast_language,
        podcast_host

),

episode_counts as (

    select
        podcast_id,
        count(*) as total_episodes,
        max(season) as total_seasons
    from episodes
    group by podcast_id

)

select
    ps.*,
    ec.total_episodes,
    ec.total_seasons,
    round(ps.total_unique_listeners * 1.0 / nullif(ec.total_episodes, 0), 1) as listeners_per_episode

from podcast_stats ps
left join episode_counts ec on ps.podcast_id = ec.podcast_id
order by total_listened_hours desc
