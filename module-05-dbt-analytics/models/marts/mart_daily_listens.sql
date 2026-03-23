-- Daily listening activity aggregated by date and podcast.
-- Use this for time-series dashboards and trend analysis.

with enriched as (

    select * from {{ ref('int_listens_enriched') }}

),

daily as (

    select
        event_date,
        podcast_id,
        podcast_name_en,
        podcast_category,

        count(*) as total_events,
        count(distinct user_id) as unique_listeners,
        count(distinct episode_id) as unique_episodes,
        sum(listened_seconds) as total_listened_seconds,
        round(avg(listened_seconds), 1) as avg_listened_seconds,
        round(avg(listen_pct), 2) as avg_listen_pct,
        count(case when event_type = 'complete' then 1 end) as completions,
        count(case when event_type = 'skip' then 1 end) as skips

    from enriched
    group by
        event_date,
        podcast_id,
        podcast_name_en,
        podcast_category

)

select * from daily
order by event_date desc, total_events desc
