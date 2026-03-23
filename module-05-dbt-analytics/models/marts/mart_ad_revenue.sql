-- Ad revenue analysis by advertiser, campaign, and time period.

with ads as (

    select * from {{ ref('stg_ad_events') }}

),

enriched as (

    select
        a.*,
        cast(a.event_timestamp as date) as event_date,
        date_trunc('month', a.event_timestamp) as event_month

    from ads a

),

ad_summary as (

    select
        event_month,
        advertiser,
        campaign_id,
        ad_type,

        -- Volume metrics
        count(*) as total_events,
        count(case when action = 'impression' then 1 end) as impressions,
        count(case when action = 'click' then 1 end) as clicks,
        count(case when action = 'skip' then 1 end) as skips,

        -- Revenue metrics
        sum(revenue_sar) as total_revenue_sar,
        round(avg(revenue_sar), 4) as avg_revenue_per_event,

        -- Performance metrics
        round(
            count(case when action = 'click' then 1 end) * 100.0
            / nullif(count(case when action = 'impression' then 1 end), 0),
            2
        ) as click_through_rate,
        round(
            count(case when action = 'skip' then 1 end) * 100.0
            / nullif(count(case when action = 'impression' then 1 end), 0),
            2
        ) as skip_rate,

        -- Duration metrics
        round(avg(duration_seconds), 1) as avg_ad_duration_seconds,

        count(distinct user_id) as unique_users_reached,
        min(event_date) as first_event_date,
        max(event_date) as last_event_date

    from enriched
    group by
        event_month,
        advertiser,
        campaign_id,
        ad_type

)

select * from ad_summary
order by event_month desc, total_revenue_sar desc
