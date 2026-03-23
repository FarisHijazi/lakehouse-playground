with source as (

    select * from read_json_auto('../data/raw/ad_events.json')

),

cleaned as (

    select
        ad_event_id,
        event_id,
        user_id,
        cast(timestamp as timestamp) as event_timestamp,
        ad_type,
        action,
        advertiser,
        campaign_id,
        coalesce(revenue_sar, 0.0) as revenue_sar,
        duration_seconds

    from source

)

select * from cleaned
