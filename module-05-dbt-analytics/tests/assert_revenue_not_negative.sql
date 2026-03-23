-- This test fails if any ad event has negative revenue.

select
    ad_event_id,
    revenue_sar
from {{ ref('stg_ad_events') }}
where revenue_sar < 0
