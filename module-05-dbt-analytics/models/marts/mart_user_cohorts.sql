-- User cohort analysis based on signup month.
-- Tracks retention and engagement by cohort.

with users as (

    select * from {{ ref('stg_users') }}

),

enriched as (

    select * from {{ ref('int_listens_enriched') }}

),

user_cohorts as (

    select
        user_id,
        date_trunc('month', signup_date) as cohort_month,
        subscription_type,
        country as user_country,
        gender
    from users

),

user_activity as (

    select
        user_id,
        date_trunc('month', event_date) as activity_month,
        count(*) as events,
        sum(listened_seconds) as listened_seconds,
        count(distinct episode_id) as distinct_episodes

    from enriched
    group by user_id, date_trunc('month', event_date)

),

cohort_activity as (

    select
        uc.cohort_month,
        ua.activity_month,
        -- Months since signup
        datediff('month', uc.cohort_month, ua.activity_month) as months_since_signup,
        uc.subscription_type,

        count(distinct uc.user_id) as active_users,
        sum(ua.events) as total_events,
        sum(ua.listened_seconds) as total_listened_seconds,
        round(avg(ua.listened_seconds), 1) as avg_listened_seconds_per_user,
        sum(ua.distinct_episodes) as total_distinct_episodes

    from user_cohorts uc
    inner join user_activity ua on uc.user_id = ua.user_id
    group by
        uc.cohort_month,
        ua.activity_month,
        datediff('month', uc.cohort_month, ua.activity_month),
        uc.subscription_type

)

select
    cohort_month,
    activity_month,
    months_since_signup,
    subscription_type,
    active_users,
    total_events,
    total_listened_seconds,
    round(total_listened_seconds / 3600.0, 1) as total_listened_hours,
    avg_listened_seconds_per_user,
    total_distinct_episodes

from cohort_activity
order by cohort_month, months_since_signup, subscription_type
