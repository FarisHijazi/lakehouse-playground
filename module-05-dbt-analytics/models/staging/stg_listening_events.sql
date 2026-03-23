with source as (

    select *
    from read_json_auto(
        '../data/raw/listening_events/*.jsonl',
        format = 'newline_delimited'
    )

),

deduplicated as (

    -- Remove duplicate event_ids, keeping the first occurrence
    select
        *,
        row_number() over (partition by event_id order by timestamp) as _row_num
    from source

),

cleaned as (

    select
        event_id,
        user_id,
        episode_id,
        event_type,
        cast(timestamp as timestamp) as event_timestamp,
        -- Cap listened_seconds: floor at 0, cap at 24 hours (86400s) as a sanity check
        case
            when listened_seconds < 0 then 0
            when listened_seconds > 86400 then 86400
            else listened_seconds
        end as listened_seconds,
        platform,
        country,
        app_version

    from deduplicated
    where _row_num = 1

)

select * from cleaned
