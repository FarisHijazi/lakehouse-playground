with source as (

    select * from read_json_auto('../data/raw/episodes.json')

),

cleaned as (

    select
        episode_id,
        podcast_id,
        title,
        cast(published_at as timestamp) as published_at,
        duration_seconds,
        season,
        episode_number

    from source

)

select * from cleaned
