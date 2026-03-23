with source as (

    select * from read_json_auto('../data/raw/podcasts.json')

),

cleaned as (

    select
        podcast_id,
        name,
        name_en,
        category,
        language,
        host,
        cast(created_at as date) as created_at

    from source

)

select * from cleaned
