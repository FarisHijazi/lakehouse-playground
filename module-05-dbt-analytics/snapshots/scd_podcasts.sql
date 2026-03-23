{% snapshot scd_podcasts %}

{{
    config(
        target_schema='snapshots',
        unique_key='podcast_id',
        strategy='check',
        check_cols=['name', 'name_en', 'category', 'language', 'host'],
    )
}}

select * from {{ ref('stg_podcasts') }}

{% endsnapshot %}
