{% snapshot scd_zones %}

{{
    config(
        target_schema='snapshots',
        unique_key='location_id',
        strategy='check',
        check_cols=['borough', 'zone_name', 'service_zone'],
    )
}}

select * from {{ ref('stg_zones') }}

{% endsnapshot %}
