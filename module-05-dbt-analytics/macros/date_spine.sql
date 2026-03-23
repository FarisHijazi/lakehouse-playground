-- Generate a continuous series of dates between start_date and end_date.
-- Useful for filling gaps in time-series data (e.g., days with zero trips).
--
-- Usage:
--   select * from {{ date_spine('2023-01-01', '2023-12-31') }}

{% macro date_spine(start_date, end_date) %}

    select
        unnest(
            generate_series(
                cast('{{ start_date }}' as date),
                cast('{{ end_date }}' as date),
                interval '1 day'
            )
        ) as date_day

{% endmacro %}
