-- Handle negative fare amounts in taxi trip data.
-- The NYC TLC data contains negative fares from adjustments, disputes,
-- and data entry errors. This macro replaces negatives with zero.
--
-- Usage in a model:
--   {{ clean_fare('fare_amount') }} as fare_amount

{% macro clean_fare(column_name) %}
    case
        when {{ column_name }} < 0 then 0
        else {{ column_name }}
    end
{% endmacro %}
