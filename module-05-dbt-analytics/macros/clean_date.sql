-- Parse a date string that may be in one of several formats:
--   YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY
--
-- Usage in a model:
--   {{ clean_date('signup_date') }} as signup_date

{% macro clean_date(column_name) %}
    case
        when {{ column_name }} ~ '^\d{4}-\d{2}-\d{2}$'
            then cast({{ column_name }} as date)
        when {{ column_name }} ~ '^\d{2}-\d{2}-\d{4}$'
            then strptime({{ column_name }}, '%d-%m-%Y')::date
        when {{ column_name }} ~ '^\d{2}/\d{2}/\d{4}$'
            then strptime({{ column_name }}, '%d/%m/%Y')::date
        else null
    end
{% endmacro %}
