-- Override the default schema name generation.
-- In dev, prepend "dev_" to custom schemas. In prod, use the schema as-is.
-- For DuckDB (single-user), we use the custom schema directly.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
