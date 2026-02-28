{% macro standardize_trip_id(column_name) -%}
nullif(trim(cast({{ column_name }} as string)), '')
{%- endmacro %}

{% macro standardize_vendor_id(column_name) -%}
nullif(trim(cast({{ column_name }} as string)), '')
{%- endmacro %}
