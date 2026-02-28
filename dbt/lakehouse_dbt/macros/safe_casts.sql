{% macro safe_cast(expression, target_type) -%}
cast({{ expression }} as {{ target_type }})
{%- endmacro %}
