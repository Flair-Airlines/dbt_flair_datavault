
{% macro parse_quotes(ITEM_NAME) -%}
    (REPLACE({{ITEM_NAME}},'"',''))
{%- endmacro %}
