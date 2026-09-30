{#- Custom schemas are used as-is (staging, marts, ...) instead of dbt's default
    "<target schema>_<custom>" so both warehouses get the same, predictable names.
    Developers isolate their work with a separate target / database, not by prefix. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
