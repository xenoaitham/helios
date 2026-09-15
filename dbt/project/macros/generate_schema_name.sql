{#
    Use a custom schema verbatim instead of dbt's default
    `<target_schema>_<custom>` prefixing. The profile already targets
    `staging`; models land in `staging` exactly (ADR-008).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
