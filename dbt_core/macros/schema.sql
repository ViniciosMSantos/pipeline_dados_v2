{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'ci' and custom_schema_name is not none -%}
        {#- Slim CI: isola cada PR em schemas próprios (ex.: ci_pr_12_silver) -#}
        {{ target.schema }}_{{ custom_schema_name }}
    {%- elif custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name }}
    {%- endif -%}
{%- endmacro %}
