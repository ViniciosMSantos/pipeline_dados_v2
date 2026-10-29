{# Remove os schemas criados pelo Slim CI de um PR (chamado ao fechar o PR).
   Uso: dbt run-operation drop_ci_schemas --target ci #}
{% macro drop_ci_schemas() %}
    {% if target.name != 'ci' %}
        {{ exceptions.raise_compiler_error("drop_ci_schemas só pode rodar com --target ci") }}
    {% endif %}
    {% for camada in ['bronze', 'silver', 'gold'] %}
        {% set schema_ci = target.schema ~ '_' ~ camada %}
        {% do log('Removendo schema ' ~ target.catalog ~ '.' ~ schema_ci, info=True) %}
        {% do run_query('drop schema if exists ' ~ target.catalog ~ '.' ~ schema_ci ~ ' cascade') %}
    {% endfor %}
{% endmacro %}
