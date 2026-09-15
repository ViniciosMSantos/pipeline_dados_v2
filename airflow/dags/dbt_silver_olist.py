from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

SILVER_MODELS = [
    "slv_olist_avaliacao",
    "slv_olist_cidades",
    "slv_olist_clientes",
    "slv_olist_itens_pedidos",
    "slv_olist_pagamentos",
    "slv_olist_pedidos",
    "slv_olist_produtos",
    "slv_olist_vendedores",
]

with DAG(
    dag_id="dbt_silver_olist",
    description="Builda e testa os models da camada silver via container dbt.",
    start_date=datetime(2026, 1, 1),
    schedule="0 10 * * *",
    catchup=False,
    tags=["dbt", "silver"],
) as dag:
    for model in SILVER_MODELS:
        BashOperator(
            task_id=f"dbt_build_{model}",
            bash_command=(
                "docker run --rm "
                '-e DBT_DATABRICKS_TOKEN="$DBT_DATABRICKS_TOKEN" '
                '-v "$DBT_CORE_HOST_PATH":/usr/app '
                '-v "$DBT_PROFILES_HOST_PATH":/root/.dbt '
                f"pipeline-dbt:1.0 build --target prod --select {model}"
            ),
        )
