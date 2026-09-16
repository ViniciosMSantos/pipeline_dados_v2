from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset

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
    schedule="0 14 * * *",
    catchup=False,
    tags=["dbt", "silver"],
    # Cada task sobe um container `docker run` isolado (dbt debug + build);
    # rodar as 8 em paralelo satura a CPU do host (WSL2 com 4 vCPUs) e o
    # heartbeat da task atrasa o suficiente pro Airflow matar com SIGTERM
    # antes do dbt terminar. Limita quantas rodam ao mesmo tempo.
    max_active_tasks=1,
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
            # Publica o Asset para que DAGs downstream (ex: dbt_gold_olist)
            # possam ser agendadas a partir da atualização desta tabela.
            outlets=[Asset(f"olist://silver/{model}")],
        )
