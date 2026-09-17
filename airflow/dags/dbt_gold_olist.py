from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset

MODELOS_GOLD = [
    "gld_olist_pedidos",
    "gld_olist_avaliacoes",
    "gld_olist_pagamentos",
    "gld_olist_itens_pedidos"
]

# Assets (tabelas silver) que o gld_olist_pedidos consome via ref() no dbt.
# Precisam bater com os outlets publicados pela dbt_silver_olist.
SILVER_DEPS_GOLD = [
    Asset("olist://silver/slv_olist_pedidos"),
    Asset("olist://silver/slv_olist_clientes"),
    Asset("olist://silver/slv_olist_itens_pedidos"),
    Asset("olist://silver/slv_olist_avaliacao"),
    Asset("olist://silver/slv_olist_pagamentos"),
    Asset("olist://silver/slv_olist_produtos"),
    Asset("olist://silver/slv_olist_vendedores"),

]

with DAG(
    dag_id="dbt_gold_olist",
    description="Builda e testa os models da camada gold via container dbt.",
    start_date=datetime(2026, 1, 1),
    # Dispara somente depois que todas as tabelas silver usadas pelos models
    # gold tiverem sido atualizadas (AND) pela dbt_silver_olist, em vez de um
    # horário fixo que não garante que a silver já rodou.
    schedule=SILVER_DEPS_GOLD,
    catchup=False,
    tags=["dbt", "gold"],
    max_active_tasks=1,   ## Roda uma task por vez para não saturar a cpu
) as dag:
    for model in MODELOS_GOLD:
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
