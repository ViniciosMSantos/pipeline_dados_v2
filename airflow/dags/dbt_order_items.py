from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dbt_order_items",
    description="Builda e testa o modelo slv_itens_pedidos (camada silver) via container dbt.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "silver", "order_items"],
) as dag:
    build_slv_itens_pedidos = BashOperator(
        task_id="dbt_build_slv_itens_pedidos",
        bash_command=(
            "docker run --rm "
            '-e DBT_DATABRICKS_TOKEN="$DBT_DATABRICKS_TOKEN" '
            '-v "$DBT_CORE_HOST_PATH":/usr/app '
            '-v "$DBT_PROFILES_HOST_PATH":/root/.dbt '
            "pipeline-dbt:1.0 build --target prod --select slv_itens_pedidos"
        ),
    )