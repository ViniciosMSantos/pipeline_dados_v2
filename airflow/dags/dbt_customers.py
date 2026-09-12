from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dbt_customers",
    description="Builda e testa o modelo slv_clientes (camada silver) via container dbt.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt", "silver", "customers"],
) as dag:
    build_slv_clientes = BashOperator(
        task_id="dbt_build_slv_clientes",
        bash_command=(
            "docker run --rm "
            '-e DBT_DATABRICKS_TOKEN="$DBT_DATABRICKS_TOKEN" '
            '-v "$DBT_CORE_HOST_PATH":/usr/app '
            '-v "$DBT_PROFILES_HOST_PATH":/root/.dbt '
            "pipeline-dbt:1.0 build --target prod --select slv_clientes"
        ),
    )
