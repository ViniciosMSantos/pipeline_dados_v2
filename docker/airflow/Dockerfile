FROM apache/airflow:3.3.1-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# FAB não vem mais por padrão no Airflow 3 (o auth manager padrão passou a
# ser o SimpleAuthManager); instalado explicitamente para manter o login
# fixo admin/admin via airflow users create.
RUN pip install --no-cache-dir apache-airflow-providers-fab
