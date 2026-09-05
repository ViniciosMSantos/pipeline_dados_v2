FROM apache/airflow:2.11.2-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow
