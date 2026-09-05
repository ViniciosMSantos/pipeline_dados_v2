FROM python:3.11-slim

WORKDIR /usr/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==1.12.3 \
    dbt-databricks==1.12.5

COPY dbt_core/ /usr/app/

ENTRYPOINT ["dbt"]