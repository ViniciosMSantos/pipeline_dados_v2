# Plano de Implementação — Pipeline Modular com Airflow, dbt e Databricks

## 1. Objetivo

Construir uma arquitetura de dados modular e preparada para produção utilizando:

* **Databricks** — processamento e armazenamento dos dados;
* **dbt** — transformação, testes, documentação e dependências entre modelos;
* **Airflow** — orquestração e agendamento;
* **Docker** — padronização e isolamento dos ambientes;
* **uv** — gerenciamento das dependências Python do projeto;
* **Power BI** — camada de consumo e visualização.

O principal objetivo da arquitetura é permitir que **cada conjunto de dados possa ser executado, testado e reprocessado de forma independente**, evitando a necessidade de executar todo o pipeline quando apenas uma tabela ou modelo apresentar problema.

---

# 2. Princípio da arquitetura

A arquitetura deve seguir o princípio:

> **Airflow orquestra → dbt transforma → Databricks processa → Power BI consome.**

O Airflow não deve conter a lógica SQL das transformações.

O dbt deve ser responsável por:

* SQL;
* dependências entre modelos;
* testes;
* documentação;
* materializações;
* execução seletiva.

O Airflow deve ser responsável por:

* agendamento;
* execução;
* retry;
* monitoramento;
* dependências operacionais;
* alertas;
* reprocessamento.

---

# 3. Arquitetura final esperada

```text
                         ┌─────────────────────┐
                         │       AIRFLOW       │
                         │   Orquestração      │
                         └──────────┬──────────┘
                                    │
                 ┌──────────────────┼──────────────────┐
                 │                  │                  │
                 ▼                  ▼                  ▼
          DAG Customers       DAG Orders        DAG Products
                 │                  │                  │
                 └──────────────────┼──────────────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │        DBT          │
                         │ Transformações SQL  │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │     DATABRICKS      │
                         │                     │
                         │ Bronze → Silver     │
                         │          → Gold     │
                         └──────────┬──────────┘
                                    │
                                    ▼
                              ┌───────────┐
                              │ Power BI  │
                              └───────────┘
```

---

# 4. Estrutura de diretórios

A primeira etapa é organizar o projeto.

A estrutura recomendada é:

```text
pipeline_dados_v2/
│
├── airflow/
│   ├── dags/
│   │   ├── dbt_customers.py
│   │   ├── dbt_orders.py
│   │   ├── dbt_products.py
│   │   ├── dbt_sellers.py
│   │   ├── dbt_order_items.py
│   │   ├── dbt_order_payments.py
│   │   ├── dbt_order_reviews.py
│   │   └── dbt_geolocation.py
│   │
│   ├── logs/
│   └── plugins/
│
├── dbt_core/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   ├── models/
│   │   ├── sources/
│   │   │   └── sources.yml
│   │   │
│   │   ├── staging/
│   │   │   ├── customers/
│   │   │   │   ├── stg_customers.sql
│   │   │   │   └── schema.yml
│   │   │   │
│   │   │   ├── orders/
│   │   │   │   ├── stg_orders.sql
│   │   │   │   └── schema.yml
│   │   │   │
│   │   │   ├── products/
│   │   │   │   ├── stg_products.sql
│   │   │   │   └── schema.yml
│   │   │   │
│   │   │   └── sellers/
│   │   │       ├── stg_sellers.sql
│   │   │       └── schema.yml
│   │   │
│   │   ├── intermediate/
│   │   │
│   │   └── marts/
│   │
│   └── tests/
│
├── docker/
│   ├── airflow/
│   │   └── Dockerfile
│   │
│   └── dbt/
│       └── Dockerfile
│
├── docker-compose.yml
├── pyproject.toml
├── uv.lock
├── .env
├── .gitignore
└── README.md
```

---

# 5. Etapa 1 — Finalizar o projeto dbt

Antes de envolver Airflow e Docker, o dbt precisa funcionar sozinho.

Entre no projeto:

```bash
cd ~/pipeline_dados_v2/dbt_core
```

Verifique:

```bash
ls -la
```

Você deve ter pelo menos:

```text
dbt_project.yml
models/
```

Depois:

```bash
uv run dbt --version
```

---

# 6. Etapa 2 — Instalar o adapter do Databricks

O dbt precisa do adapter correspondente ao Databricks.

No projeto Python:

```bash
uv add dbt-databricks
```

Depois:

```bash
uv sync
```

Verifique:

```bash
uv run dbt --version
```

O adapter do Databricks deverá aparecer entre os plugins instalados.

---

# 7. Etapa 3 — Configurar a conexão com Databricks

Criar/configurar o `profiles.yml` para o ambiente do Databricks.

Exemplo conceitual:

```yaml
dbt_core:
  target: dev

  outputs:
    dev:
      type: databricks
      catalog: bhub_lakehouse_prd
      schema: silver

      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
```

As credenciais **não devem ser colocadas diretamente no Git**.

Utilizar variáveis de ambiente:

```text
DATABRICKS_HOST
DATABRICKS_HTTP_PATH
DATABRICKS_TOKEN
```

O `.env` deve estar no `.gitignore`.

---

# 8. Etapa 4 — Validar a conexão

Executar:

```bash
uv run dbt debug
```

O objetivo é chegar a algo equivalente a:

```text
Connection test: OK
```

Somente depois de o `dbt debug` funcionar deve-se continuar.

---

# 9. Etapa 5 — Criar as Sources

As tabelas Bronze devem ser declaradas como `sources`.

Arquivo:

```text
models/sources/sources.yml
```

Exemplo:

```yaml
version: 2

sources:
  - name: bronze
    database: bhub_lakehouse_prd
    schema: bronze

    tables:
      - name: brz_olist_customers

      - name: brz_olist_geolocation

      - name: brz_olist_order_items

      - name: brz_olist_order_payments

      - name: brz_olist_order_reviews

      - name: brz_olist_products

      - name: brz_olist_sellers

      - name: brz_orders

      - name: brz_product_category_name_translation
```

A partir disso, o dbt passa a referenciar as tabelas utilizando:

```sql
{{ source('bronze', 'brz_olist_customers') }}
```

---

# 10. Etapa 6 — Criar os modelos Staging

Cada tabela Bronze deve possuir seu modelo de tratamento.

Exemplo:

```text
brz_olist_customers
        │
        ▼
stg_customers
```

Arquivo:

```text
models/staging/customers/stg_customers.sql
```

Exemplo:

```sql
{{ config(
    materialized='table'
) }}

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('bronze', 'brz_olist_customers') }}
```

---

# 11. Etapa 7 — Criar testes

Cada modelo deve possuir testes.

Exemplo:

```yaml
version: 2

models:

  - name: stg_customers

    description: "Clientes tratados a partir da camada Bronze."

    columns:

      - name: customer_id
        tests:
          - not_null

      - name: customer_unique_id
        tests:
          - not_null
```

Os testes permitem identificar problemas automaticamente.

Por exemplo:

```text
stg_customers
      │
      ▼
TESTE customer_id NOT NULL
      │
      ├── PASSOU
      │
      └── FALHOU
```

---

# 12. Etapa 8 — Validar cada modelo individualmente

Essa etapa é fundamental para a arquitetura modular.

Para testar somente customers:

```bash
uv run dbt build --select stg_customers
```

Para orders:

```bash
uv run dbt build --select stg_orders
```

Para products:

```bash
uv run dbt build --select stg_products
```

Assim, cada modelo pode ser desenvolvido e corrigido individualmente.

---

# 13. Etapa 9 — Utilizar o grafo de dependências do dbt

O dbt deve controlar as dependências entre os modelos.

Por exemplo:

```text
Bronze
  │
  ▼
stg_orders
  │
  ▼
int_orders
  │
  ▼
fct_orders
```

Se for executado:

```bash
uv run dbt build --select stg_orders+
```

o dbt poderá executar o modelo e seus descendentes.

Isso é melhor do que colocar toda a lógica de dependência dentro do Airflow.

---

# 14. Etapa 10 — Criar a camada Intermediate

Quando houver necessidade de combinar ou preparar dados para os marts:

```text
Staging
   │
   ▼
Intermediate
   │
   ▼
Marts
```

Exemplo:

```text
stg_orders
      │
      ├──────────────┐
      ▼              ▼
stg_order_items   stg_payments
      │              │
      └──────┬───────┘
             ▼
       int_order_sales
             │
             ▼
         fct_orders
```

A camada Intermediate deve conter transformações intermediárias reutilizáveis.

---

# 15. Etapa 11 — Criar os Marts

A camada Gold/Marts será responsável pelos dados preparados para consumo analítico.

Exemplo:

```text
fct_orders
dim_customers
dim_products
dim_sellers
dim_date
```

Essas tabelas serão as principais candidatas ao consumo pelo Power BI.

---

# 16. Etapa 12 — Validar todo o projeto dbt

Depois de criar os modelos:

```bash
uv run dbt parse
```

Depois:

```bash
uv run dbt build
```

O `dbt build` deve executar:

* modelos;
* testes;
* dependências.

O objetivo é que o projeto esteja funcionando corretamente **sem Airflow**.

---

# 17. Etapa 13 — Containerizar o dbt

Depois que o dbt estiver funcionando localmente, criar o container.

Exemplo conceitual:

```text
Docker
│
└── dbt
     │
     ├── dbt_project.yml
     ├── models/
     ├── profiles.yml
     └── dependências
```

O container deve ser capaz de executar:

```bash
dbt build --select stg_customers
```

sem depender da instalação manual do computador.

---

# 18. Etapa 14 — Testar o container dbt

Antes de criar o Airflow:

```text
Docker
   │
   ▼
DBT
   │
   ▼
Databricks
```

Teste:

```bash
docker compose run --rm dbt dbt debug
```

Depois:

```bash
docker compose run --rm dbt dbt build --select stg_customers
```

Se funcionar, o dbt está operacional dentro do Docker.

---

# 19. Etapa 15 — Containerizar o Airflow

Criar o ambiente Airflow no Docker.

Estrutura:

```text
Docker Compose
│
├── Airflow Webserver
├── Airflow Scheduler
├── Airflow Metadata DB
└── dbt
```

O Airflow será responsável somente pela orquestração.

---

# 20. Etapa 16 — Criar o primeiro DAG

Não criar todos os DAGs imediatamente.

Começar com:

```text
dbt_customers.py
```

Fluxo:

```text
Airflow
   │
   ▼
Executa container dbt
   │
   ▼
dbt build --select stg_customers
   │
   ▼
Databricks
```

---

# 21. Etapa 17 — Criar DAGs modulares

Depois que o primeiro funcionar, criar os demais.

Exemplo:

```text
airflow/dags/

├── dbt_customers.py
├── dbt_orders.py
├── dbt_order_items.py
├── dbt_order_payments.py
├── dbt_order_reviews.py
├── dbt_products.py
├── dbt_sellers.py
└── dbt_geolocation.py
```

Cada DAG será responsável por um domínio/modelo.

---

# 22. Etapa 18 — Exemplo de execução modular

Imagine que:

```text
customers       → OK
products        → OK
sellers         → OK
orders          → ERRO
payments        → OK
reviews         → OK
```

Não será necessário executar tudo novamente.

O Airflow poderá executar somente:

```text
DAG orders
    │
    ▼
dbt build --select stg_orders+
```

Assim:

```text
orders
  │
  ▼
stg_orders
  │
  ▼
int_orders
  │
  ▼
fct_orders
```

serão reprocessados conforme a necessidade.

---

# 23. Etapa 19 — Implementar retries

Cada DAG deve possuir política de retry.

Exemplo conceitual:

```text
Execução
   │
   ▼
Falhou
   │
   ▼
Retry 1
   │
   ▼
Falhou
   │
   ▼
Retry 2
   │
   ▼
Falhou
   │
   ▼
Alerta
```

Isso evita que falhas temporárias derrubem o processo definitivamente.

---

# 24. Etapa 20 — Implementar alertas

Quando um DAG falhar:

```text
Airflow
   │
   ▼
DAG FAILED
   │
   ├── Log
   ├── Retry
   └── Alerta
```

O objetivo é permitir identificar rapidamente:

* qual DAG falhou;
* qual modelo falhou;
* qual teste falhou;
* qual foi o erro;
* quando ocorreu;
* quantas tentativas foram realizadas.

---

# 25. Etapa 21 — Separar desenvolvimento e produção

Criar pelo menos dois ambientes:

```text
DEV
 │
 ├── Desenvolvimento
 ├── Testes
 └── Validação

PROD
 │
 ├── Execução oficial
 ├── Dados oficiais
 └── Power BI
```

Nunca utilizar credenciais de produção diretamente no desenvolvimento.

---

# 26. Etapa 22 — Controle de versões

Tudo que for código deve estar no Git:

```text
dbt
Airflow
Docker
SQL
YAML
README
configurações não sensíveis
```

Não versionar:

```text
.env
tokens
senhas
credenciais
segredos
```

O `.gitignore` deve contemplar esses arquivos.

---

# 27. Etapa 23 — CI/CD

Depois que a arquitetura estiver funcionando, implementar CI/CD.

Fluxo:

```text
Developer
    │
    ▼
Git Push
    │
    ▼
GitHub
    │
    ▼
CI
    │
    ├── dbt parse
    ├── dbt build/test
    └── validações
    │
    ▼
Deploy
    │
    ▼
Produção
```

O objetivo é impedir que código quebrado seja enviado diretamente para produção.

---

# 28. Etapa 24 — Criar uma execução completa

Mesmo com arquitetura modular, deve existir uma forma de executar o pipeline completo.

Por exemplo:

```text
DAG: olist_production
```

Fluxo:

```text
customers ──────┐
                │
products ───────┤
                │
sellers ────────┤
                ▼
             orders
                │
       ┌────────┼────────┐
       ▼        ▼        ▼
    items    payments   reviews
       │        │        │
       └────────┼────────┘
                ▼
              marts
```

Isso permite executar o processamento completo de produção quando necessário.

---

# 29. Arquitetura operacional final

A arquitetura deverá permitir **dois modos de operação**.

## Execução completa

```text
Airflow
   │
   ▼
Pipeline completo
   │
   ▼
DBT
   │
   ▼
Databricks
```

Utilizado para a execução normal programada.

---

## Execução isolada

```text
Airflow
   │
   ▼
DAG Orders
   │
   ▼
DBT
   │
   ▼
stg_orders+
   │
   ▼
Databricks
```

Utilizado para:

* correção de erro;
* reprocessamento;
* desenvolvimento;
* manutenção;
* recuperação de dados.

---

# 30. Regra fundamental da arquitetura

A responsabilidade de cada componente deve ficar bem definida:

| Componente | Responsabilidade              |
| ---------- | ----------------------------- |
| Databricks | Processamento e armazenamento |
| Bronze     | Dados brutos                  |
| dbt        | Transformação e testes        |
| Airflow    | Orquestração                  |
| Docker     | Ambiente de execução          |
| Git/GitHub | Versionamento                 |
| CI/CD      | Validação e deploy            |
| Power BI   | Consumo analítico             |

Evitar colocar SQL de transformação diretamente nos DAGs do Airflow.

O Airflow deve dizer:

```text
"Execute o modelo X"
```

e não:

```text
"Execute este SQL gigantesco."
```

---

# 31. Ordem recomendada de implementação

Não implementar tudo simultaneamente.

A ordem recomendada é:

```text
1. uv
   ↓
2. dbt
   ↓
3. dbt-databricks
   ↓
4. Conexão Databricks
   ↓
5. Sources
   ↓
6. Staging
   ↓
7. Testes
   ↓
8. Intermediate
   ↓
9. Marts
   ↓
10. dbt build funcionando
   ↓
11. Docker para dbt
   ↓
12. Airflow
   ↓
13. Primeiro DAG
   ↓
14. DAGs modulares
   ↓
15. Retry + logs + alertas
   ↓
16. DAG completo
   ↓
17. CI/CD
   ↓
18. Produção
```

---

# 32. Critério para considerar o projeto pronto

O projeto pode ser considerado operacionalmente maduro quando for possível fazer:

### Execução normal

```bash
dbt build
```

ou executar o DAG completo pelo Airflow.

### Execução específica

```bash
dbt build --select stg_customers
```

### Execução com dependências

```bash
dbt build --select stg_orders+
```

### Reprocessamento

Executar novamente somente o DAG/modelo que apresentou problema.

### Monitoramento

Visualizar no Airflow:

```text
SUCCESS
FAILED
RUNNING
RETRY
```

### Rastreabilidade

Conseguir identificar:

```text
DAG
 ↓
Task
 ↓
dbt model
 ↓
Databricks
 ↓
Tabela
 ↓
Teste
```

---

# 33. Resultado esperado

Ao final, o projeto terá uma arquitetura semelhante a:

```text
                         GITHUB
                            │
                            ▼
                         CI/CD
                            │
                            ▼
                    ┌───────────────┐
                    │    AIRFLOW    │
                    │ Orquestração  │
                    └───────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
          Customers       Orders       Products
              │             │             │
              └─────────────┼─────────────┘
                            ▼
                     ┌────────────┐
                     │    DBT     │
                     │ Transform. │
                     └─────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  DATABRICKS │
                    └──────┬──────┘
                           │
                    Bronze → Silver
                           │
                           ▼
                          Gold
                           │
                           ▼
                       POWER BI
```

O principal ganho dessa arquitetura é a **resiliência operacional**: uma falha em `orders` não precisa obrigatoriamente provocar a execução novamente de `customers`, `products`, `sellers`, `reviews` etc.

O pipeline passa a ser **modular, observável, reprocessável e escalável**, características importantes de uma arquitetura de dados orientada para produção.

## Próximo passo recomendado

Não comece pelo Airflow ainda.

A próxima implementação deve ser:

```text
dbt
 ↓
Databricks
 ↓
sources.yml
 ↓
stg_customers
 ↓
schema.yml
 ↓
dbt build --select stg_customers
```

Depois que **`stg_customers` funcionar corretamente de ponta a ponta**, replicamos o padrão para as demais tabelas. Só então partimos para Docker e Airflow.
