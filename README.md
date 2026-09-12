# pipeline_dados_v2

Pipeline de dados modular para o e-commerce **Olist**, construído com **Databricks**, **dbt**, **Airflow** e **Docker**, com **Power BI** como camada de consumo.

O plano de implementação completo está em [doc_projeto.md](doc_projeto.md).

## Arquitetura

```text
Airflow (orquestração) → dbt (transformação/testes) → Databricks (Bronze → Silver → Gold) → Power BI (consumo)
```

Princípio: o Airflow não contém lógica SQL. Ele apenas aciona o dbt, que é responsável por SQL, dependências entre modelos, testes, documentação e materializações. Isso permite reprocessar um domínio (ex.: `orders`) sem precisar rodar o pipeline inteiro novamente.

## Estrutura do repositório

```text
pipeline_dados_v2/
├── dbt_core/          # projeto dbt (models, sources, macros, seeds, snapshots, tests)
├── airflow/
│   ├── dags/          # DAGs (ex.: dbt_customers.py)
│   ├── logs/          # logs do Airflow (gerado em runtime, não versionado)
│   └── plugins/
├── docker/
│   ├── dbt/           # Dockerfile da imagem dbt (dbt-core + dbt-databricks)
│   └── airflow/       # Dockerfile da imagem do Airflow (+ Docker CLI)
├── docker-compose.yml # sobe Postgres + Airflow (api-server/scheduler/dag-processor)
├── .env.example       # template de variáveis (copiar para .env, não versionado)
├── .github/workflows/ # CI: builda a imagem dbt e valida a versão
└── doc_projeto.md     # plano de implementação detalhado, etapa a etapa
```

Detalhes do projeto dbt (camadas, convenções de nome, configuração de credenciais, comandos) estão em [dbt_core/README.md](dbt_core/README.md).

## Versionamento

Repositório: [github.com/ViniciosMSantos/pipeline_dados_v2](https://github.com/ViniciosMSantos/pipeline_dados_v2) (privado).

- `master` deve sempre refletir um estado funcional do projeto; trabalho em andamento vai em branch `feature/<descrição>` e chega em `master` via Pull Request.
- Commits seguem [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`).
- Marcos do projeto (ex.: "dbt funcionando sozinho") são marcados com tags (`git tag -a v0.1.0 -m "..."`).
- Nunca versionar `.env`, tokens, senhas ou `profiles.yml` — ver [.gitignore](.gitignore).

Detalhes completos da estratégia de versionamento estão em [doc_projeto.md](doc_projeto.md#26-etapa-22--controle-de-versões-e-github).

## Status atual

- ✅ Projeto dbt inicializado, com `sources` da camada Bronze declaradas ([dbt_core/models/bronze/source_silver.yml](dbt_core/models/bronze/source_silver.yml)).
- ✅ Primeiro modelo Silver implementado: `slv_clientes` (com testes `not_null`).
- ✅ Dockerfile do dbt, containerizado e validado contra o Databricks.
- ✅ Stack de orquestração no ar: Airflow 3.3.1 (api-server + scheduler + dag-processor + Postgres) via `docker-compose.yml`, com o primeiro DAG (`dbt_customers`) rodando o container dbt de ponta a ponta.
- ✅ CI básico no GitHub Actions (builda a imagem dbt).
- ⏳ Pendente: demais modelos Silver, camada Gold, mais testes dbt, mais DAGs, retries/alertas, separação formal dev/prod, CI/CD rodando `dbt build`/`test` de fato.

## Como rodar

### 1. Só o dbt (mais rápido, sem Airflow)

```bash
cd dbt_core
uv sync
uv run dbt debug
uv run dbt build --select slv_clientes
```

### 2. Stack completa (Docker + Airflow orquestrando o dbt)

Pré-requisitos: Docker e Docker Compose instalados, e um `~/.dbt/profiles.yml` configurado (ver [dbt_core/README.md](dbt_core/README.md)).

1. **Construir a imagem do dbt** (usada pelo Airflow para rodar os models):
   ```bash
   docker build -t pipeline-dbt:1.0 -f docker/dbt/Dockerfile .
   ```

2. **Criar o `.env`** a partir do template e preencher os valores:
   ```bash
   cp .env.example .env
   ```
   No `.env`, ajuste:
   - `AIRFLOW_UID` → resultado de `id -u`
   - `DOCKER_GID` → resultado de `getent group docker | cut -d: -f3`
   - `DBT_DATABRICKS_TOKEN` → token do Databricks (target `dev`)
   - `DBT_CORE_HOST_PATH` → caminho absoluto de `dbt_core` **no host** (ex.: `$(pwd)/dbt_core`)
   - `DBT_PROFILES_HOST_PATH` → caminho absoluto do `~/.dbt` **no host**
   - `AIRFLOW_JWT_SECRET` → segredo aleatório usado pela Task Execution API do Airflow 3 (gere com `python3 -c "import secrets; print(secrets.token_hex(32))"`)

   > As tasks do Airflow rodam `docker run` contra o daemon do **host** (docker-outside-of-docker), por isso os dois últimos caminhos precisam ser reais do host, não do container do Airflow.

3. **Subir a stack**:
   ```bash
   docker compose up -d --build
   ```
   Isso sobe, nessa ordem: `postgres` (metadata do Airflow) → `airflow-init` (migra o banco e cria o usuário admin, depois encerra) → `airflow-api-server`, `airflow-scheduler` e `airflow-dag-processor`.

4. **Acompanhar até ficar saudável**:
   ```bash
   docker compose ps
   ```
   Espere `postgres` e `airflow-api-server` aparecerem como `healthy`.

5. **Acessar a UI do Airflow**: [http://localhost:8080](http://localhost:8080)
   Login padrão: `admin` / `admin` (ou os valores definidos em `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` no `.env`).

6. **Ativar e disparar o DAG** `dbt_customers` (pela UI, ou via CLI):
   ```bash
   docker compose exec airflow-scheduler airflow dags unpause dbt_customers
   docker compose exec airflow-scheduler airflow dags trigger dbt_customers
   ```

7. **Ver os logs da task**: pela UI (Grid → task → Logs) ou em `airflow/logs/dag_id=dbt_customers/...`.

8. **Derrubar a stack** quando terminar:
   ```bash
   docker compose down
   ```
   (os dados do Postgres ficam no volume `postgres-db-volume`; use `docker compose down -v` só se quiser apagar o histórico do Airflow também).

Credenciais do Databricks ficam em `~/.dbt/profiles.yml` e no `.env` (ambos fora do repositório) — nunca commitar tokens, senhas ou esses arquivos.

### 3. Deploy automático

Todo push/merge na `master` dispara o workflow [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml), que roda em um **self-hosted runner do GitHub Actions** instalado nesta própria máquina (`~/actions-runner`, serviço systemd `actions.runner.ViniciosMSantos-pipeline_dados_v2.*`). O workflow:

1. Sincroniza `/home/vinicios_santos/pipeline_dados_v2` com `origin/master` via `git reset --hard` — **qualquer edição feita direto nesta máquina, fora do Git, é descartada no próximo deploy**.
2. Rebuilda a imagem `pipeline-dbt:1.0`.
3. Roda `docker compose up -d --build` para atualizar os serviços do Airflow.

Comandos úteis:
```bash
# Ver status do runner
sudo systemctl status "actions.runner.ViniciosMSantos-pipeline_dados_v2.*"

# Reinstalar o runner do zero (ex.: máquina reformatada)
# 1. Gere um token em Settings > Actions > Runners > New self-hosted runner no GitHub
# 2. mkdir -p ~/actions-runner && cd ~/actions-runner
# 3. Baixe e extraia o pacote linux-x64 da release mais recente de actions/runner
# 4. ./config.sh --url https://github.com/ViniciosMSantos/pipeline_dados_v2 --token <TOKEN>
# 5. sudo ./svc.sh install && sudo ./svc.sh start
```
