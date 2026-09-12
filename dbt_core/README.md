# Databricks_dbt

Treinamento com DBT — pipeline de transformação de dados do e-commerce **Olist** rodando no **Databricks**, usando arquitetura em camadas (Bronze → Silver → Gold).

## Sobre o projeto

Este projeto usa o [dbt](https://docs.getdbt.com/) (data build tool) para transformar dados dentro do Databricks via SQL, seguindo a arquitetura medallion:

- **Bronze**: dados brutos, já carregados no Databricks fora do dbt. O dbt apenas os declara como `source` em [models/bronze/source_silver.yml](models/bronze/source_silver.yml).
- **Silver**: dados tratados/padronizados a partir da Bronze (ex.: [models/silver/slv_clientes.sql](models/silver/slv_clientes.sql)).
- **Gold**: modelos de negócio, prontos para consumo (dashboards, análises), a partir da Silver.

## Estrutura do projeto

```
dbt_core/
├── models/
│   ├── bronze/       # sources (yml) apontando para as tabelas brutas no Databricks
│   ├── silver/        # modelos .sql + .yml de dados tratados
│   └── gold/          # modelos .sql de negócio/agregados
├── macros/             # macros Jinja reutilizáveis (ex.: generate_schema_name)
├── seeds/              # arquivos .csv versionados e carregados via `dbt seed`
├── snapshots/          # snapshots (SCD type 2) de tabelas mutáveis
├── tests/              # testes customizados (singular tests)
├── analyses/           # queries auxiliares, não materializadas
├── dbt_project.yml     # configuração principal do projeto
├── packages.yml        # dependências de pacotes dbt (ex.: dbt_utils)
├── pyproject.toml      # dependências Python do projeto (gerenciado com uv)
└── .github/workflows/  # pipeline de CI/CD para deploy no Databricks (prod)
```

### Convenção de nomes

- Cada modelo `.sql` deve ter o **mesmo nome** do arquivo `.yml` que o documenta (ex.: `slv_clientes.sql` + entrada `name: slv_clientes` em `slv_clientes.yml`). O dbt identifica um modelo pelo nome do arquivo, então nomes divergentes (ex.: usar `.` em vez de `_`) fazem a documentação do `.yml` não ser aplicada ao modelo.
- Prefixos por camada: `brz_` (bronze, fonte externa), `slv_` (silver), sufixo `_gold` (gold).
- Cada camada é materializada em seu próprio schema no Databricks (`bronze`, `silver`, `gold`), configurado em [dbt_project.yml](dbt_project.yml).

## Pré-requisitos

- Python 3.12 (ver [.python-version](.python-version))
- [uv](https://docs.astral.sh/uv/) para gerenciar o ambiente virtual e as dependências
- Acesso a um SQL Warehouse no Databricks (host, http_path e um token ou service principal)

## Configuração do ambiente

1. Instale as dependências do projeto, incluindo o `dbt-databricks` (cria o `.venv` automaticamente):
   ```bash
   uv sync
   ```

2. Ative o ambiente virtual:
   ```bash
   # Linux/macOS
   source .venv/bin/activate
   ```
   ```powershell
   # Windows (PowerShell)
   .venv\Scripts\Activate.ps1
   ```

3. Configure o arquivo de credenciais **`~/.dbt/profiles.yml`** (Windows: `%USERPROFILE%\.dbt\profiles.yml`, ou seja `C:\Users\<usuário>\.dbt\profiles.yml`) — fica fora do repositório, nunca deve ser commitado. O nome do profile precisa bater com o campo `profile:` do [dbt_project.yml](dbt_project.yml) (`pipeline_dados`):
   ```yaml
   pipeline_dados:
     target: dev
     outputs:
       dev:
         type: databricks
         catalog: dados_dev
         schema: default
         host: <seu-host>.cloud.databricks.com
         http_path: /sql/1.0/warehouses/<warehouse_id>
         token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
         threads: 4

       prod:
         type: databricks
         catalog: dados_prod
         schema: default
         host: <seu-host>.cloud.databricks.com
         http_path: /sql/1.0/warehouses/<warehouse_id>
         auth_type: oauth-m2m
         client_id: "{{ env_var('DBX_SP_CLIENT_ID') }}"
         client_secret: "{{ env_var('DBX_SP_CLIENT_SECRET') }}"
         threads: 4
   ```

4. Exporte as variáveis de ambiente usadas acima antes de rodar o dbt (ajuste conforme o target usado):
   ```bash
   # Linux/macOS
   export DBT_DATABRICKS_TOKEN="<personal access token>"      # target dev
   export DBX_SP_CLIENT_ID="<client id do service principal>"  # target prod
   export DBX_SP_CLIENT_SECRET="<client secret do service principal>"  # target prod
   ```
   ```powershell
   # Windows (PowerShell) — só vale para a sessão atual do terminal
   $env:DBT_DATABRICKS_TOKEN = "<personal access token>"      # target dev
   $env:DBX_SP_CLIENT_ID = "<client id do service principal>"  # target prod
   $env:DBX_SP_CLIENT_SECRET = "<client secret do service principal>"  # target prod
   ```

5. Instale os pacotes declarados em [packages.yml](packages.yml) (ex.: `dbt_utils`):
   ```bash
   dbt deps
   ```

6. Teste a conexão com o Databricks:
   ```bash
   dbt debug
   ```

> ⚠️ **Bronze é fixa em prod**: a source declarada em [models/bronze/source_silver.yml](models/bronze/source_silver.yml) usa `database: dados_prod` fixo, independente do `target` ativo — a Bronze é carregada fora do dbt e só existe no catálogo de produção. Isso é intencional: rodar com `--target dev` não cria uma cópia da Bronze em dev, apenas faz o dbt **ler** os dados brutos de prod e **escrever** as tabelas Silver/Gold no catálogo `dados_dev`, isolado de produção. É o suficiente para testar a criação/lógica das tabelas sem arriscar o schema de prod.

## Ambiente de teste (dev)

Use o target `dev` sempre que for testar a criação/alteração de um modelo antes de liberar para produção. Ele lê a Bronze de `dados_prod` (ver aviso acima) e materializa Silver/Gold em `dados_dev`, isolado do catálogo de produção.

### Checklist para testar do zero (primeira vez numa máquina nova)

1. `cd dbt_core`
2. `uv sync` — cria o `.venv` e instala as dependências Python (dbt-core, dbt-databricks etc.).
3. Ative o `.venv` (comando do passo 2 de [Configuração do ambiente](#configuração-do-ambiente) acima, conforme o SO).
4. Crie `~/.dbt/profiles.yml` (Windows: `%USERPROFILE%\.dbt\profiles.yml`) com os targets `dev`/`prod` — modelo no passo 3 de [Configuração do ambiente](#configuração-do-ambiente).
5. Exporte `DBT_DATABRICKS_TOKEN` (passo 4 acima) — sem isso o `profiles.yml` não resolve o `env_var(...)` e a conexão falha.
6. `dbt deps` — instala os pacotes de [packages.yml](packages.yml) (ex.: `dbt_utils`) em `dbt_packages/` (pasta local, não versionada; sem isso o `dbt build` falha com `Compilation Error: ... package(s) not found`).
7. `dbt debug` — deve terminar em `All checks passed!`. Se falhar aqui, o problema é credencial/host/http_path, não modelo.
8. `dbt build --target dev --select <nome_do_modelo>` — testa só o modelo em questão antes de rodar o projeto inteiro.

> Os passos 2–6 só precisam ser refeitos quando o ambiente for recriado (máquina nova, `.venv` apagado) ou as dependências mudarem. No dia a dia, normalmente só os passos 3 (se abriu um terminal novo), 5 e 8 são necessários.

### Opção 1 — local (uv/dbt CLI)

Com o ambiente configurado (seção anterior) e `DBT_DATABRICKS_TOKEN` exportado:

```bash
dbt build --target dev --select slv_clientes   # roda seed+run+test só desse modelo
dbt build --target dev                         # roda o projeto inteiro em dev
```

`dev` é o `target` padrão do `profiles.yml` do exemplo acima, então basta omitir `--target dev` se ele já for o `target:` default do seu profile.

### Opção 2 — via Docker (mesmo fluxo do Airflow)

Reproduz localmente o mesmo container que as DAGs (`airflow/dags/dbt_orders.py`, `dbt_customers.py`) rodam em prod, só trocando o target:

```bash
# 1. Build da imagem (uma vez, ou sempre que o Dockerfile/dbt_core mudar)
docker build -t pipeline-dbt:1.0 -f docker/dbt/Dockerfile .

# 2. Rodar build em dev contra o mesmo profiles.yml usado localmente
docker run --rm \
  -e DBT_DATABRICKS_TOKEN="$DBT_DATABRICKS_TOKEN" \
  -v "$(pwd)/dbt_core":/usr/app \
  -v "$HOME/.dbt":/root/.dbt \
  pipeline-dbt:1.0 build --target dev --select slv_clientes
```

Ajuste os caminhos dos `-v` se o `profiles.yml` ou o `dbt_core/` não estiverem nesses locais (são os mesmos `DBT_CORE_HOST_PATH`/`DBT_PROFILES_HOST_PATH` usados pelo `docker-compose.yml` do Airflow — ver [.env.example](../.env.example)). No Windows, troque `$HOME` por `%USERPROFILE%` (cmd) ou `$env:USERPROFILE` (PowerShell).

## Comandos principais do dbt

| Comando | O que faz |
|---|---|
| `dbt debug` | Valida a conexão com o Databricks e a configuração do projeto |
| `dbt deps` | Instala os pacotes listados em `packages.yml` |
| `dbt seed` | Carrega os `.csv` de `seeds/` como tabelas |
| `dbt run` | Executa (materializa) os modelos |
| `dbt run --select slv_clientes` | Executa apenas um modelo específico |
| `dbt run --select silver.*` | Executa todos os modelos de uma pasta/camada |
| `dbt run --select slv_clientes+` | Executa o modelo e tudo que depende dele (downstream) |
| `dbt test` | Roda os testes definidos nos `.yml` (ex.: `not_null`, `unique`) |
| `dbt build` | Roda `seed` + `run` + `test` (+ snapshots) na ordem correta do DAG |
| `dbt docs generate` | Gera a documentação do projeto |
| `dbt docs serve` | Sobe um servidor local para visualizar a documentação/gráfico de linhagem |
| `dbt run --target prod` | Executa usando o target `prod` do `profiles.yml` (schema/catálogo de produção) |
| `dbt clean` | Remove `target/` e `dbt_packages/` |

## CI/CD

- [.github/workflows/dbt-ci.yml](../.github/workflows/dbt-ci.yml): roda em push/PR para `master`. Só builda a imagem `pipeline-dbt:1.0` (`docker build -f docker/dbt/Dockerfile .`) e confere `dbt --version` dentro dela — **não** conecta no Databricks nem roda modelos, então não substitui testar em `dev` antes de mergear (ver seção [Ambiente de teste (dev)](#ambiente-de-teste-dev) acima).
- [.github/workflows/deploy.yml](../.github/workflows/deploy.yml): roda em push para `master`, em um runner self-hosted (`pipeline-dados`). Sincroniza a working tree com `origin/master` (`git reset --hard`), reconstrói a imagem `pipeline-dbt:1.0` e sobe a stack do Airflow (`docker compose up -d --build`) — é o Airflow dessa stack (DAGs `dbt_orders`/`dbt_customers`, `--target prod`) quem de fato materializa em produção, não o workflow do GitHub em si.

> ⚠️ Como nenhum dos dois workflows roda `dbt build`/`dbt test` contra o Databricks, um erro de modelo (SQL, teste, referência quebrada) só aparece quando a DAG do Airflow rodar em prod. Rode `dbt build --target dev` (ou a variante Docker acima) localmente antes de mergear para `master`.

### Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
