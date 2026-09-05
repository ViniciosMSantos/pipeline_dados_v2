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
   source .venv/bin/activate
   ```

3. Configure o arquivo de credenciais **`~/.dbt/profiles.yml`** (fica fora do repositório, nunca deve ser commitado). O nome do profile precisa bater com o campo `profile:` do [dbt_project.yml](dbt_project.yml) (`pipeline_dados`):
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
   export DBT_DATABRICKS_TOKEN="<personal access token>"      # target dev
   export DBX_SP_CLIENT_ID="<client id do service principal>"  # target prod
   export DBX_SP_CLIENT_SECRET="<client secret do service principal>"  # target prod
   ```

5. Instale os pacotes declarados em [packages.yml](packages.yml) (ex.: `dbt_utils`):
   ```bash
   dbt deps
   ```

6. Teste a conexão com o Databricks:
   ```bash
   dbt debug
   ```

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

O workflow [.github/workflows/databricks-ci.yml](.github/workflows/databricks-ci.yml) roda a cada push na branch `main`: instala o `dbt-databricks`, roda `dbt deps` e depois `dbt run --profiles-dir . --target prod`.

> ⚠️ **Atenção**: esse workflow, do jeito que está hoje, não deve funcionar. Ele usa `--profiles-dir .` (ou seja, espera um `profiles.yml` dentro do repositório), mas o `profiles.yml` foi removido do versionamento (corretamente, por conter credenciais) e não existe no repo. Além disso, os secrets usados no workflow (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`) não correspondem às variáveis que o `profiles.yml` local espera para o target `prod` (`DBX_SP_CLIENT_ID` e `DBX_SP_CLIENT_SECRET`, com host/http_path fixos no arquivo). Antes de confiar nesse pipeline, é preciso gerar um `profiles.yml` dentro do job (por exemplo, com um passo que escreve o arquivo a partir dos secrets do GitHub) e alinhar os nomes das variáveis de ambiente.

### Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
