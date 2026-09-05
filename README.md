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
├── docker/dbt/        # Dockerfile da imagem dbt (dbt-core + dbt-databricks)
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
- ✅ Primeiro modelo Silver implementado: `slv_clientes`.
- ✅ Dockerfile do dbt e CI básico no GitHub Actions.
- ⏳ Pendente: demais modelos Silver, camada Gold, testes dbt, Airflow (DAGs), `docker-compose.yml` e integração real com Databricks.

## Como rodar

```bash
cd dbt_core
uv sync
uv run dbt debug
uv run dbt build --select slv_clientes
```

Credenciais do Databricks ficam em `~/.dbt/profiles.yml` (fora do repositório) — nunca commitar tokens ou o arquivo `.env`.
