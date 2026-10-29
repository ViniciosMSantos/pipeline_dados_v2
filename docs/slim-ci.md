# dbt Slim CI

Em cada PR para a `master` o workflow `dbt-slim-ci.yml` roda:

```
dbt build --target ci --select state:modified+ --defer --state /state --empty
```

- `state:modified+`: só os models alterados e seus dependentes.
- `--defer --state`: o que não mudou é lido da produção (manifest gerado no deploy).
- `--empty`: o SQL roda no Databricks com 0 linhas (valida sintaxe, colunas e joins;
  não valida qualidade dos dados).
- Cada PR escreve em `dados_dev.ci_pr_<N>_silver|gold` (macro `generate_schema_name`).
  Ao fechar o PR, o macro `drop_ci_schemas` remove esses schemas.

## Setup único no servidor (self-hosted runner)

1. Adicionar o target `ci` em `~/.dbt/profiles.yml`:

```yaml
    ci:
      type: databricks
      catalog: dados_dev
      schema: "{{ env_var('DBT_CI_SCHEMA', 'ci_local') }}"
      host: dbc-bd51087c-3c7d.cloud.databricks.com
      http_path: /sql/1.0/warehouses/df426e47607ac190
      token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
      threads: 4
```

2. Fazer um push na `master` para o deploy gerar `~/dbt-state/manifest.json`.
   Enquanto ele não existir, o CI roda só `dbt parse`.
3. O `.env` do servidor deve ter `DBT_DATABRICKS_TOKEN` (e `DBX_SP_CLIENT_ID/SECRET`
   se o target `prod` usar OAuth).
