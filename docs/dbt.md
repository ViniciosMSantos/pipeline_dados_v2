# Guia de dbt — comandos e conceitos

Referência de dbt (data build tool) aplicada a este projeto: transformação SQL em cima do Databricks, seguindo a arquitetura medallion (`bronze` → `silver` → `gold`) usada em [dbt_core/models/](../dbt_core/models/). Para setup do ambiente (profiles.yml, uv, variáveis), ver [dbt_core/README.md](../dbt_core/README.md) — este documento foca em **o que dá para fazer com o dbt** e **como cada comando funciona**.

## Sumário

1. [O que é o dbt](#1-o-que-é-o-dbt)
2. [Estrutura de um projeto](#2-estrutura-de-um-projeto)
3. [Materializações](#3-materializações)
4. [Comandos principais](#4-comandos-principais)
5. [Seletores de modelos (`--select`)](#5-seletores-de-modelos---select)
6. [Testes](#6-testes)
7. [Snapshots (SCD Type 2)](#7-snapshots-scd-type-2)
8. [Seeds](#8-seeds)
9. [Sources e source freshness](#9-sources-e-source-freshness)
10. [Jinja e macros](#10-jinja-e-macros)
11. [Packages](#11-packages)
12. [Profiles e targets (dev/prod)](#12-profiles-e-targets-devprod)
13. [Documentação (docs generate/serve)](#13-documentação-docs-generateserve)
14. [Boas práticas usadas neste projeto](#14-boas-práticas-usadas-neste-projeto)
15. [Referência rápida](#15-referência-rápida)

---

## 1. O que é o dbt

dbt transforma dados **dentro** do warehouse (aqui, Databricks): você escreve `SELECT`s em arquivos `.sql`, e o dbt se encarrega de:

- Descobrir a ordem de execução correta a partir das dependências (`ref()`/`source()`), montando um DAG de modelos.
- Materializar cada modelo como view/tabela/incremental no schema configurado.
- Rodar testes de qualidade de dados declarados em `.yml`.
- Gerar documentação e o grafo de linhagem (lineage).
- Versionar snapshots de tabelas mutáveis (histórico tipo SCD2).

O dbt **não extrai** dados de sistemas externos — isso é a camada Bronze, carregada fora do dbt. O dbt cuida do que vem depois: Bronze → Silver → Gold.

## 2. Estrutura de um projeto

```
dbt_core/
├── models/
│   ├── bronze/     # sources (.yml) apontando pra tabelas brutas já existentes no Databricks
│   ├── silver/     # modelos .sql + .yml de dados tratados/padronizados
│   └── gold/       # modelos .sql + .yml de negócio (agregados, prontos pra consumo)
├── seeds/          # .csv versionados, carregados como tabela via `dbt seed`
├── snapshots/      # snapshots .sql (histórico de mudanças em tabelas mutáveis)
├── macros/         # funções Jinja reutilizáveis
├── tests/          # testes singulares (um .sql = um teste customizado)
├── analyses/       # queries auxiliares, compiladas mas nunca materializadas
├── dbt_project.yml # configuração do projeto (schemas, materializações padrão por pasta)
└── packages.yml    # dependências de pacotes dbt (ex.: dbt_utils)
```

Cada nível dessa árvore corresponde a um "tipo de recurso" do dbt, com seu próprio comando (`dbt seed`, `dbt snapshot`, `dbt test`, etc.) — ver seção 4.

## 3. Materializações

A materialização define **como** o resultado de um `SELECT` vira um objeto no Databricks. Configura-se com `{{ config(materialized='...') }}` no topo do `.sql`, ou por pasta inteira no `dbt_project.yml` (é o que este projeto faz: `bronze`/`silver`/`gold` = `table` por padrão).

| Materialização | O que faz | Quando usar |
|---|---|---|
| `view` | Cria uma `VIEW` — a query roda a cada leitura | Modelos intermediários baratos de recalcular, sem necessidade de performance de leitura |
| `table` | Roda o `SELECT` e grava o resultado como tabela física (`CREATE TABLE AS`, com `DROP`+recriação a cada `run`) | Modelos finais (Gold) e Silver que são lidos com frequência — é o padrão deste projeto |
| `incremental` | Na primeira execução cria a tabela inteira; nas seguintes, insere/atualiza **só as linhas novas/alteradas** (`is_incremental()` no SQL) | Tabelas grandes onde reprocessar tudo toda vez é caro (ex.: fatos com histórico longo) |
| `ephemeral` | Não cria nada no banco — é injetado como CTE (`WITH`) nos modelos que o referenciam | CTEs compartilhadas entre modelos, sem valor em existir como objeto próprio |

Exemplo de modelo incremental:

```sql
{{ config(materialized='incremental', unique_key='id_pedido') }}

select * from {{ ref('slv_olist_pedidos') }}

{% if is_incremental() %}
  where data_pedido > (select max(data_pedido) from {{ this }})
{% endif %}
```

`{{ this }}` referencia a própria tabela já materializada — só existe fazendo sentido dentro do bloco incremental.

## 4. Comandos principais

| Comando | O que faz |
|---|---|
| `dbt debug` | Testa a conexão com o Databricks e valida `dbt_project.yml`/`profiles.yml` |
| `dbt deps` | Instala os pacotes de `packages.yml` em `dbt_packages/` |
| `dbt seed` | Carrega os `.csv` de `seeds/` como tabelas |
| `dbt run` | Executa (materializa) os models — não roda testes nem snapshots |
| `dbt snapshot` | Executa os snapshots de `snapshots/` |
| `dbt test` | Roda os testes declarados nos `.yml` e os singulares de `tests/` |
| `dbt build` | Roda, na ordem certa do DAG: `seed` → `snapshot` → `run` → `test`. É o comando recomendado no dia a dia (e o que as DAGs do Airflow deste projeto usam) |
| `dbt compile` | Só compila o Jinja pra SQL puro (em `target/compiled/`), sem executar nada — útil pra debugar um `ref()`/macro |
| `dbt show --select modelo` | Compila e roda um modelo, mostrando uma amostra do resultado no terminal, sem materializar |
| `dbt ls` (ou `list`) | Lista os recursos que um `--select` casaria, sem executar nada — bom pra validar um seletor antes de rodar de verdade |
| `dbt source freshness` | Verifica se as tabelas declaradas como `source` estão "frescas" dentro do prazo configurado |
| `dbt docs generate` | Gera os metadados de documentação (`target/catalog.json` + `manifest.json`) |
| `dbt docs serve` | Sobe um servidor local com a documentação navegável e o grafo de linhagem |
| `dbt clean` | Remove `target/` e `dbt_packages/` |
| `dbt retry` | Reexecuta só os nós que falharam/não rodaram na última invocação (lê `target/run_results.json`) |

Flags que valem para (quase) todos os comandos acima:

| Flag | Efeito |
|---|---|
| `--select` / `-s` | Restringe a um subconjunto de modelos/recursos (seção 5) |
| `--exclude` | Remove um subconjunto do que seria rodado |
| `--target` / `-t` | Escolhe o target do `profiles.yml` (`dev`/`prod`) |
| `--full-refresh` | Força um `incremental` a ser reconstruído do zero (dropa e recria) |
| `--vars '{chave: valor}'` | Passa variáveis Jinja para dentro dos modelos (`{{ var('chave') }}`) |
| `--threads N` | Sobrescreve o número de threads paralelas do profile |
| `--fail-fast` | Para a execução no primeiro erro, em vez de tentar rodar tudo |

Exemplos usados neste projeto (ver [dbt_core/README.md](../dbt_core/README.md#comandos-principais-do-dbt) e as DAGs em [airflow/dags/](../airflow/dags/)):

```bash
dbt build --target dev --select slv_olist_pedidos     # testa um modelo isolado em dev
dbt build --target prod --select gld_olist_pedidos    # é isso que o Airflow roda em produção
```

## 5. Seletores de modelos (`--select`)

O `--select` aceita bem mais que nome de modelo solto:

| Sintaxe | Seleciona |
|---|---|
| `slv_olist_pedidos` | Só esse modelo |
| `silver.*` ou `silver` (por pasta/tag) | Todos os modelos daquela pasta |
| `slv_olist_pedidos+` | O modelo **e tudo que depende dele** (downstream) |
| `+slv_olist_pedidos` | O modelo **e tudo que ele depende** (upstream) |
| `+slv_olist_pedidos+` | Upstream e downstream juntos |
| `slv_olist_pedidos+1` | Downstream, mas só 1 nível (evita explodir o grafo inteiro) |
| `tag:nightly` | Modelos marcados com `{{ config(tags=['nightly']) }}` |
| `config.materialized:incremental` | Todos os modelos com essa config |
| `path:models/gold` | Todos os modelos daquele diretório |
| `source:bronze+` | A source e tudo que consome dela |
| `resource_type:snapshot` | Só snapshots (útil combinado com `dbt ls`) |
| `state:modified+` | (precisa de `--state`) Só o que mudou desde o último `manifest.json` salvo, e seus downstream — usado em CI para rodar só o que foi alterado num PR |

Combine com espaço (união/OR) ou vírgula (interseção/AND):

```bash
dbt run --select silver gold          # roda silver OU gold
dbt run --select tag:nightly,gold     # roda o que é tag:nightly E está em gold
```

## 6. Testes

Dois tipos:

**Testes genéricos** (schema tests) — declarados em `.yml`, reaproveitáveis em qualquer coluna:

```yaml
models:
  - name: gld_olist_pedidos
    columns:
      - name: id_pedido
        tests:
          - unique
          - not_null
      - name: status_pedido
        tests:
          - accepted_values:
              values: ['Entregue', 'Cancelado', 'Em trânsito']
      - name: id_cliente
        tests:
          - relationships:
              to: ref('slv_olist_clientes')
              field: id_cliente
```

Os quatro acima (`unique`, `not_null`, `accepted_values`, `relationships`) já vêm no dbt-core. Pacotes como `dbt_utils` adicionam mais (`dbt_utils.expression_is_true`, `dbt_utils.not_constant`, etc.).

**Testes singulares** — um arquivo `.sql` em `tests/` que retorna linhas quando algo está **errado** (o teste passa se a query não retornar nenhuma linha):

```sql
-- tests/assert_valor_pedido_positivo.sql
select id_pedido
from {{ ref('gld_olist_pedidos') }}
where valor_total_pedido < 0
```

Rodar: `dbt test` (todos) ou `dbt test --select gld_olist_pedidos` (só os testes daquele modelo). `severity: warn` no `.yml` faz o teste reportar sem quebrar o build; o padrão é `error`.

## 7. Snapshots (SCD Type 2)

Snapshot resolve um problema específico: uma tabela de origem é **mutável** (um `UPDATE` sobrescreve o valor antigo), mas você precisa saber **o que ela era antes**. O `dbt snapshot` grava, a cada execução, o estado atual da fonte numa tabela de histórico, com colunas de controle (`dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`) — isso é o padrão **Slowly Changing Dimension Type 2**.

Este projeto ainda não tem snapshots em [dbt_core/snapshots/](../dbt_core/snapshots/), mas o caso de uso natural aqui seria histórico de `status_pedido` ou de endereço/cadastro do cliente. Exemplo:

```sql
-- snapshots/snap_olist_pedidos_status.sql
{% snapshot snap_olist_pedidos_status %}

{{
    config(
        target_schema='snapshots',
        unique_key='id_pedido',
        strategy='timestamp',
        updated_at='data_atualizacao',
    )
}}

select id_pedido, status_pedido, data_atualizacao
from {{ source('bronze', 'pedidos') }}

{% endsnapshot %}
```

Duas estratégias de detecção de mudança:

| Estratégia | Como detecta mudança | Quando usar |
|---|---|---|
| `timestamp` | Compara uma coluna `updated_at` com o valor já registrado | A fonte tem uma coluna confiável de última atualização |
| `check` (`check_cols=['col1', 'col2']` ou `check_cols='all'`) | Compara o valor das colunas listadas | Não existe `updated_at`, ou ele não é confiável |

Depois de criado, o snapshot roda com `dbt snapshot` (incluso automaticamente em `dbt build`) e vira uma tabela normal que outros modelos podem consumir via `{{ ref('snap_olist_pedidos_status') }}`, com uma linha por versão do registro. `invalidate_hard_deletes=true` marca como "expirada" (`dbt_valid_to` preenchido) uma linha que sumiu da fonte, em vez de deixá-la como se ainda fosse válida.

> Snapshot é feito pra ser rodado com frequência (ex.: diariamente, antes da Silver) — quanto maior o intervalo entre execuções, maior a chance de perder uma mudança intermediária que aconteceu e voltou entre uma execução e outra.

## 8. Seeds

`seeds/*.csv` são pequenos arquivos estáticos versionados no Git, carregados como tabela com `dbt seed`. Bom para tabelas de mapeamento/lookup que mudam raramente e são mantidas por quem desenvolve (ex.: de-para de código de status, feriados, taxas fixas) — nunca para dados operacionais grandes ou que mudam via sistema externo (isso é Bronze/source).

```bash
dbt seed                              # carrega todos os .csv de seeds/
dbt seed --select nome_do_seed        # só um
dbt seed --full-refresh               # dropa e recria (útil se mudou o schema do csv)
```

## 9. Sources e source freshness

`source()` declara uma tabela que **já existe** no warehouse, criada fora do dbt (aqui, a camada Bronze — ver [dbt_core/models/bronze/source_silver.yml](../dbt_core/models/bronze/source_silver.yml)). Isso dá ao dbt visibilidade da tabela no grafo de linhagem sem tentar criá-la:

```yaml
sources:
  - name: bronze
    database: dados_prod
    schema: bronze
    tables:
      - name: pedidos
        loaded_at_field: data_carga
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
```

Usada nos modelos como `{{ source('bronze', 'pedidos') }}`. Rodar `dbt source freshness` consulta o `loaded_at_field` mais recente e avisa/quebra se a Bronze estiver desatualizada — útil como checagem antes de disparar a Silver (ver o comentário sobre isso na DAG [dbt_gold_olist.py](../airflow/dags/dbt_gold_olist.py), que já resolve um problema parecido no lado Silver→Gold via Assets).

## 10. Jinja e macros

Todo `.sql` do dbt é compilado como template Jinja antes de virar SQL puro. As funções mais usadas:

- `{{ ref('nome_do_modelo') }}` — referencia outro modelo do projeto; é isso que monta o DAG de dependências (o dbt resolve pro nome de schema/tabela certo, considerando o target ativo).
- `{{ source('nome_source', 'nome_tabela') }}` — referencia uma source.
- `{{ config(...) }}` — configura materialização, schema, tags, etc. daquele modelo específico (sobrepõe o `dbt_project.yml`).
- `{{ var('chave', 'default') }}` — lê uma variável passada via `--vars` ou definida em `dbt_project.yml`.
- `{{ this }}` — dentro de um modelo, referencia a própria tabela materializada (usado em `is_incremental()`).

Macros customizadas ficam em `macros/*.sql` e são funções Jinja reutilizáveis:

```sql
{% macro limpa_cpf(coluna) %}
    regexp_replace({{ coluna }}, '[^0-9]', '')
{% endmacro %}
```

Uso: `select {{ limpa_cpf('cpf_cliente') }} as cpf_cliente from ...`.

## 11. Packages

`packages.yml` declara dependências de outros projetos dbt (o mais comum é [dbt_utils](https://github.com/dbt-labs/dbt-utils), com macros e testes genéricos prontos):

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

`dbt deps` baixa isso para `dbt_packages/` (não versionado — precisa rodar de novo em máquina nova, ver checklist no [dbt_core/README.md](../dbt_core/README.md#checklist-para-testar-do-zero-primeira-vez-numa-máquina-nova)).

## 12. Profiles e targets (dev/prod)

O **profile** (`~/.dbt/profiles.yml`, fora do repositório) define **como conectar** no Databricks; o **target** dentro dele define **qual ambiente**. Este projeto usa:

- `dev` — token pessoal, catálogo `dados_dev`. Lê a Bronze de `dados_prod` (ela só existe lá) e escreve Silver/Gold em `dados_dev`, isolado.
- `prod` — service principal (OAuth M2M), catálogo `dados_prod`. É o target usado pelas DAGs do Airflow (`--target prod`).

```bash
dbt run --target dev     # ambiente de teste
dbt run --target prod    # produção — normalmente só via Airflow, não à mão
```

Detalhes completos de configuração em [dbt_core/README.md](../dbt_core/README.md#configuração-do-ambiente).

## 13. Documentação (docs generate/serve)

```bash
dbt docs generate   # gera manifest.json/catalog.json em target/
dbt docs serve       # sobe http://localhost:8080 com o site de documentação
```

O site mostra, por modelo: SQL compilado, colunas documentadas no `.yml`, testes aplicados e o **grafo de linhagem** (DAG) completo do projeto — útil para visualizar o impacto de mudar um modelo Silver sobre os Gold que dependem dele.

## 14. Boas práticas usadas neste projeto

- **Um schema por camada** (`bronze`, `silver`, `gold`), configurado uma vez em [dbt_project.yml](../dbt_core/dbt_project.yml) em vez de em cada model.
- **Nome do arquivo `.sql` = nome do arquivo `.yml`** que o documenta — nome divergente faz a documentação/testes não serem aplicados ao modelo (ver convenção em [dbt_core/README.md](../dbt_core/README.md#convenção-de-nomes)).
- **Prefixos por camada**: `slv_` (silver), sufixo/prefixo `gld_`/`_gold` conforme a camada.
- **`dbt build` em vez de `dbt run`** nas DAGs do Airflow — garante que teste quebrado derruba a task, em vez de silenciosamente materializar dado ruim.
- **Bronze fixa em `dados_prod`** independente do target ativo — testar em `dev` nunca duplica a carga bruta, só isola onde a Silver/Gold é escrita.

## 15. Referência rápida

```bash
dbt debug                                    # valida conexão/config
dbt deps                                     # instala packages.yml
dbt seed                                     # carrega seeds/*.csv
dbt snapshot                                 # roda snapshots/*.sql
dbt run --select <seletor>                   # materializa modelos
dbt test --select <seletor>                  # roda testes
dbt build --select <seletor> --target prod   # seed+snapshot+run+test, na ordem certa
dbt compile --select <seletor>               # só compila o Jinja, não executa
dbt show --select <modelo>                   # roda e mostra amostra, sem materializar
dbt ls --select <seletor>                    # lista o que o seletor casaria
dbt source freshness                         # checa atraso das sources
dbt docs generate && dbt docs serve          # documentação + lineage
dbt clean                                    # limpa target/ e dbt_packages/
dbt retry                                    # reexecuta só o que falhou da última vez
```
