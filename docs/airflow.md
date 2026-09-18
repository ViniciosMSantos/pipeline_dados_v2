# Guia de Airflow — comandos e conceitos

Referência de Apache Airflow (v3.3.1 neste projeto, ver [docker-compose.yml](../docker-compose.yml)) aplicada às DAGs em [airflow/dags/](../airflow/dags/). Aqui o Airflow **não contém lógica SQL** — ele só aciona containers `docker run` do dbt (`docker/dbt/Dockerfile`); toda transformação vive no dbt (ver [docs/dbt.md](dbt.md)). Este documento foca em **conceitos e comandos**, não em setup (setup completo está no [README.md](../README.md#2-stack-completa-docker--airflow-orquestrando-o-dbt) da raiz).

## Sumário

1. [Conceitos fundamentais](#1-conceitos-fundamentais)
2. [Airflow 3: o que mudou](#2-airflow-3-o-que-mudou)
3. [Anatomia de uma DAG (exemplo real do projeto)](#3-anatomia-de-uma-dag-exemplo-real-do-projeto)
4. [Operators](#4-operators)
5. [Scheduling](#5-scheduling)
6. [Assets (data-aware scheduling)](#6-assets-data-aware-scheduling)
7. [Dependências entre tasks](#7-dependências-entre-tasks)
8. [Trigger rules](#8-trigger-rules)
9. [Retries, timeouts e alertas](#9-retries-timeouts-e-alertas)
10. [Controle de concorrência](#10-controle-de-concorrência)
11. [CLI do Airflow](#11-cli-do-airflow)
12. [Rodando neste projeto (Docker Compose)](#12-rodando-neste-projeto-docker-compose)
13. [Connections e Variables](#13-connections-e-variables)
14. [Monitorando pela UI](#14-monitorando-pela-ui)
15. [Boas práticas usadas neste projeto](#15-boas-práticas-usadas-neste-projeto)
16. [Referência rápida](#16-referência-rápida)

---

## 1. Conceitos fundamentais

| Termo | O que é |
|---|---|
| **DAG** (Directed Acyclic Graph) | Um pipeline: um arquivo Python que declara um conjunto de tasks e suas dependências. Não pode ter ciclos — daí o nome. |
| **Task** | Uma unidade de trabalho dentro de uma DAG (ex.: rodar um `docker run`). Instância de um **Operator**. |
| **Operator** | O "tipo" de trabalho que uma task faz (`BashOperator` roda um comando shell, `PythonOperator` roda uma função Python, etc.). |
| **Task Instance** | Uma execução específica de uma task, para um `run` específico da DAG — é isso que tem estado (`success`, `failed`, `running`...). |
| **DAG Run** | Uma execução completa da DAG inteira, disparada por schedule, manualmente, ou por um Asset atualizado. |
| **Scheduler** | Processo que decide quando cada DAG deve rodar e enfileira as tasks. |
| **DAG Processor** | (separado do scheduler desde o Airflow 3) processo que faz o parsing dos arquivos `.py` em `dags/` e monta a representação interna das DAGs. |
| **API Server** | Serve a UI web e a API REST (substituiu o antigo webserver). |
| **Executor** | Componente que efetivamente roda as tasks (aqui, `LocalExecutor` via Docker Compose — cada task roda no processo do worker local, que por sua vez dispara um `docker run` externo). |
| **Metadata DB** | Banco (Postgres, neste projeto) onde o Airflow guarda estado de DAGs, tasks, conexões, variáveis etc. |

## 2. Airflow 3: o que mudou

Pontos relevantes que aparecem nas DAGs deste projeto:

- **`airflow.sdk`** é o novo pacote para autoria de DAGs (`from airflow.sdk import Asset`), separado do runtime interno do scheduler — pensado para DAGs rodarem isoladas do ambiente do Airflow em si (Task Execution API, via JWT — daí o `AIRFLOW_JWT_SECRET` no `.env`).
- **Assets substituem Datasets** (Airflow 2.4–2.x) como mecanismo de *data-aware scheduling* — mesmo conceito, nome novo e API mais rica (ver seção 6).
- **DAG Processor** roda como serviço separado do scheduler (visível nos serviços do [docker-compose.yml](../docker-compose.yml)), para isolar falhas de parsing de DAG do agendamento em si.
- Operators "clássicos" (`BashOperator`, `PythonOperator`) migraram para pacotes `providers` (`airflow.providers.standard.operators.bash`), em vez de morarem no core do `airflow`.

## 3. Anatomia de uma DAG (exemplo real do projeto)

Usando [airflow/dags/dbt_silver_olist.py](../airflow/dags/dbt_silver_olist.py) como referência:

```python
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset

SILVER_MODELS = ["slv_olist_pedidos", "slv_olist_clientes", ...]

with DAG(
    dag_id="dbt_silver_olist",
    description="Builda e testa os models da camada silver via container dbt.",
    start_date=datetime(2026, 1, 1),   # a partir de quando a DAG "existe" pro scheduler
    schedule="0 14 * * *",              # cron: todo dia às 14h
    catchup=False,                      # não roda runs "atrasados" retroativos
    tags=["dbt", "silver"],             # só organização/filtro na UI
    max_active_tasks=1,                 # no máx. 1 task rodando por vez, nesta DAG
) as dag:
    for model in SILVER_MODELS:
        BashOperator(
            task_id=f"dbt_build_{model}",
            bash_command="docker run --rm ... pipeline-dbt:1.0 build --select " + model,
            outlets=[Asset(f"olist://silver/{model}")],   # publica um Asset ao terminar
        )
```

Pontos-chave:
- O `with DAG(...) as dag:` é um context manager — toda task criada dentro dele é automaticamente associada a essa DAG.
- Uma task por modelo, gerada em loop — o Airflow não sabe que isso é dbt, só vê N tasks independentes rodando o mesmo comando parametrizado.
- `outlets=[Asset(...)]` é o que permite outra DAG (`dbt_gold_olist`) reagir a essa task terminando (seção 6).

## 4. Operators

| Operator | Pacote/uso | Neste projeto |
|---|---|---|
| `BashOperator` | `airflow.providers.standard.operators.bash` — roda um comando shell | **É o único usado aqui** — dispara `docker run ... pipeline-dbt:1.0 build --select <model>` para cada model dbt |
| `PythonOperator` | `airflow.providers.standard.operators.python` — roda uma função Python | Não usado neste projeto (não há lógica Python nas DAGs, só orquestração) |
| `EmptyOperator` | `airflow.providers.standard.operators.empty` — task "vazia" | Útil como ponto de sincronização (ex.: juntar vários ramos antes de seguir) |
| `BranchPythonOperator` | Decide dinamicamente qual task rodar a seguir, baseado em uma função Python | Útil se um dia precisar de lógica condicional (ex.: pular Gold se Silver não mudou nada) |
| Sensors (`BaseSensorOperator` e subclasses) | Espera uma condição externa antes de deixar a DAG seguir (arquivo aparecer, API responder, etc.) | Não usado — aqui a espera por dependência é resolvida via Assets, não sensor |

`docker run` direto via `BashOperator` (em vez do `DockerOperator` dedicado) é uma escolha deliberada deste projeto: as tasks rodam contra o **daemon Docker do host** (docker-outside-of-docker), então basta o binário `docker` estar disponível dentro do container do Airflow (ver `docker/airflow/Dockerfile`), sem precisar da dependência extra do provider `docker`.

## 5. Scheduling

O parâmetro `schedule` de uma DAG aceita:

| Valor | Significado |
|---|---|
| `"0 14 * * *"` | Expressão cron (min hora dia mês dia-da-semana) — aqui, todo dia às 14h |
| `"@daily"`, `"@hourly"`, `"@weekly"`, `"@once"` | Presets equivalentes a cron comuns |
| `timedelta(hours=6)` | Intervalo fixo a partir do `start_date` |
| `None` | Só dispara manualmente (UI, CLI ou API) |
| Lista de `Asset(...)` | *Data-aware scheduling* — dispara quando os assets forem atualizados (seção 6) — **usado em `dbt_gold_olist`** |

`catchup=False` (usado nas duas DAGs deste projeto) evita que o Airflow tente rodar todos os intervalos "perdidos" entre `start_date` e agora na primeira vez que a DAG é ativada — sem isso, uma `start_date` de meses atrás geraria dezenas de runs retroativos de uma vez.

## 6. Assets (data-aware scheduling)

Em vez de agendar por horário, uma DAG pode ser agendada para rodar **quando outra DAG atualiza um dado específico**. É o mecanismo usado em [dbt_gold_olist.py](../airflow/dags/dbt_gold_olist.py):

```python
SILVER_DEPS_GOLD = [
    Asset("olist://silver/slv_olist_pedidos"),
    Asset("olist://silver/slv_olist_clientes"),
    Asset("olist://silver/slv_olist_itens_pedidos"),
    # ...
]

with DAG(..., schedule=SILVER_DEPS_GOLD) as dag:
    ...
```

- Um `Asset` é identificado por uma URI (aqui, um esquema arbitrário `olist://silver/<nome>` — não precisa ser uma URL real, é só um identificador único).
- Uma task publica ("produz") um Asset declarando `outlets=[Asset(...)]` — é o que [dbt_silver_olist.py](../airflow/dags/dbt_silver_olist.py) faz ao final de cada `dbt build`.
- Quando o `schedule` é uma **lista** de Assets, o Airflow usa lógica **AND**: a DAG só dispara depois que **todos** os assets da lista tiverem sido atualizados pelo menos uma vez desde o último disparo — não a cada atualização individual.
- **Cuidado com a URI**: o texto do `Asset(...)` no `outlets` de quem publica precisa ser **idêntico, caractere a caractere**, ao `Asset(...)` no `schedule` de quem consome. Uma divergência (plural/singular, typo) faz a DAG consumidora nunca dar match naquele asset — ela simplesmente nunca dispara sozinha, sem erro visível (foi exatamente esse bug que existia em `dbt_gold_olist.py` antes de ser corrigido).
- A UI do Airflow tem uma aba **Assets** que mostra o grafo de quem produz/consome cada um, útil pra depurar isso visualmente.

Vantagem sobre schedule fixo por horário: a DAG Gold só roda depois que a Silver realmente terminou de atualizar, em vez de um horário fixo que "torce" pra Silver já ter rodado antes.

## 7. Dependências entre tasks

Dentro da mesma DAG, dependência é declarada com os operadores `>>`/`<<`, ou métodos explícitos:

```python
task_a >> task_b >> task_c        # a roda, depois b, depois c
task_a >> [task_b, task_c]        # b e c rodam em paralelo, após a
task_a.set_downstream(task_b)     # equivalente a task_a >> task_b
```

Neste projeto, as tasks de cada DAG **não têm dependência declarada entre si** — todas as tasks do loop `for model in MODELOS_GOLD` são independentes entre si (o `max_active_tasks=1` só limita paralelismo por CPU, não impõe ordem). A ordem real entre camadas (Silver antes de Gold) vem do Asset scheduling entre DAGs (seção 6), não de `>>` dentro da DAG.

Para dependências mais complexas: `airflow.models.baseoperator.chain(t1, t2, t3)` (equivalente a `t1 >> t2 >> t3`, mais legível com listas) e `cross_downstream([a, b], [c, d])` (todos de um grupo apontam para todos do outro).

## 8. Trigger rules

Por padrão, uma task só roda se **todas** as suas upstream tiverem sucesso (`all_success`). Isso é configurável por task via `trigger_rule`:

| `trigger_rule` | Roda quando |
|---|---|
| `all_success` (padrão) | Todas as upstream tiveram sucesso |
| `all_failed` | Todas as upstream falharam |
| `all_done` | Todas as upstream terminaram (sucesso ou falha) — útil para tasks de limpeza/notificação |
| `one_success` | Pelo menos uma upstream teve sucesso |
| `none_failed` | Nenhuma upstream falhou (skips contam como "ok") |

Não usado atualmente neste projeto (todas as tasks são independentes), mas relevante se, por exemplo, uma task de notificação de erro precisar rodar mesmo que uma task anterior falhe (`trigger_rule="all_done"` ou `"one_failed"`).

## 9. Retries, timeouts e alertas

Configurável por task (via `default_args` da DAG, ou por operator individual):

```python
BashOperator(
    task_id="...",
    bash_command="...",
    retries=2,                          # tenta de novo até 2x em caso de falha
    retry_delay=timedelta(minutes=5),   # espera entre tentativas
    execution_timeout=timedelta(hours=1),  # mata a task se passar disso
    on_failure_callback=minha_funcao,   # chamado quando a task falha (ex.: postar no Slack)
)
```

Nenhuma DAG deste projeto configura `retries` hoje — está listado como pendente no [README.md](../README.md#status-atual) ("retries/alertas"). Sem isso, uma falha transiente (ex.: warehouse do Databricks demorando pra subir) derruba a task sem nova tentativa automática.

## 10. Controle de concorrência

| Parâmetro | Escopo | Efeito |
|---|---|---|
| `max_active_tasks` | Por DAG | Quantas tasks daquela DAG podem rodar ao mesmo tempo, entre todos os runs ativos. Este projeto usa `1` nas duas DAGs, porque cada task sobe um `docker run` isolado e o host (WSL2, 4 vCPUs) satura com paralelismo — task lenta demais tem o heartbeat atrasado e o Airflow manda `SIGTERM` antes do dbt terminar (comentário em [dbt_silver_olist.py](../airflow/dags/dbt_silver_olist.py)). |
| `max_active_runs` | Por DAG | Quantos **runs completos** daquela DAG podem existir simultaneamente (ex.: se disparar manualmente enquanto o schedule também dispara) |
| `concurrency` (deprecated, virou `max_active_tasks`) | — | Nome antigo, evitar em código novo |
| `parallelism` | Global (`airflow.cfg`) | Teto de tasks rodando ao mesmo tempo em toda a instância do Airflow, todas as DAGs somadas |

## 11. CLI do Airflow

Comandos rodados dentro do container (`docker compose exec airflow-scheduler airflow ...`, ver seção 12):

| Comando | O que faz |
|---|---|
| `airflow dags list` | Lista as DAGs que o Airflow enxerga |
| `airflow dags unpause <dag_id>` | Ativa uma DAG (sai do estado "paused" em que toda DAG nova começa) |
| `airflow dags pause <dag_id>` | Pausa (para de disparar novos runs automaticamente) |
| `airflow dags trigger <dag_id>` | Dispara um run manual imediatamente |
| `airflow dags state <dag_id> <execution_date>` | Mostra o estado de um run específico |
| `airflow dags backfill <dag_id> -s <start> -e <end>` | Roda retroativamente para um intervalo de datas (ignora `catchup`) |
| `airflow tasks list <dag_id>` | Lista as tasks de uma DAG |
| `airflow tasks test <dag_id> <task_id> <data>` | Roda uma task isolada, fora do scheduler, sem gravar estado — ótimo pra debugar rápido |
| `airflow tasks states-for-dag-run <dag_id> <run_id>` | Estado de cada task de um run específico |
| `airflow connections list` / `add` / `delete` | Gerencia Connections (seção 13) |
| `airflow variables list` / `set` / `get` | Gerencia Variables (seção 13) |
| `airflow db migrate` | Aplica migrações no metadata DB (rodado automaticamente pelo serviço `airflow-init`) |
| `airflow config get-value <section> <key>` | Consulta um valor efetivo de configuração |

## 12. Rodando neste projeto (Docker Compose)

Setup completo está no [README.md](../README.md#2-stack-completa-docker--airflow-orquestrando-o-dbt) da raiz; aqui só os comandos do dia a dia:

```bash
docker compose up -d --build            # sobe postgres + airflow-init + api-server + scheduler + dag-processor
docker compose ps                       # confere quem está "healthy"
docker compose logs -f airflow-scheduler   # acompanha logs do scheduler em tempo real

# CLI do Airflow dentro do container do scheduler:
docker compose exec airflow-scheduler airflow dags unpause dbt_silver_olist
docker compose exec airflow-scheduler airflow dags trigger dbt_silver_olist
docker compose exec airflow-scheduler airflow dags list

docker compose down                     # derruba os serviços (mantém o volume do postgres)
docker compose down -v                  # derruba tudo, incluindo histórico do Airflow no postgres
```

UI: [http://localhost:8080](http://localhost:8080). Logs de task também ficam em `airflow/logs/dag_id=<dag>/...` no host, fora da UI.

> As tasks rodam `docker run` contra o **daemon Docker do host** (não um Docker aninhado) — por isso `DBT_CORE_HOST_PATH`/`DBT_PROFILES_HOST_PATH` no `.env` precisam ser caminhos reais do host, e o container do Airflow precisa do socket `/var/run/docker.sock` montado com o grupo `docker` certo (`DOCKER_GID`).

## 13. Connections e Variables

- **Connections**: credenciais/endpoints reutilizáveis entre DAGs (ex.: conexão com um banco, uma API), geridas em Admin → Connections na UI ou `airflow connections add`. Este projeto não usa nenhuma — a credencial do Databricks vem de variável de ambiente (`DBT_DATABRICKS_TOKEN`) passada direto pro container do dbt via `-e` no `docker run`, não por uma Connection do Airflow.
- **Variables**: pares chave/valor globais, acessíveis nas DAGs via `Variable.get("chave")`, geridos em Admin → Variables ou `airflow variables set`. Também não usado aqui — os parâmetros de cada DAG (lista de models, assets) estão hardcoded no próprio `.py`, o que é intencional para manter a DAG auto-contida e legível sem depender de estado configurado só na UI.

## 14. Monitorando pela UI

- **Grid**: histórico de runs de uma DAG, uma coluna por run, uma linha por task — cor indica o estado (verde=sucesso, vermelho=falha, etc.).
- **Graph**: visualização do DAG de tasks de um run específico, útil pra ver dependências e onde travou.
- **Assets**: grafo de quem produz/consome cada `Asset` — útil pra depurar por que uma DAG agendada por asset não disparou (ver seção 6).
- **Logs**: clicando em uma task instance, mostra o log completo daquela execução (aqui, o output do `docker run`, incluindo o log do dbt).

## 15. Boas práticas usadas neste projeto

- **Airflow sem lógica SQL** — cada DAG só monta um `docker run --select <model>`; toda a lógica de dependência entre modelos vive no `ref()` do dbt, não em `>>` do Airflow.
- **Uma task por model dbt**, gerada em loop a partir de uma lista — adicionar um model novo à camada é só adicionar um item na lista (`MODELOS_GOLD`/`SILVER_MODELS`), sem tocar no resto da DAG.
- **`max_active_tasks=1`** nas duas DAGs — limite de CPU do host é mais restritivo que "rodar tudo em paralelo seria mais rápido".
- **Assets em vez de horário fixo** para encadear Silver → Gold — a DAG Gold só dispara depois que a Silver terminou de verdade, não depois de "tempo suficiente que provavelmente terminou".
- **`catchup=False`** nas duas DAGs — evita reprocessar histórico inteiro só por ter ativado a DAG depois da `start_date`.

## 16. Referência rápida

```bash
# Gerenciar DAGs
airflow dags list
airflow dags unpause <dag_id>
airflow dags pause <dag_id>
airflow dags trigger <dag_id>
airflow dags backfill <dag_id> -s 2026-01-01 -e 2026-01-07

# Depurar tasks
airflow tasks list <dag_id>
airflow tasks test <dag_id> <task_id> <data>
airflow tasks states-for-dag-run <dag_id> <run_id>

# Connections/Variables
airflow connections list
airflow variables list

# Docker Compose (este projeto)
docker compose up -d --build
docker compose ps
docker compose logs -f airflow-scheduler
docker compose exec airflow-scheduler airflow dags trigger dbt_silver_olist
docker compose down
```
