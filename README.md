# Pipeline de Indicadores Econômicos — Banco Central do Brasil

> Pipeline de dados batch ponta a ponta para ingestão, processamento e análise de
> indicadores econômicos do BCB (Selic, IPCA, Dólar) com Airflow, PySpark, dbt e BigQuery.

![CI](https://github.com/davidkleinn/pipeline-indicadores-bcb/actions/workflows/dbt_ci.yml/badge.svg)

## Arquitetura

\`\`\`mermaid
flowchart LR
    API[API SGS - Banco Central] -->|extract_daily.py| DAG[Airflow DAG diária]
    DAG --> RAW1[(Postgres: raw.bcb_series)]

    API -->|backfill manual| SPARK[PySpark]
    SPARK --> RAW2[(Postgres: raw.bcb_historico)]
    SPARK --> PARQUET[Parquet particionado por ano]

    RAW1 --> STG[dbt: staging]
    RAW2 --> STG
    STG --> INT[dbt: intermediate - ephemeral]
    INT --> MARTS[dbt: marts]
    DAG -.-> STG

    MARTS --> BQ[(BigQuery)]
    MARTS --> BI[Dashboard Power BI]
\`\`\`

## Stack

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow (LocalExecutor) |
| Processamento distribuído | PySpark |
| Transformação | dbt-core |
| Armazenamento local | PostgreSQL |
| Cloud warehouse | BigQuery (GCP Sandbox) |
| Dashboard | Power BI Desktop |
| Containerização | Docker + Docker Compose |
| Versionamento | Git + GitHub |

## Resultados

- **42 anos** de histórico processado (Dólar desde 1984, Selic desde 1986, IPCA desde 1980)
- **21.021 registros** no total: 10.420 (Dólar) + 10.044 (Selic) + 557 (IPCA)
- **3 indicadores** econômicos processados de ponta a ponta
- **16 testes** de qualidade de dados com dbt (todos passando)
- Pipeline diário idempotente — roda N vezes no mesmo dia sem duplicar dados
  (`ON CONFLICT DO NOTHING` na carga diária, `TRUNCATE + reload` no backfill)

## Screenshots

### Lineage do dbt (raw → staging → intermediate → marts)
![dbt lineage](docs/screenshots/dbt_lineage.png)

### Tabelas no BigQuery
![BigQuery](docs/screenshots/bigquery_tabelas.png)

### Dashboard — visão geral
![Dashboard](docs/screenshots/dashboard_visao_geral.png)

### Dashboard — relação Selic × IPCA
![Selic x IPCA](docs/screenshots/dashboard_selic_ipca.png)

## Como rodar localmente

### Pré-requisitos
- Docker Desktop
- Python 3.10+
- Java 17 (necessário para o PySpark)
- Conta Google (gratuita) para o BigQuery Sandbox

### Setup (Windows / PowerShell)

\`\`\`powershell
# 1. Clone o repositório
git clone https://github.com/davidkleinn/pipeline-indicadores-bcb.git
cd pipeline-indicadores-bcb

# 2. Crie o ambiente virtual e instale as dependências
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 3. Configure as variáveis de ambiente
Copy-Item .env.example .env
# Edite o .env com seus valores (senhas, GCP_PROJECT_ID, etc.)

# 4. Suba a infraestrutura
docker compose up -d
# Aguarde o serviço airflow-init concluir (~1-2 min na primeira vez)

# 5. Acesse a UI do Airflow em localhost:8080

# 6. Execute o backfill histórico (apenas uma vez)
python spark_jobs\backfill_historico.py

# 7. Rode o dbt manualmente para validar (a DAG também roda isso automaticamente)
. .\load_env.ps1
cd dbt\pipeline_bcb
dbt run
dbt test

# 8. Dispare a DAG diária manualmente na UI do Airflow
#    ou aguarde o agendamento @daily
\`\`\`

### Notas específicas de ambiente Windows

Este projeto foi desenvolvido e testado no Windows, e alguns ajustes de ambiente
foram necessários — documentados aqui para quem for reproduzir:

- **PySpark exige `winutils.exe`/`hadoop.dll`** (Hadoop 3.3.6) e a variável
  `HADOOP_HOME` configurada, mesmo rodando 100% local — sem isso, a escrita em
  disco (Parquet) falha com `UnsatisfiedLinkError`.
- **Conexões PostgreSQL usam `127.0.0.1` explícito, não `localhost`** — o Windows
  PT-BR pode resolver `localhost` através do arquivo `hosts` do sistema com
  codificação Windows-1252, o que quebra bibliotecas que esperam UTF-8 (`psycopg2`).
- **A porta do Postgres exposta ao host é `5433`, não a padrão `5432`** (ajustável
  via `POSTGRES_HOST_PORT` no `.env`) — evita conflito com instalações locais de
  Postgres que já ocupam a porta padrão.
- **dbt e Airflow rodam em ambientes Python isolados dentro do container**
  (`/opt/airflow/dbt_venv`), evitando conflito de dependências entre `dbt-core` e
  o `apache-airflow-providers-postgres`.

## Limitações conhecidas

- **Volume de dados:** séries econômicas são naturalmente pequenas (~21 mil
  registros no total). O PySpark foi escolhido para demonstrar a API de DataFrame
  e window functions corretamente — não porque o volume exige processamento
  distribuído. Numa entrevista, essa é a resposta honesta se perguntarem sobre isso.
- **API do BCB:** desde março de 2025 limita consultas a janelas de até 10 anos —
  a paginação automática em `extract/bcb_client.py` (`get_serie_completa`) resolve isso.
- **BigQuery Sandbox:** não suporta DML (`UPDATE`/`MERGE`); a carga usa
  `WRITE_TRUNCATE` (substituição completa) por design. Além disso, **tabelas no
  Sandbox expiram automaticamente após 60 dias** de inatividade — os screenshots em
  `docs/screenshots/` são a evidência permanente do resultado; rode
  `scripts/export_to_bigquery.py` novamente para repopular se necessário.