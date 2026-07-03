# dags/bcb_daily_pipeline.py
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

# ─── Definição da DAG ──────────────────────────────────────────────────────
@dag(
    dag_id="bcb_daily_pipeline",
    schedule="@daily",
    # start_date NUNCA pode ser datetime.now() — o Airflow calcula runs pendentes
    # a partir dessa data. Uma data dinâmica causa comportamento imprevisível.
    start_date=datetime(2024, 1, 1),
    catchup=False,   # não executa runs retroativas desde start_date
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bcb", "extracao"],
)
def bcb_daily_pipeline():
    """
    Pipeline diário de indicadores econômicos do Banco Central do Brasil.

    Fase atual (2b):
        extract_series → load_raw_to_postgres

    Fase 4 (pendente):
        ... → dbt_run → dbt_test
    """

    # ── Task 1: Extração ───────────────────────────────────────────────────
    @task()
    def extract_series() -> dict:
        """
        Chama extract_today() e retorna os dados via XCom.
        O Airflow serializa o dict como JSON automaticamente.
        """
        # Import dentro da função = acontece em runtime, não no parse da DAG.
        # Isso evita que um erro de import quebre o carregamento de TODAS as DAGs.
        from extract.extract_daily import extract_today
        return extract_today()

    # ── Task 2: Carga idempotente no Postgres ──────────────────────────────
    @task()
    def load_raw_to_postgres(dados: dict) -> None:
        """
        Insere os dados em raw.bcb_series.

        ON CONFLICT DO NOTHING garante idempotência:
        rodar a DAG duas vezes no mesmo dia não duplica nenhuma linha.
        """
        INSERT_SQL = """
            INSERT INTO raw.bcb_series (codigo_serie, data, valor, data_extracao)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (codigo_serie, data) DO NOTHING;
        """

        # PostgresHook lê a connection "pipeline_bcb_pg" que definimos via
        # variável de ambiente AIRFLOW_CONN_PIPELINE_BCB_PG no docker-compose.yml.
        # Nenhuma credencial está hardcoded aqui.
        hook = PostgresHook(postgres_conn_id="pipeline_bcb_pg")
        conn = hook.get_conn()
        cursor = conn.cursor()

        total_inseridos = 0
        total_ignorados = 0

        try:
            for codigo_str, registros in dados.items():
                for item in registros:
                    cursor.execute(INSERT_SQL, (
                        int(codigo_str),   # "1" → 1
                        item["data"],      # "2025-06-25" → Postgres converte pra DATE
                        float(item["valor"]),
                    ))
                    # cursor.rowcount == 1 → linha inserida
                    # cursor.rowcount == 0 → conflito, ignorado (ON CONFLICT DO NOTHING)
                    if cursor.rowcount > 0:
                        total_inseridos += 1
                    else:
                        total_ignorados += 1

            conn.commit()
            print(f"Inseridos: {total_inseridos} | Ignorados (já existiam): {total_ignorados}")

        except Exception:
            conn.rollback()
            raise  # relança para o Airflow registrar a falha e aplicar retry
        finally:
            cursor.close()
            conn.close()

    # ── Task 3 e 4: transformação via dbt ──────────────────────────────────
    # BashOperator (não astronomer-cosmos) por simplicidade — suficiente
    # pra um projeto deste porte, sem dependência pesada adicional.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt/pipeline_bcb && /opt/airflow/dbt_venv/bin/dbt run --select marts",
        env={
            "POSTGRES_USER": os.environ["POSTGRES_USER"],
            "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
            "PIPELINE_DB": os.environ["PIPELINE_DB"],
            # Aponta pra rede interna do Docker, não pro 127.0.0.1:5433
            # que só existe fora do container (ver profiles.yml).
            "DBT_POSTGRES_HOST": "postgres",
            "DBT_POSTGRES_PORT": "5432",
            "POSTGRES_HOST_PORT": "5432", # <-- Adicione esta linha também!
            # dbt procura profiles.yml em $DBT_PROFILES_DIR — mais direto
            # e explícito do que manipular a variável HOME.
            "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt/pipeline_bcb && /opt/airflow/dbt_venv/bin/dbt test --select marts",
        env={
            "POSTGRES_USER": os.environ["POSTGRES_USER"],
            "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
            "PIPELINE_DB": os.environ["PIPELINE_DB"],
            "DBT_POSTGRES_HOST": "postgres",
            "DBT_POSTGRES_PORT": "5432",
            "POSTGRES_HOST_PORT": "5432", # <-- Adicione esta linha também!
            "DBT_PROFILES_DIR": "/opt/airflow/.dbt",
        },
    )

    # ── Orquestração: define a dependência entre as tasks ──────────────────
    # A TaskFlow API passa o retorno de extract_series() como argumento
    # para load_raw_to_postgres() automaticamente via XCom.
    dados = extract_series()
    tabela_carregada = load_raw_to_postgres(dados)
    tabela_carregada >> dbt_run >> dbt_test

# Instancia a DAG — sem isso o Airflow não a reconhece
dag_instance = bcb_daily_pipeline()