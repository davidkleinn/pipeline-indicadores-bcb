# scripts/export_to_bigquery.py

import os
from pathlib import Path

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env", encoding="utf-8")

# 127.0.0.1 + porta do .env: mesmo motivo das Fases 3/4 — "localhost" no
# Windows PT-BR pode disparar resolução de nome problemática, e a porta
# real mapeada pelo Docker Compose neste projeto é 5433, não 5432.
PG_HOST = "127.0.0.1"
PG_PORT = os.environ["POSTGRES_HOST_PORT"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASS = os.environ["POSTGRES_PASSWORD"]
PG_DB   = os.environ["PIPELINE_DB"]

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET    = "indicadores_bcb"

TABELAS = [
    "fct_indicadores_diarios",
    "dim_data",
    "mart_indicadores_mensal",
]


def main():
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    client = bigquery.Client(project=PROJECT_ID)

    for tabela in TABELAS:
        print(f"Exportando staging.{tabela} → {DATASET}.{tabela} ...")

        df = pd.read_sql_table(tabela, engine, schema="staging")

        destino = f"{PROJECT_ID}.{DATASET}.{tabela}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # substitui a tabela inteira a cada carga
            autodetect=True,
        )

        job = client.load_table_from_dataframe(df, destino, job_config=job_config)
        job.result()  # bloqueia até o job terminar

        print(f"  ✓ {len(df)} linhas carregadas em {destino}")


if __name__ == "__main__":
    main()