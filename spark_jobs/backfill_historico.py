# spark_jobs/backfill_historico.py

import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env", encoding="utf-8")

print("PIPELINE_DB:", os.environ.get("PIPELINE_DB"))
print("POSTGRES_USER:", os.environ.get("POSTGRES_USER"))
print("POSTGRES_PASSWORD:", repr(os.environ.get("POSTGRES_PASSWORD")))

RAW_DIR = ROOT / "data" / "raw" / "historico"
OUTPUT_PARQUET = ROOT / "data" / "processed" / "historico_indicadores"

# Só essas séries são diárias — só elas recebem média móvel de 30 dias.
SERIES_DIARIAS = [1, 11]


# ─── Leitura e união das 3 séries ──────────────────────────────────────────
def ler_series(spark: SparkSession):
    arquivos = {
        1:   RAW_DIR / "1.json",
        11:  RAW_DIR / "11.json",
        433: RAW_DIR / "433.json",
    }

    dfs = []
    for codigo, caminho in arquivos.items():
        # multiLine=True é obrigatório: o JSON foi salvo com indent=2,
        # ou seja, é um array formatado em várias linhas. Sem essa opção,
        # o Spark tenta ler linha a linha e o resultado vem corrompido
        # (coluna _corrupt_record).
        df = (
            spark.read
            .option("multiLine", True)
            .json(str(caminho))
            .withColumn("codigo_serie", F.lit(codigo))
        )
        dfs.append(df)

    df_unificado = dfs[0]
    for df in dfs[1:]:
        df_unificado = df_unificado.unionByName(df)

    return df_unificado


# ─── Casts e coluna derivada de ano ────────────────────────────────────────
def aplicar_casts(df):
    return (
        df
        # "yyyy-MM-dd": formato que o extract_historico.py de fato gravou
        # (ISO), não "dd/MM/yyyy" da API original.
        .withColumn("data", F.to_date("data", "yyyy-MM-dd"))
        .withColumn("valor", F.col("valor").cast(DoubleType()))
        .withColumn("ano", F.year("data"))
    )


# ─── Window functions ──────────────────────────────────────────────────────
def aplicar_window_functions(df):
    janela = Window.partitionBy("codigo_serie").orderBy("data")
    janela_30_linhas = janela.rowsBetween(-29, 0)

    df = df.withColumn("valor_anterior", F.lag("valor").over(janela))

    # Variação percentual: válida pra TODAS as séries — lag() respeita a
    # granularidade própria de cada uma automaticamente.
    df = df.withColumn(
        "variacao_pct",
        F.when(
            # Adicionamos a regra: o valor não pode ser nulo E NÃO PODE SER ZERO
            F.col("valor_anterior").isNotNull() & (F.col("valor_anterior") != 0),
            (F.col("valor") - F.col("valor_anterior")) / F.col("valor_anterior") * 100,
        ),
    )

    # Média móvel: só pras séries diárias. rowsBetween(-29, 0) pega 30
    # LINHAS anteriores (incluindo a atual), não 30 dias de calendário —
    # como Selic/Dólar têm furos em fins de semana/feriados, isso é, na
    # prática, uma média móvel de ~6 semanas úteis. Pra IPCA o mesmo
    # cálculo representaria 30 meses, daí o filtro abaixo.
    df = df.withColumn(
        "media_movel_30d",
        F.when(F.col("codigo_serie").isin(*SERIES_DIARIAS), F.avg("valor").over(janela_30_linhas)),
    )

    return df.drop("valor_anterior")

# ─── Escrita em Parquet particionado ───────────────────────────────────────
def escrever_parquet(df):
    print("Convertendo para Pandas para gravar o Parquet (driblando o Hadoop no Windows)...")
    pdf = df.select(
        "codigo_serie", "data", "valor", "media_movel_30d", "variacao_pct", "ano"
    ).toPandas()
    
    # O Pandas consegue salvar em pastas particionadas igualzinho ao Spark!
    pdf.to_parquet(
        str(OUTPUT_PARQUET),
        engine="pyarrow",
        partition_cols=["ano"],
        index=False
    )
    print(f"Parquet gravado em {OUTPUT_PARQUET}")


# ─── Carga no Postgres: truncate + reload ──────────────────────────────────
# ─── Carga no Postgres: truncate + reload ──────────────────────────────────
def carregar_postgres(df):
    print("Convertendo para Pandas para inserção...")
    pdf = df.select(
        "codigo_serie", "data", "valor", "media_movel_30d", "variacao_pct", "ano"
    ).toPandas()

    # O FIX DO DATETIME: Força a conversão pra datetime do pandas e pega a data
    pdf["data"] = pd.to_datetime(pdf["data"]).dt.date
    
    # Transforma NaN (nulo do Pandas) em None (NULL do Postgres)
    pdf = pdf.where(pd.notnull(pdf), None)

    # Limpeza agressiva das credenciais pra tirar qualquer aspa invisível
    import os
    db = os.environ.get("PIPELINE_DB", "pipeline_bcb").replace("'", "").replace('"', '').strip()
    user = os.environ.get("POSTGRES_USER", "airflow").replace("'", "").replace('"', '').strip()
    pwd = os.environ.get("POSTGRES_PASSWORD", "airflow").replace("'", "").replace('"', '').strip()

    print("Conectando no PostgreSQL via psycopg2 (o que funciona de verdade)...")
    import psycopg2
    from psycopg2.extras import execute_values
    
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        dbname=db,
        user=user,
        password=pwd
    )
    conn.autocommit = True
    cur = conn.cursor()

    try:
        print("Esvaziando tabela raw.bcb_historico (Truncate)...")
        cur.execute("TRUNCATE TABLE raw.bcb_historico;")

        registros = list(pdf.itertuples(index=False, name=None))
        print("Inserindo dados em lote...")
        
        execute_values(
            cur,
            """
            INSERT INTO raw.bcb_historico (codigo_serie, data, valor, media_movel_30d, variacao_pct, ano)
            VALUES %s
            """,
            registros,
            page_size=2000
        )
        print(f"🎉 ACABOU A TORTURA! SUCESSO ABSOLUTO! {len(registros)} linhas no banco!")
    finally:
        cur.close()
        conn.close()


# ─── Orquestração local do job ─────────────────────────────────────────────
def main():
    spark = (
        SparkSession.builder
        .appName("backfill_historico_bcb")
        .master("local[*]")
        # Dataset pequeno: as 200 partições padrão de shuffle geram overhead
        # de scheduling sem benefício nenhum nesse volume de dados.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = ler_series(spark)
        df = aplicar_casts(df)
        df = aplicar_window_functions(df)

        # Reaproveitado em count(), escrita Parquet e carga Postgres —
        # sem cache, o Spark recalcularia leitura+casts+window 3 vezes.
        df.cache()

        total = df.count()
        print(f"Total de registros unificados: {total}")

        escrever_parquet(df)
        carregar_postgres(df)

        df.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()