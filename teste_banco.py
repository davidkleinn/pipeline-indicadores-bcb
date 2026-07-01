import os
from dotenv import load_dotenv
import psycopg2

# O 'override=True' é a mágica: ele ignora o cache do terminal e FORÇA a leitura do arquivo
load_dotenv(override=True)

senha = os.environ.get("POSTGRES_PASSWORD")
print(f"🕵️ Python está tentando usar a senha: '{senha}'")

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname=os.environ.get("PIPELINE_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=senha
    )
    print("✅ CONEXÃO COM BANCO PERFEITA!")
    conn.close()
except Exception as e:
    print(f"❌ Erro de conexão: {e}")