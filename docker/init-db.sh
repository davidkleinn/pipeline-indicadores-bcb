#!/bin/bash
set -e

# Cria o banco de dados do pipeline
# (o banco 'airflow' já foi criado automaticamente via POSTGRES_DB)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE pipeline_bcb;
EOSQL

# Cria o schema raw dentro do pipeline_bcb
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "pipeline_bcb" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw;
EOSQL

echo "Init: banco pipeline_bcb e schema raw criados com sucesso."