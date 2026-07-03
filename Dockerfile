FROM apache/airflow:2.9.3

# Muda para o usuário root para instalar pacotes do Linux
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Volta para o usuário do airflow
USER airflow

# 1. Instala o provider do Postgres no ambiente principal do Airflow
RUN pip install --no-cache-dir apache-airflow-providers-postgres

# 2. Cria um ambiente virtual isolado apenas para o dbt
RUN python -m venv /opt/airflow/dbt_venv
RUN /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core dbt-postgres