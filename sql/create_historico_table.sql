-- sql/create_historico_table.sql

CREATE TABLE IF NOT EXISTS raw.bcb_historico (
    codigo_serie    INTEGER   NOT NULL,
    data            DATE      NOT NULL,
    valor           NUMERIC   NOT NULL,
    media_movel_30d NUMERIC,
    variacao_pct    NUMERIC,
    ano             INTEGER   NOT NULL,
    UNIQUE (codigo_serie, data)
);