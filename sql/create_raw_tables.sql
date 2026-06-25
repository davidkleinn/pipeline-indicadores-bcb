CREATE TABLE IF NOT EXISTS raw.bcb_series (
    codigo_serie    INTEGER     NOT NULL,
    data            DATE        NOT NULL,
    valor           NUMERIC     NOT NULL,
    data_extracao   TIMESTAMP   NOT NULL DEFAULT now(),
    UNIQUE (codigo_serie, data)
);