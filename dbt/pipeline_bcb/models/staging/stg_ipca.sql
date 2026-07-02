-- IPCA é mensal: média móvel de 30 linhas seria 30 meses, não 30 dias.
-- Por isso media_movel_30d é NULL para essa série — comportamento esperado,
-- decidido já na Fase 3 (spark_jobs/backfill_historico.py).
with fonte as (
    select * from {{ source('raw', 'bcb_historico') }}
    where codigo_serie = 433
)

select
    data,
    valor            as inflacao_ipca_pct,
    media_movel_30d,  -- sempre NULL para esta série; mantido só por consistência de schema
    variacao_pct     as ipca_variacao_mensal,
    ano
from fonte