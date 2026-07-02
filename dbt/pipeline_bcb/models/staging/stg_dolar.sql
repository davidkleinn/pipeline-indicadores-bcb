with fonte as (
    select * from {{ source('raw', 'bcb_historico') }}
    where codigo_serie = 1
)

select
    data,
    valor            as cotacao_dolar_brl,
    media_movel_30d  as dolar_media_30d,
    variacao_pct     as dolar_variacao_pct,
    ano
from fonte