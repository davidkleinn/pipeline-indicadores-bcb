with fonte as (
    select * from {{ source('raw', 'bcb_historico') }}
    where codigo_serie = 11
)

select
    data,
    valor            as taxa_selic_pct,
    media_movel_30d  as selic_media_30d,
    variacao_pct     as selic_variacao_pct,
    ano
from fonte