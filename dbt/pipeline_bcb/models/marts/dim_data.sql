with datas as (
    select distinct data
    from {{ ref('int_indicadores_unificados') }}
)

select
    data,
    extract(year  from data)::integer   as ano,
    extract(month from data)::integer   as mes,
    extract(quarter from data)::integer as trimestre,
    extract(dow   from data)::integer   as dia_semana_num,
    to_char(data, 'TMMonth')            as nome_mes,
    to_char(data, 'YYYY-MM')            as ano_mes
from datas