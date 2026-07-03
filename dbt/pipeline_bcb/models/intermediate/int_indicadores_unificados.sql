-- models/intermediate/int_indicadores_unificados.sql
with selic as (
    select
        data,
        taxa_selic_pct      as valor,
        'Selic'             as indicador,
        'diario'            as frequencia,
        ano
    from {{ ref('stg_selic') }}
),

dolar as (
    select
        data,
        cotacao_dolar_brl   as valor,
        'Dolar'             as indicador,
        'diario'            as frequencia,
        ano
    from {{ ref('stg_dolar') }}
),

ipca as (
    select
        data,
        inflacao_ipca_pct   as valor,
        'IPCA'              as indicador,
        'mensal'            as frequencia,
        ano
    from {{ ref('stg_ipca') }}
)

select * from selic
union all
select * from dolar
union all
select * from ipca