with mensal as (
    select
        ano_mes,
        indicador,
        frequencia,
        round(avg(valor)::numeric, 4)  as valor_medio,
        round(min(valor)::numeric, 4)  as valor_minimo,
        round(max(valor)::numeric, 4)  as valor_maximo,
        count(*)                       as qtd_observacoes
    from {{ ref('fct_indicadores_diarios') }}
    group by 1, 2, 3
)

select
    ano_mes,
    indicador,
    frequencia,
    valor_medio,
    valor_minimo,
    valor_maximo,
    qtd_observacoes,
    lag(valor_medio) over (
        partition by indicador
        order by ano_mes
    ) as valor_medio_mes_anterior,
    round(
        (valor_medio - lag(valor_medio) over (partition by indicador order by ano_mes))
        / nullif(lag(valor_medio) over (partition by indicador order by ano_mes), 0)
        * 100,
        2
    ) as variacao_pct_mensal
from mensal