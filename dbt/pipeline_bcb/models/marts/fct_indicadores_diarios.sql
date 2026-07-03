select
    f.data,
    f.indicador,
    f.frequencia,
    f.valor,
    f.ano,
    d.mes,
    d.trimestre,
    d.nome_mes,
    d.ano_mes,
    d.dia_semana_num
from {{ ref('int_indicadores_unificados') }} f
left join {{ ref('dim_data') }} d on f.data = d.data