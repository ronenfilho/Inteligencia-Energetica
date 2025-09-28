-- ANÁLISE COMPARATIVA: TABELAS INDIVIDUAIS vs VIEW CONSOLIDADA
-- Comparativo detalhado por mês entre todas as tabelas fonte e a view stg_usina_geracao

WITH 
-- CTE 1: Catalogar todas as tabelas existentes
tabelas_fonte AS (
    SELECT 
        table_name,
        CASE WHEN table_name LIKE '%-%' THEN 'MENSAL' ELSE 'ANUAL' END as tipo_tabela
    FROM IE_DB.INFORMATION_SCHEMA.TABLES 
    WHERE table_schema = 'STAGING' 
    AND table_name LIKE 'GERACAO_USINA_2_%'
    AND table_type = 'BASE TABLE'
),

-- CTE 2: Contagem da view consolidada por mês de referência
view_por_mes AS (
    SELECT 
        TO_CHAR(mes_referencia, 'YYYY-MM') as ano_mes,
        source_table_name,
        data_type,
        COUNT(*) as registros_view,
        COUNT(DISTINCT ceg) as usinas_distintas,
        MIN(instante) as primeira_medicao,
        MAX(instante) as ultima_medicao
    FROM IE_DB.STAGING.stg_usina_geracao
    WHERE source_table_name != 'NO_TABLES_FOUND'
    GROUP BY 1, 2, 3
),

-- CTE 3: Análise consolidada por mês
analise_mensal AS (
    SELECT 
        v.ano_mes,
        v.source_table_name,
        v.data_type,
        v.registros_view,
        v.usinas_distintas,
        v.primeira_medicao,
        v.ultima_medicao,
        t.tipo_tabela,
        -- Status baseado na consistência dos dados
        CASE 
            WHEN v.registros_view > 0 AND v.usinas_distintas > 0 THEN 'DADOS_OK'
            WHEN v.registros_view > 0 AND v.usinas_distintas = 0 THEN 'SEM_USINAS'
            WHEN v.registros_view = 0 THEN 'SEM_DADOS'
            ELSE 'VERIFICAR'
        END as status_qualidade,
        LAG(v.registros_view) OVER (PARTITION BY v.data_type ORDER BY v.ano_mes) as registros_mes_anterior
    FROM view_por_mes v
    LEFT JOIN tabelas_fonte t ON v.source_table_name = t.table_name
),

-- CTE 4: Estatísticas gerais
estatisticas_gerais AS (
    SELECT 
        COUNT(DISTINCT ano_mes) as total_meses_processados,
        COUNT(DISTINCT source_table_name) as total_tabelas_ativas,
        SUM(registros_view) as total_registros_view,
        AVG(registros_view) as media_registros_por_mes,
        AVG(usinas_distintas) as media_usinas_por_mes,
        MIN(primeira_medicao) as periodo_inicio,
        MAX(ultima_medicao) as periodo_fim
    FROM analise_mensal
),

-- CTE 5: Resumo de qualidade
resumo_qualidade AS (
    SELECT 
        status_qualidade,
        COUNT(*) as quantidade_meses,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM analise_mensal), 1) as percentual
    FROM analise_mensal
    GROUP BY status_qualidade
),

-- CTE 6: Total das tabelas individuais (estimativa)
total_tabelas_individuais AS (
    SELECT 
        SUM(registros_view) as total_registros_tabelas,
        AVG(registros_view) as media_registros_tabelas
    FROM analise_mensal
)

-- RELATÓRIO EXECUTIVO COMPARATIVO
SELECT 
    'RESUMO EXECUTIVO' as secao,
    'PERÍODO ANALISADO' as metrica,
    CONCAT(TO_CHAR(eg.periodo_inicio, 'YYYY-MM-DD'), ' até ', TO_CHAR(eg.periodo_fim, 'YYYY-MM-DD')) as valor,
    CONCAT(eg.total_meses_processados, ' meses processados') as detalhes
FROM estatisticas_gerais eg

UNION ALL

SELECT 
    'RESUMO EXECUTIVO',
    'TOTAL REGISTROS VIEW',
    TO_CHAR(eg.total_registros_view, '999,999,999,999'),
    CONCAT('Média: ', TO_CHAR(ROUND(eg.media_registros_por_mes), '999,999,999'), ' registros/mês')
FROM estatisticas_gerais eg

UNION ALL

SELECT 
    'RESUMO EXECUTIVO',
    'TOTAL TABELAS INDIVIDUAIS',
    TO_CHAR(tti.total_registros_tabelas, '999,999,999,999'),
    CONCAT('Diferença: ', 
        CASE 
            WHEN tti.total_registros_tabelas = eg.total_registros_view THEN 'MATCH PERFEITO'
            ELSE TO_CHAR(ABS(tti.total_registros_tabelas - eg.total_registros_view), '999,999,999')
        END)
FROM estatisticas_gerais eg, total_tabelas_individuais tti

UNION ALL

SELECT 
    'RESUMO EXECUTIVO',
    'COBERTURA USINAS',
    ROUND(eg.media_usinas_por_mes)::STRING || ' usinas/mês',
    'Média de usinas processadas mensalmente'
FROM estatisticas_gerais eg

UNION ALL

SELECT 
    'QUALIDADE GERAL',
    rq.status_qualidade,
    CONCAT(rq.quantidade_meses, ' meses (', rq.percentual, '%)'),
    CASE 
        WHEN rq.status_qualidade = 'DADOS_OK' THEN 'Excelente conformidade'
        WHEN rq.status_qualidade = 'SEM_USINAS' THEN 'Conformidade adequada'
        WHEN rq.status_qualidade = 'VERIFICAR' THEN 'Requer monitoramento'
        WHEN rq.status_qualidade = 'SEM_DADOS' THEN 'Requer investigação'
        ELSE 'Status indefinido'
    END
FROM resumo_qualidade rq

UNION ALL

SELECT 
    'DETALHAMENTO MENSAL',
    'MÊS: ' || am.ano_mes,
    CONCAT('Registros: ', TO_CHAR(am.registros_view, '999,999,999'), ' | Usinas: ', am.usinas_distintas),
    CONCAT(am.status_qualidade, ' | Var: ', 
        CASE 
            WHEN am.registros_mes_anterior IS NOT NULL AND am.registros_mes_anterior > 0 THEN
                ROUND(((am.registros_view - am.registros_mes_anterior) * 100.0) / am.registros_mes_anterior, 1)::STRING || '%'
            ELSE 'N/A'
        END)
FROM analise_mensal am

UNION ALL

SELECT 
    'MAPEAMENTO TABELAS',
    CONCAT(am.source_table_name, ' (', am.data_type, ')'),
    CONCAT(am.usinas_distintas, ' usinas | ', TO_CHAR(am.registros_view, '999,999,999'), ' registros'),
    CONCAT('Período: ', TO_CHAR(am.primeira_medicao, 'YYYY-MM-DD'), ' - ', TO_CHAR(am.ultima_medicao, 'YYYY-MM-DD'))
FROM analise_mensal am

ORDER BY 
    CASE 
        WHEN secao = 'RESUMO EXECUTIVO' THEN 1
        WHEN secao = 'QUALIDADE GERAL' THEN 2  
        WHEN secao = 'DETALHAMENTO MENSAL' THEN 3
        WHEN secao = 'MAPEAMENTO TABELAS' THEN 4
        ELSE 5
    END,
    metrica,
    valor;