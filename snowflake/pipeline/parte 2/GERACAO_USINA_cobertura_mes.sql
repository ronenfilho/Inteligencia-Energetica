-- Consulta objetiva para verificar cobertura mensal
-- Usando IE_DB.STAGING diretamente

WITH meses_disponiveis AS (
    SELECT DISTINCT
        CASE 
            WHEN table_name LIKE '%-%' THEN 
                -- Mensal: GERACAO_USINA_2_2022-01
                RIGHT(table_name, 7)  -- Pega 2022-01
            ELSE 
                -- Anual: GERACAO_USINA_2_2000 -> expandir em 12 meses
                RIGHT(table_name, 4) || '-' || LPAD(m.mes, 2, '0')
        END as ano_mes
    FROM IE_DB.INFORMATION_SCHEMA.TABLES 
    CROSS JOIN (
        SELECT 1 as mes UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL
        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL  
        SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL
        SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
    ) m
    WHERE table_schema = 'STAGING' 
    AND table_name LIKE 'GERACAO_USINA_2_%'
    AND (
        table_name LIKE '%-%' OR  -- Dados mensais
        table_name NOT LIKE '%-%'  -- Dados anuais (todos os meses)
    )
),

periodo_completo AS (
    SELECT 
        MIN(ano_mes) as inicio,
        MAX(ano_mes) as fim
    FROM meses_disponiveis
)

-- RESULTADO: Verificar cobertura
SELECT 
    'RESUMO' as tipo,
    COUNT(*) as total_meses,
    MIN(ano_mes) as primeiro_mes,
    MAX(ano_mes) as ultimo_mes,
    NULL as mes_especifico
FROM meses_disponiveis

UNION ALL

SELECT 
    'DISPONÍVEL' as tipo,
    NULL as total_meses,
    NULL as primeiro_mes, 
    NULL as ultimo_mes,
    ano_mes as mes_especifico
FROM meses_disponiveis
ORDER BY tipo DESC, mes_especifico;