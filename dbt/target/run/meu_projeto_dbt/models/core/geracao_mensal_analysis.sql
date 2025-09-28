
  create or replace   view IE_DB.CORE.geracao_mensal_analysis
  
   as (
    -- models/core/geracao_mensal_analysis.sql
-- Análise mensal de geração consolidando dados anuais e mensais
-- Expande dados anuais em meses para análise consistente

WITH monthly_data AS (
    -- Dados já mensais (2022+)
    SELECT 
        mes_referencia,
        EXTRACT(YEAR FROM mes_referencia) as ano,
        EXTRACT(MONTH FROM mes_referencia) as mes,
        ceg,
        nom_usina,
        nom_tipousina,
        nom_tipocombustivel,
        id_subsistema,
        nom_subsistema,
        id_estado,
        nom_estado,
        SUM(val_geracao_mw) as geracao_total_mw,
        COUNT(*) as total_registros,
        MIN(instante) as primeiro_registro,
        MAX(instante) as ultimo_registro
    FROM IE_DB.STAGING.stg_usina_geracao
    WHERE data_type = 'monthly'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

annual_expanded AS (
    -- Expandir dados anuais em 12 meses
    SELECT 
        DATE_TRUNC('month', 
            DATEADD('month', month_num - 1, 
                DATE_TRUNC('year', mes_referencia)
            )
        ) as mes_referencia,
        EXTRACT(YEAR FROM mes_referencia) as ano,
        month_num as mes,
        ceg,
        nom_usina,
        nom_tipousina,
        nom_tipocombustivel,
        id_subsistema,
        nom_subsistema,
        id_estado,
        nom_estado,
        -- Dividir geração anual por 12 (aproximação)
        SUM(val_geracao_mw) / 12.0 as geracao_total_mw,
        COUNT(*) as total_registros,
        MIN(instante) as primeiro_registro,
        MAX(instante) as ultimo_registro
    FROM IE_DB.STAGING.stg_usina_geracao
    CROSS JOIN (
        SELECT 1 as month_num UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL
        SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL
        SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL
        SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
    ) months
    WHERE data_type = 'annual'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),

consolidated AS (
    SELECT 
        mes_referencia,
        ano,
        mes,
        ceg,
        nom_usina,
        nom_tipousina,
        nom_tipocombustivel,
        id_subsistema,
        nom_subsistema,
        id_estado,
        nom_estado,
        geracao_total_mw,
        total_registros,
        primeiro_registro,
        ultimo_registro,
        'monthly' as source_granularity
    FROM monthly_data
    
    UNION ALL
    
    SELECT 
        mes_referencia,
        ano,
        mes,
        ceg,
        nom_usina,
        nom_tipousina,
        nom_tipocombustivel,
        id_subsistema,
        nom_subsistema,
        id_estado,
        nom_estado,
        geracao_total_mw,
        total_registros,
        primeiro_registro,
        ultimo_registro,
        'annual_expanded' as source_granularity
    FROM annual_expanded
)

SELECT 
    *,
    -- Calcular médias móveis trimestrais
    AVG(geracao_total_mw) OVER (
        PARTITION BY ceg 
        ORDER BY ano, mes 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as geracao_media_3m,
    
    -- Comparação com mesmo mês do ano anterior
    LAG(geracao_total_mw, 12) OVER (
        PARTITION BY ceg, mes 
        ORDER BY ano
    ) as geracao_mesmo_mes_ano_anterior,
    
    -- Ranking mensal por tipo de combustível
    ROW_NUMBER() OVER (
        PARTITION BY ano, mes, nom_tipocombustivel 
        ORDER BY geracao_total_mw DESC
    ) as ranking_combustivel_mes,
    
    CURRENT_TIMESTAMP() as processed_at

FROM consolidated
ORDER BY ano DESC, mes DESC, geracao_total_mw DESC
  );

