-- models/core/fact_geracao_summary.sql
-- Fato agregado de geração por período com análise temporal

SELECT 
    mes_referencia,
    data_type,
    source_table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT ceg) as total_usinas,
    COUNT(DISTINCT id_subsistema) as total_subsistemas,
    COUNT(DISTINCT nom_estado) as total_estados,
    
    -- Estatísticas de geração
    COUNT(val_geracao_mw) as records_with_generation,
    SUM(val_geracao_mw) as total_generation_mw,
    AVG(val_geracao_mw) as avg_generation_mw,
    MIN(val_geracao_mw) as min_generation_mw,
    MAX(val_geracao_mw) as max_generation_mw,
    
    -- Cobertura temporal
    MIN(instante) as period_start,
    MAX(instante) as period_end,
    DATEDIFF('day', MIN(instante), MAX(instante)) + 1 as period_days,
    
    -- Qualidade dos dados
    ROUND(
        (COUNT(val_geracao_mw) * 100.0) / COUNT(*), 2
    ) as generation_completeness_pct,
    
    -- Metadados
    CURRENT_TIMESTAMP() as processed_at

FROM {{ ref('stg_usina_geracao') }}
WHERE source_table_name != 'NO_TABLES_FOUND'
GROUP BY mes_referencia, data_type, source_table_name
ORDER BY mes_referencia DESC, source_table_name