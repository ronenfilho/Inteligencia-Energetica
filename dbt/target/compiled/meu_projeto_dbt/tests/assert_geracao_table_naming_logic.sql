-- Teste para validar a lógica de union das tabelas de geração
-- Verifica se tabelas anuais e mensais são processadas corretamente

SELECT 
    source_table_name,
    data_type,
    mes_referencia,
    COUNT(*) as record_count,
    MIN(instante) as min_instante,
    MAX(instante) as max_instante
FROM IE_DB.STAGING.stg_usina_geracao
WHERE source_table_name != 'NO_TABLES_FOUND'
GROUP BY source_table_name, data_type, mes_referencia
HAVING 
    -- Validar que tabelas anuais têm data_type = 'annual'
    (source_table_name NOT LIKE '%-%' AND data_type != 'annual')
    OR
    -- Validar que tabelas mensais têm data_type = 'monthly'  
    (source_table_name LIKE '%-%' AND data_type != 'monthly')
    OR
    -- Validar que não há registros sem data
    record_count = 0