-- tests/assert_geracao_tables_union.sql
-- Teste para verificar se o union dinâmico está capturando todas as tabelas

WITH expected_tables AS (
    SELECT table_name 
    FROM information_schema.tables 
    WHERE upper(table_schema) = 'STAGING' 
    AND upper(table_name) LIKE 'GERACAO_USINA_%'
),

actual_tables AS (
    SELECT DISTINCT source_table_name AS table_name
    FROM {{ ref('stg_usina_geracao') }}
    WHERE source_table_name != 'NO_TABLES_FOUND'
),

missing_tables AS (
    SELECT table_name
    FROM expected_tables
    WHERE table_name NOT IN (SELECT table_name FROM actual_tables)
)

-- O teste falha se houver tabelas que não foram incluídas no union
SELECT *
FROM missing_tables