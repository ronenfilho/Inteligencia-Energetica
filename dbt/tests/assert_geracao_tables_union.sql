-- tests/assert_geracao_tables_union.sql
-- Teste para verificar se o union dinâmico está capturando todas as tabelas
-- Compatível com nomenclatura ONS (anuais e mensais)

WITH expected_tables AS (
    SELECT table_name 
    FROM information_schema.tables 
    WHERE upper(table_schema) = 'STAGING' 
    AND upper(table_name) LIKE 'GERACAO_USINA_2_%'
),

actual_tables AS (
    SELECT DISTINCT source_table_name AS table_name
    FROM {{ ref('stg_usina_geracao') }}
    WHERE source_table_name != 'NO_TABLES_FOUND'
),

missing_tables AS (
    SELECT 
        table_name,
        CASE 
            WHEN table_name LIKE '%-%' THEN 'monthly'
            ELSE 'annual'
        END as expected_type,
        'Tabela não incluída no union' as error_message
    FROM expected_tables
    WHERE table_name NOT IN (SELECT table_name FROM actual_tables)
),

-- Também verificar se há inconsistências de tipo
type_mismatches AS (
    SELECT 
        a.table_name,
        'Tipo incorreto detectado' as error_message
    FROM actual_tables a
    JOIN {{ ref('stg_usina_geracao') }} sg ON a.table_name = sg.source_table_name
    WHERE (
        (a.table_name LIKE '%-%' AND sg.data_type != 'monthly')
        OR
        (a.table_name NOT LIKE '%-%' AND sg.data_type != 'annual')
    )
    GROUP BY a.table_name
)

-- O teste falha se houver tabelas faltando ou com tipos incorretos
SELECT table_name, error_message FROM missing_tables
UNION ALL
SELECT table_name, error_message FROM type_mismatches