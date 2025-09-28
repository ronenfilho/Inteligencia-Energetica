-- models/stg_usina_geracao.sql
-- Union de todas as tabelas de geração de usina
-- Compatível com nomenclatura ONS: anuais (2000-2021) e mensais (2022+)
-- Usa macro union_geracao_tables para centralizar a lógica

{{ union_geracao_tables(schema='STAGING', table_pattern='GERACAO_USINA_2_%') }}