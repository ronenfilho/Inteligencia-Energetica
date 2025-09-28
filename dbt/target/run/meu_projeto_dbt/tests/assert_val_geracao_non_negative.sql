select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- Teste para validar que valores de geração não sejam negativos
-- Permite valores nulos (usinas que não estão gerando)
-- Inclui informações sobre o tipo de dados e origem

SELECT 
    source_table_name,
    data_type,
    mes_referencia,
    instante,
    ceg,
    nom_usina,
    val_geracao_mw,
    'Valor negativo de geração encontrado' as error_message
FROM IE_DB.STAGING.stg_usina_geracao
WHERE val_geracao_mw < 0
      
    ) dbt_internal_test