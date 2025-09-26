-- Teste para validar que valores de geração não sejam negativos
-- Permite valores nulos (usinas que não estão gerando)

SELECT *
FROM IE_DB.STAGING.stg_usina_geracao
WHERE val_geracao_mw < 0