-- 2025-09-27
SELECT COUNT(*) FROM IE_DB.STAGING.stg_usina_geracao_go; 
-- 42288
SELECT COUNT(*) FROM IE_DB.STAGING.stg_usina_geracao; 
-- 80.886.723 

-- consulta para extração 
SELECT 
    val_geracao_mw as geracao_mwh
    , id_ons as id_usina
    , instante as medicao_data_hora
    /*mes_referencia,
    id_subsistema,
    nom_subsistema,
    id_estado,
    nom_estado,
    id_ons,
    ceg,
    nom_usina,
    nom_tipousina,
    nom_tipocombustivel,
    cod_modalidadeoperacao,
    instante,
    val_geracao_mw*/
FROM IE_DB.STAGING.stg_usina_geracao
;
--limit 10;



SELECT 
    EXTRACT(YEAR FROM mes_referencia) AS ano,
    EXTRACT(MONTH FROM mes_referencia) AS mes
FROM IE_DB.STAGING.stg_usina_geracao_go
GROUP BY 
    EXTRACT(YEAR FROM mes_referencia),
    EXTRACT(MONTH FROM mes_referencia)
ORDER BY 
    ano,
    mes;

SELECT * FROM IE_DB.STAGING.GERACAO_USINA_2_2000
UNION ALL 
SELECT * FROM IE_DB.STAGING."GERACAO_USINA_2_2022-01"
;

SELECT * FROM IE_DB.STAGING."GERACAO_USINA_2_2023-04"
WHERE ID_ESTADO = 'GO' AND nom_tipocombustivel = 'Fotovoltaica';

-- consulta para extração
SELECT 
    val_geracao_mw as geracao_mwh
    , id_ons as id_usina
    , instante as medicao_data_hora
FROM IE_DB.STAGING.stg_usina_geracao
WHERE 
    nom_tipocombustivel = 'Fotovoltaica'
    AND ID_ESTADO = 'GO' 
LIMIT 10;

-- 
SELECT 
    distinct nom_usina
FROM IE_DB.STAGING.stg_usina_geracao
WHERE 
    nom_tipocombustivel = 'Fotovoltaica'
    AND ID_ESTADO = 'GO' 
LIMIT 10;