COPY INTO @~/dados_geracao_go.csv
FROM (
    SELECT 
        mes_referencia,
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
        val_geracao_mw
    FROM IE_DB.STAGING.stg_usina_geracao_go
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
HEADER = TRUE;
