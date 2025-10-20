USE IE_DB.STAGING;

CREATE OR REPLACE VIEW vw_geracao_solar_go AS
WITH base_geracao AS (
    SELECT 
        val_geracao_mw AS geracao_mwh,
        id_ons AS id_usina,
        instante AS medicao_data_hora,
        EXTRACT(MONTH FROM instante) AS mes,
        EXTRACT(YEAR FROM instante) AS ano,
        id_estado AS uf
    FROM stg_usina_geracao
    WHERE nom_tipocombustivel = 'Fotovoltaica'
      AND id_estado = 'GO'
),
base_irradiacao AS (
    SELECT
        uf,
        mes,
        IRRADIACAO_MEDIA_KWH_M2_DIA AS irradiacao_media_diaria,
        TIPO_ANGULO,
        MEDIA,
        DELTA,
        data_carga,
        fonte,
        CASE UPPER(mes)
            WHEN 'JANEIRO'   THEN 1
            WHEN 'FEVEREIRO' THEN 2
            WHEN 'MARCO'     THEN 3
            WHEN 'ABRIL'     THEN 4
            WHEN 'MAIO'      THEN 5
            WHEN 'JUNHO'     THEN 6
            WHEN 'JULHO'     THEN 7
            WHEN 'AGOSTO'    THEN 8
            WHEN 'SETEMBRO'  THEN 9
            WHEN 'OUTUBRO'   THEN 10
            WHEN 'NOVEMBRO'  THEN 11
            WHEN 'DEZEMBRO'  THEN 12
        END AS mes_num
    FROM vw_irradiacao_solar_long
    WHERE uf = 'GO'
)
SELECT
    g.id_usina,
    g.uf,
    g.ano,
    g.mes,
    DATE_FROM_PARTS(g.ano, g.mes, 1) AS data_mes,
    g.medicao_data_hora,
    g.geracao_mwh,

    -- Colunas adicionais por tipo de ângulo
    i_plano.irradiacao_media_diaria AS plano_horizontal,
    i_latitude.irradiacao_media_diaria AS angulo_igual_a_latitude,
    i_media.irradiacao_media_diaria AS maior_media_anual,
    i_minimo.irradiacao_media_diaria AS maior_minimo_mensal,

    -- Média e Delta gerais (usando Plano Horizontal como referência)
    i_plano.MEDIA AS media,
    i_plano.DELTA AS delta,
/*
    i_plano.data_carga AS data_carga,
    i_plano.fonte AS fonte
*/
FROM base_geracao g
LEFT JOIN base_irradiacao i_plano
    ON g.uf = i_plano.uf
   AND g.mes = i_plano.mes_num
   AND i_plano.TIPO_ANGULO = 'Plano Horizontal'
LEFT JOIN base_irradiacao i_latitude
    ON g.uf = i_latitude.uf
   AND g.mes = i_latitude.mes_num
   AND i_latitude.TIPO_ANGULO = 'Ângulo igual a latitude'
LEFT JOIN base_irradiacao i_media
    ON g.uf = i_media.uf
   AND g.mes = i_media.mes_num
   AND i_media.TIPO_ANGULO = 'Maior média anual'
LEFT JOIN base_irradiacao i_minimo
    ON g.uf = i_minimo.uf
   AND g.mes = i_minimo.mes_num
   AND i_minimo.TIPO_ANGULO = 'Maior mínimo mensal';

SELECT * FROM vw_geracao_solar_go;