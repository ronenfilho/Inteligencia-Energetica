create or replace view VW_GERACAO_SOLAR_GO_PIVOT(
	ID_USINA,
	UF,
	ANO,
	MES,
	DATA_MES,
	MEDICAO_DATA_HORA,
	GERACAO_MWH,
	IRRADIACAO_PLANO_HORIZONTAL,
	IRRADIACAO_ANGULO_LATITUDE,
	IRRADIACAO_MAIOR_MEDIA_ANUAL,
	IRRADIACAO_MAIOR_MINIMO_MENSAL,
	INCLINACAO,
	DATA_CARGA_IRRADIACAO,
	FONTE_IRRADIACAO
) as
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
        INCLINACAO,
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
    p.PLANO_HORIZONTAL        AS irradiacao_plano_horizontal,
    p.ANGULO_IGUAL_LATITUDE   AS irradiacao_angulo_latitude,
    p.MAIOR_MEDIA_ANUAL       AS irradiacao_maior_media_anual,
    p.MAIOR_MINIMO_MENSAL     AS irradiacao_maior_minimo_mensal,
    p.inclinacao,
    p.data_carga AS data_carga_irradiacao,
    p.fonte AS fonte_irradiacao
FROM base_geracao g
LEFT JOIN (
    SELECT
        uf,
        mes_num,
        MAX(CASE WHEN TIPO_ANGULO = 'Plano Horizontal' THEN irradiacao_media_diaria END) AS PLANO_HORIZONTAL,
        MAX(CASE WHEN TIPO_ANGULO = 'Ângulo igual a latitude' THEN irradiacao_media_diaria END) AS ANGULO_IGUAL_LATITUDE,
        MAX(CASE WHEN TIPO_ANGULO = 'Maior média anual' THEN irradiacao_media_diaria END) AS MAIOR_MEDIA_ANUAL,
        MAX(CASE WHEN TIPO_ANGULO = 'Maior mínimo mensal' THEN irradiacao_media_diaria END) AS MAIOR_MINIMO_MENSAL,
        MAX(INCLINACAO) AS inclinacao,
        MAX(data_carga) AS data_carga,
        MAX(fonte) AS fonte
    FROM base_irradiacao
    GROUP BY uf, mes_num
) p
    ON g.uf = p.uf
   AND g.mes = p.mes_num;
   
   select * from vw_irradiacao_solar_long; 
