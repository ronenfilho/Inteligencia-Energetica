-- models/stg_usina_geracao.sql
-- Union de todas as tabelas de geração de usina
    SELECT
            '2022-01-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-01'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-01"
        
        UNION ALL
        
    SELECT
            '2022-02-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-02'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-02"
        
        UNION ALL
        
    SELECT
            '2022-03-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-03'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-03"
        
        UNION ALL
        
    SELECT
            '2022-04-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-04'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-04"
        
        UNION ALL
        
    SELECT
            '2022-05-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-05'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-05"
        
        UNION ALL
        
    SELECT
            '2022-06-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-06'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-06"
        
        UNION ALL
        
    SELECT
            '2022-07-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-07'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-07"
        
        UNION ALL
        
    SELECT
            '2022-08-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-08'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-08"
        
        UNION ALL
        
    SELECT
            '2022-09-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-09'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-09"
        
        UNION ALL
        
    SELECT
            '2022-10-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-10'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-10"
        
        UNION ALL
        
    SELECT
            '2022-11-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-11'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-11"
        
        UNION ALL
        
    SELECT
            '2022-12-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2022-12'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2022-12"
        
        UNION ALL
        
    SELECT
            '2023-01-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-01'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-01"
        
        UNION ALL
        
    SELECT
            '2023-02-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-02'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-02"
        
        UNION ALL
        
    SELECT
            '2023-03-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-03'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-03"
        
        UNION ALL
        
    SELECT
            '2023-04-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-04'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-04"
        
        UNION ALL
        
    SELECT
            '2023-05-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-05'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-05"
        
        UNION ALL
        
    SELECT
            '2023-06-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-06'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-06"
        
        UNION ALL
        
    SELECT
            '2023-07-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-07'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-07"
        
        UNION ALL
        
    SELECT
            '2023-08-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-08'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-08"
        
        UNION ALL
        
    SELECT
            '2023-09-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-09'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-09"
        
        UNION ALL
        
    SELECT
            '2023-10-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-10'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-10"
        
        UNION ALL
        
    SELECT
            '2023-11-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-11'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-11"
        
        UNION ALL
        
    SELECT
            '2023-12-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2023-12'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2023-12"
        
        UNION ALL
        
    SELECT
            '2024-01-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-01'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-01"
        
        UNION ALL
        
    SELECT
            '2024-02-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-02'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-02"
        
        UNION ALL
        
    SELECT
            '2024-03-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-03'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-03"
        
        UNION ALL
        
    SELECT
            '2024-04-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-04'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-04"
        
        UNION ALL
        
    SELECT
            '2024-05-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-05'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-05"
        
        UNION ALL
        
    SELECT
            '2024-06-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-06'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-06"
        
        UNION ALL
        
    SELECT
            '2024-07-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-07'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-07"
        
        UNION ALL
        
    SELECT
            '2024-08-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-08'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-08"
        
        UNION ALL
        
    SELECT
            '2024-09-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-09'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-09"
        
        UNION ALL
        
    SELECT
            '2024-10-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-10'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-10"
        
        UNION ALL
        
    SELECT
            '2024-11-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-11'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-11"
        
        UNION ALL
        
    SELECT
            '2024-12-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2024-12'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2024-12"
        
        UNION ALL
        
    SELECT
            '2025-01-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2025-01'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2025-01"
        
        UNION ALL
        
    SELECT
            '2025-02-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2025-02'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2025-02"
        
        UNION ALL
        
    SELECT
            '2025-03-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2025-03'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2025-03"
        
        UNION ALL
        
    SELECT
            '2025-04-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2025-04'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2025-04"
        
        UNION ALL
        
    SELECT
            '2025-05-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2025-05'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2025-05"
        
        UNION ALL
        
    SELECT
            '2025-06-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2025-06'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2025-06"
        
        UNION ALL
        
    SELECT
            '2025-07-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2025-07'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2025-07"
        
        UNION ALL
        
    SELECT
            '2025-08-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2025-08'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2025-08"
        
        UNION ALL
        
    SELECT
            '2025-09-01'::DATE AS mes_referencia,
            id_subsistema::STRING AS id_subsistema,
            nom_subsistema::STRING AS nom_subsistema,
            id_estado::STRING AS id_estado,
            nom_estado::STRING AS nom_estado,
            id_ons::STRING AS id_ons,
            ceg::STRING AS ceg,
            nom_usina::STRING AS nom_usina,
            nom_tipousina::STRING AS nom_tipousina,
            nom_tipocombustivel::STRING AS nom_tipocombustivel,
            cod_modalidadeoperacao::STRING AS cod_modalidadeoperacao,
            din_instante::TIMESTAMP AS instante,
            val_geracao::NUMBER(38, 5) AS val_geracao_mw,
            'GERACAO_USINA_2_2025-09'::STRING AS source_table_name
        FROM STAGING."GERACAO_USINA_2_2025-09"
        
    
