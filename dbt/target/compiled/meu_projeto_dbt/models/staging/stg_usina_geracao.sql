-- models/stg_usina_geracao.sql
-- Union de todas as tabelas de geração de usina
-- Compatível com nomenclatura ONS: anuais (2000-2021) e mensais (2022+)
-- Usa macro union_geracao_tables para centralizar a lógica


        SELECT
                '2000-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2000'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2000
            
            
            UNION ALL
            
        SELECT
                '2001-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2001'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2001
            
            
            UNION ALL
            
        SELECT
                '2002-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2002'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2002
            
            
            UNION ALL
            
        SELECT
                '2003-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2003'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2003
            
            
            UNION ALL
            
        SELECT
                '2004-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2004'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2004
            
            
            UNION ALL
            
        SELECT
                '2005-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2005'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2005
            
            
            UNION ALL
            
        SELECT
                '2006-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2006'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2006
            
            
            UNION ALL
            
        SELECT
                '2007-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2007'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2007
            
            
            UNION ALL
            
        SELECT
                '2008-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2008'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2008
            
            
            UNION ALL
            
        SELECT
                '2009-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2009'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2009
            
            
            UNION ALL
            
        SELECT
                '2010-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2010'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2010
            
            
            UNION ALL
            
        SELECT
                '2011-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2011'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2011
            
            
            UNION ALL
            
        SELECT
                '2012-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2012'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2012
            
            
            UNION ALL
            
        SELECT
                '2013-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2013'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2013
            
            
            UNION ALL
            
        SELECT
                '2014-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2014'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2014
            
            
            UNION ALL
            
        SELECT
                '2015-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2015'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2015
            
            
            UNION ALL
            
        SELECT
                '2016-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2016'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2016
            
            
            UNION ALL
            
        SELECT
                '2017-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2017'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2017
            
            
            UNION ALL
            
        SELECT
                '2018-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2018'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2018
            
            
            UNION ALL
            
        SELECT
                '2019-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2019'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2019
            
            
            UNION ALL
            
        SELECT
                '2020-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2020'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2020
            
            
            UNION ALL
            
        SELECT
                '2021-01-01'::DATE AS mes_referencia,
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
                'GERACAO_USINA_2_2021'::STRING AS source_table_name,
                'annual'::STRING AS data_type
            FROM STAGING.GERACAO_USINA_2_2021
            
            
            UNION ALL
            
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
                'GERACAO_USINA_2_2022-01'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-02'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-03'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-04'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-05'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-06'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-07'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-08'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-09'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-10'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-11'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2022-12'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-01'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-02'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-03'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-04'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-05'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-06'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-07'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-08'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-09'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-10'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-11'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2023-12'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-01'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-02'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-03'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-04'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-05'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-06'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-07'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-08'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-09'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-10'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-11'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2024-12'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2025-01'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2025-02'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2025-03'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2025-04'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2025-05'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2025-06'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2025-07'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2025-08'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
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
                'GERACAO_USINA_2_2025-09'::STRING AS source_table_name,
                'monthly'::STRING AS data_type
            FROM STAGING."GERACAO_USINA_2_2025-09"
            
            
        
    

