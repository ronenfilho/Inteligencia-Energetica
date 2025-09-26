-- models/stg_usina_geracao.sql

-- Bloco de dados para Janeiro de 2025
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
    val_geracao::NUMBER(38, 5) AS val_geracao_mw
FROM {{ source('staging','geracao_usina_2_2025_01') }}

UNION ALL

-- Bloco de dados para Fevereiro de 2025
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
    val_geracao::NUMBER(38, 5) AS val_geracao_mw
FROM {{ source('staging','geracao_usina_2_2025_02') }}

UNION ALL

-- Bloco de dados para Março de 2025
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
    val_geracao::NUMBER(38, 5) AS val_geracao_mw
FROM {{ source('staging','geracao_usina_2_2025_03') }}

UNION ALL

-- Bloco de dados para Abril de 2025
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
    val_geracao::NUMBER(38, 5) AS val_geracao_mw
FROM {{ source('staging','geracao_usina_2_2025_04') }}

UNION ALL

-- Bloco de dados para Maio de 2025
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
    val_geracao::NUMBER(38, 5) AS val_geracao_mw
FROM {{ source('staging','geracao_usina_2_2025_05') }}

UNION ALL

-- Bloco de dados para Junho de 2025
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
    val_geracao::NUMBER(38, 5) AS val_geracao_mw
FROM {{ source('staging','geracao_usina_2_2025_06') }}

UNION ALL

-- Bloco de dados para Julho de 2025
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
    val_geracao::NUMBER(38, 5) AS val_geracao_mw
FROM {{ source('staging','geracao_usina_2_2025_07') }}

UNION ALL

-- Bloco de dados para Agosto de 2025
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
    val_geracao::NUMBER(38, 5) AS val_geracao_mw
FROM {{ source('staging','geracao_usina_2_2025_08') }}

UNION ALL

-- Bloco de dados para Setembro de 2025
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
    val_geracao::NUMBER(38, 5) AS val_geracao_mw
FROM {{ source('staging','geracao_usina_2_2025_09') }}