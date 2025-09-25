-- CTE principal para buscar os dados de staging.
WITH stg_dados AS (
    SELECT * FROM IE_DB.STAGING.stg_usina_disp
),

-- CTE para buscar os dados da dimensão de usina.
dim_usina AS (
    SELECT * FROM IE_DB.CORE.dim_usina
),

-- CTE para buscar os dados da dimensão de localidade.
dim_localidade AS (
    SELECT * FROM IE_DB.CORE.dim_localidade
),

-- CTE para buscar os dados da dimensão de tempo.
dim_tempo AS (
    SELECT * FROM IE_DB.CORE.dim_tempo
)

-- Seleção final para construir a tabela de fatos.
-- O objetivo aqui é substituir as chaves de negócio (ex: 'ceg', 'nom_estado')
-- pelas chaves primárias substitutas das tabelas de dimensão (ex: 'id_dim_usina').
SELECT
    -- Chave primária da fato (opcional, mas boa prática)
    md5(cast(coalesce(cast(stg.ceg as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(stg.instante as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS id_fact_disponibilidade,

    -- Chaves estrangeiras (Foreign Keys) das nossas dimensões
    dus.id_dim_usina,
    dlo.id_dim_localidade,
    dti.id_dim_tempo,

    -- Coluna de data original para referência e joins mais fáceis
    stg.instante,

    -- Métricas (os fatos numéricos que queremos analisar)
    stg.pot_instalada_mw,
    stg.disp_operacional_mw,
    stg.disp_sincronizada_mw

FROM stg_dados AS stg

-- JOIN com a dimensão de usina usando a chave de negócio 'ceg'
LEFT JOIN dim_usina AS dus
    ON stg.ceg = dus.ceg

-- JOIN com a dimensão de localidade usando a combinação de 'nom_subsistema' e 'nom_estado'
LEFT JOIN dim_localidade AS dlo
    ON stg.nom_subsistema = dlo.nom_subsistema
    AND stg.nom_estado = dlo.nom_estado

-- JOIN com a dimensão de tempo usando a chave de negócio 'instante'
LEFT JOIN dim_tempo AS dti
    ON stg.instante = dti.instante