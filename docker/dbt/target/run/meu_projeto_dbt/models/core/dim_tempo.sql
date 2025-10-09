
  create or replace   view LAB_PIPELINE.CORE.dim_tempo
  
   as (
    WITH instantes_unicos AS (
    SELECT
        DISTINCT instante
    FROM LAB_PIPELINE.STAGING.stg_usina_disp
)
SELECT
    md5(cast(coalesce(cast(instante::STRING as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS id_dim_tempo,
    instante,
    EXTRACT(YEAR FROM instante)    AS ano,
    EXTRACT(MONTH FROM instante)   AS mes,
    EXTRACT(DAY FROM instante)     AS dia,
    EXTRACT(HOUR FROM instante)    AS hora,
    EXTRACT(DAYOFWEEK FROM instante) AS dia_da_semana -- Ex: 0=Domingo, 1=Segunda, etc. (varia com o DB)
FROM instantes_unicos
  );

