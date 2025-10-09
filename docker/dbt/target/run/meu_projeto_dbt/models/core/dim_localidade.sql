
  create or replace   view LAB_PIPELINE.CORE.dim_localidade
  
   as (
    WITH localidades_unicas AS (
    SELECT
        DISTINCT
        nom_subsistema,
        nom_estado
    FROM LAB_PIPELINE.STAGING.stg_usina_disp
)
SELECT
    md5(cast(coalesce(cast(nom_subsistema as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nom_estado as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS id_dim_localidade,
    nom_subsistema,
    nom_estado
FROM localidades_unicas
  );

