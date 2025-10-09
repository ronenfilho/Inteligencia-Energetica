
  create or replace   view LAB_PIPELINE.CORE_CORE.dim_usina
  
   as (
    

WITH usinas_unicas AS (
    SELECT
        DISTINCT
        ceg,
        nom_usina,
        nom_tipocombustivel
    FROM LAB_PIPELINE.CORE_STAGING.stg_usina_disp
)

SELECT
    md5(cast(coalesce(cast(ceg as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS id_dim_usina,
    ceg,
    nom_usina,
    nom_tipocombustivel
FROM usinas_unicas
  );

