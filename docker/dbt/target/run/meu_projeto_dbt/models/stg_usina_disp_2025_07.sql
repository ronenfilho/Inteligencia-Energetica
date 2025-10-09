
  create or replace   view LAB_PIPELINE.STAGING.stg_usina_disp_2025_07
  
   as (
    SELECT
  id_subsistema,
  nom_estado,
  nom_usina,
  din_instante::TIMESTAMP       AS instante,
  val_potenciainstalada         AS pot_instalada_mw,
  val_dispoperacional           AS disp_operacional_mw,
  val_dispsincronizada          AS disp_sincronizada_mw
FROM LAB_PIPELINE.STAGING.disponibilidade_usina_2025_07
  );

