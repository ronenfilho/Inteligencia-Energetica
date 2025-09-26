-- models/stg_usina_geracao_go.sql
-- Dados de geração das usinas do estado de Goiás (GO)

SELECT 
    mes_referencia,
    id_subsistema,
    nom_subsistema,
    id_estado,
    nom_estado,
    id_ons,
    ceg,
    nom_usina,
    nom_tipousina,
    nom_tipocombustivel,
    cod_modalidadeoperacao,
    instante,
    val_geracao_mw
FROM IE_DB.STAGING.stg_usina_geracao
WHERE id_estado = 'GO'
  AND nom_tipocombustivel = 'Fotovoltaica'