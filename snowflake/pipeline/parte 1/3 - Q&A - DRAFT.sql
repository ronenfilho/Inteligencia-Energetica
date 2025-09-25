USE DATABASE IE_DB;

-- Qual a disponibilidade operacional total por estado em um determinado mês?
SELECT
    dlo.nom_estado,
    SUM(fct.disp_operacional_mw) AS total_disp_operacional_mw
FROM CORE.FACT_DISPONIBILIDADE AS fct
JOIN CORE.DIM_LOCALIDADE AS dlo ON fct.id_dim_localidade = dlo.id_dim_localidade
JOIN CORE.DIM_TEMPO AS dti ON fct.id_dim_tempo = dti.id_dim_tempo
WHERE
    dti.ano = 2025 AND dti.mes = 8 -- Filtrando por Agosto de 2025
GROUP BY
    dlo.nom_estado
ORDER BY
    total_disp_operacional_mw DESC;

-- Qual usina teve a maior média de potência instalada no último trimestre?
SELECT
    dus.nom_usina,
    AVG(fct.pot_instalada_mw) AS media_pot_instalada_mw
FROM CORE.FACT_DISPONIBILIDADE AS fct
JOIN CORE.DIM_USINA AS dus ON fct.id_dim_usina = dus.id_dim_usina
WHERE
    fct.instante >= '2025-07-01' AND fct.instante < '2025-10-01' -- Filtrando pelo 3º trimestre
GROUP BY
    dus.nom_usina
ORDER BY
    media_pot_instalada_mw DESC
LIMIT 1;

-- Qual a média de disponibilidade sincronizada por hora do dia para usinas do tipo "Hidráulica"?
SELECT
    dti.hora,
    AVG(fct.disp_sincronizada_mw) AS media_disp_sincronizada_mw
FROM CORE.FACT_DISPONIBILIDADE AS fct
JOIN CORE.DIM_USINA AS dus ON fct.id_dim_usina = dus.id_dim_usina
JOIN CORE.DIM_TEMPO AS dti ON fct.id_dim_tempo = dti.id_dim_tempo
WHERE
    dus.nom_tipocombustivel = 'Hidráulica' -- Supondo que o tipo de combustível identifique a usina hidráulica
GROUP BY
    dti.hora
ORDER BY
    dti.hora;
