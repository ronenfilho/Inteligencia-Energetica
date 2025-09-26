-- models/stg_usina_geracao.sql
-- Union de todas as tabelas de geração de usina

{%- set geracao_tables_query -%}
    SELECT table_name 
    FROM information_schema.tables 
    WHERE upper(table_schema) = 'STAGING' 
    AND upper(table_name) LIKE 'GERACAO_USINA%'
    ORDER BY table_name
{%- endset -%}

{%- set results = run_query(geracao_tables_query) -%}
{%- if execute -%}
    {%- set geracao_tables = results.columns[0].values() -%}
{%- else -%}
    {%- set geracao_tables = [] -%}
{%- endif -%}

{% if geracao_tables|length > 0 %}
    {% for table in geracao_tables %}
        {%- set table_name_clean = table.replace('-', '_') -%}
        {%- set table_parts = table_name_clean.split('_') -%}
        {%- if table_parts|length >= 4 -%}
            {%- set year = table_parts[-2] -%}
            {%- set month = table_parts[-1] -%}
            {%- if month|length == 1 -%}
                {%- set month = '0' + month -%}
            {%- endif -%}
            {%- set mes_referencia = year + '-' + month + '-01' -%}
        {%- else -%}
            {%- set mes_referencia = '1900-01-01' -%}
        {%- endif -%}

        SELECT
            '{{ mes_referencia }}'::DATE AS mes_referencia,
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
            '{{ table }}'::STRING AS source_table_name
        FROM STAGING."{{ table }}"
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
{% else %}
    -- Fallback se nenhuma tabela for encontrada
    SELECT 
        NULL::DATE AS mes_referencia,
        NULL::STRING AS id_subsistema,
        NULL::STRING AS nom_subsistema,
        NULL::STRING AS id_estado,
        NULL::STRING AS nom_estado,
        NULL::STRING AS id_ons,
        NULL::STRING AS ceg,
        NULL::STRING AS nom_usina,
        NULL::STRING AS nom_tipousina,
        NULL::STRING AS nom_tipocombustivel,
        NULL::STRING AS cod_modalidadeoperacao,
        NULL::TIMESTAMP AS instante,
        NULL::NUMBER(38, 5) AS val_geracao_mw,
        'NO_TABLES_FOUND'::STRING AS source_table_name
    WHERE FALSE
{% endif %}