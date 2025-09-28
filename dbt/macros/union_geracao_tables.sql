-- Macro para unir tabelas de geração seguindo padrão ONS
-- Tabelas anuais: GERACAO_USINA_2_2000 (sem aspas)
-- Tabelas mensais: GERACAO_USINA_2_2022-01 (com aspas)

{% macro union_geracao_tables(schema='STAGING', table_pattern='GERACAO_USINA_2_%') %}
    
    {%- set geracao_tables_query -%}
        SELECT table_name 
        FROM information_schema.tables 
        WHERE upper(table_schema) = '{{ schema.upper() }}' 
        AND upper(table_name) LIKE '{{ table_pattern.upper() }}'
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
            {%- set table_parts = table.split('_') -%}
            
            {# Detectar tipo de tabela e definir referência correta #}
            {%- if '-' in table -%}
                {# Tabela mensal: GERACAO_USINA_2_2022-01 #}
                {%- set year_month = table_parts[-1] -%}
                {%- set mes_referencia = year_month + '-01' -%}
                {%- set table_reference = '"' + table + '"' -%}
                {%- set data_type = 'monthly' -%}
            {%- else -%}
                {# Tabela anual: GERACAO_USINA_2_2000 #}
                {%- set year = table_parts[-1] -%}
                {%- set mes_referencia = year + '-01-01' -%}
                {%- set table_reference = table -%}
                {%- set data_type = 'annual' -%}
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
                '{{ table }}'::STRING AS source_table_name,
                '{{ data_type }}'::STRING AS data_type
            FROM {{ schema }}.{{ table_reference }}
            
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
            'NO_TABLES_FOUND'::STRING AS source_table_name,
            'fallback'::STRING AS data_type
        WHERE FALSE
    {% endif %}

{% endmacro %}