-- Macro para fazer UNION de tabelas baseado em prefixo
-- Extrai automaticamente o mês/ano de referência do nome da tabela

{%- macro union_tables_by_prefix(schema_name, table_prefix, extract_date_from_name=true) -%}

    {%- set tables_query -%}
        SELECT table_name 
        FROM information_schema.tables 
        WHERE upper(table_schema) = upper('{{ schema_name }}')
        AND upper(table_name) LIKE upper('{{ table_prefix }}%')
        ORDER BY table_name
    {%- endset -%}

    {%- set results = run_query(tables_query) -%}
    {%- if execute -%}
        {%- set tables = results.columns[0].values() -%}
    {%- else -%}
        {%- set tables = [] -%}
    {%- endif -%}

    {%- if tables|length > 0 -%}
        {%- for table in tables -%}
            
            {%- if extract_date_from_name -%}
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
            {%- else -%}
                {%- set mes_referencia = 'NULL' -%}
            {%- endif -%}

            SELECT
                {%- if extract_date_from_name %}
                '{{ mes_referencia }}'::DATE AS mes_referencia,
                {%- endif %}
                *,
                '{{ table }}'::STRING AS source_table_name
            FROM {{ schema_name }}."{{ table }}"
            
            
            {%- if not loop.last %}
            UNION ALL
            {%- endif -%}
            
        {%- endfor -%}
    {%- else -%}
        -- Fallback se nenhuma tabela for encontrada
        SELECT 
            {%- if extract_date_from_name %}
            NULL::DATE AS mes_referencia,
            {%- endif %}
            'NO_TABLES_FOUND'::STRING AS source_table_name
        WHERE FALSE -- Não retorna nenhuma linha
    {%- endif -%}

{%- endmacro -%}