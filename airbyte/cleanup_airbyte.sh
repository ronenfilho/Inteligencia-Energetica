#!/bin/bash

# Script para limpar recursos do Airbyte
# Uso: ./cleanup_airbyte.sh <client_id> <client_secret>

if [ $# -ne 2 ]; then
    echo "❌ Erro: Client ID e Client Secret são obrigatórios"
    echo "Uso: $0 <client_id> <client_secret>"
    echo ""
    echo "Para obter suas credenciais:"
    echo "1. Acesse Airbyte Cloud -> Settings -> Account -> Applications"
    echo "2. Crie uma nova aplicação ou use uma existente"
    echo "3. Copie o client_id e client_secret"
    exit 1
fi

CLIENT_ID="$1"
CLIENT_SECRET="$2"
BASE_URL="https://api.airbyte.com/v1"
WORKSPACE_ID="71262590-7a33-4874-8be1-d80cc8125c1c"

# Função para gerar um novo access token
get_access_token() {
    echo "🔑 Gerando novo access token..."
    
    local response=$(curl -s -X POST https://api.airbyte.com/v1/applications/token \
        -H "Content-Type: application/json" \
        -d "{
            \"client_id\": \"$CLIENT_ID\",
            \"client_secret\": \"$CLIENT_SECRET\"
        }")
    
    local token=$(echo "$response" | jq -r '.access_token // empty')
    
    if [ -z "$token" ] || [ "$token" = "null" ]; then
        echo "❌ Erro ao gerar access token"
        echo "Resposta da API: $response"
        exit 1
    fi
    
    echo "✅ Access token gerado com sucesso"
    echo "$token"
}

echo "🧹 Iniciando limpeza do Airbyte..."
echo "Workspace ID: $WORKSPACE_ID"
echo "Token: ${TOKEN:0:20}..."

# Função para fazer requisições
# Função para fazer chamadas à API com tratamento de erro e token refresh
api_call() {
    local method="$1"
    local endpoint="$2"
    local data="$3"
    
    # Gera um novo token para cada chamada (tokens expiram em 3 minutos)
    local current_token=$(get_access_token)
    
    if [ -n "$data" ]; then
        curl -s -X "$method" "$BASE_URL$endpoint" \
            -H "Authorization: Bearer $current_token" \
            -H "Content-Type: application/json" \
            -d "$data"
    else
        curl -s -X "$method" "$BASE_URL$endpoint" \
            -H "Authorization: Bearer $current_token" \
            -H "Content-Type: application/json"
    fi
}

echo ""
echo "📋 Listando conexões existentes (com paginação)..."

# Função para listar todas as conexões com paginação
list_all_connections() {
    local limit=100
    local offset=0
    local has_more=true
    local total_found=0
    
    echo "📋 Conexões encontradas:"
    
    while [ "$has_more" = true ]; do
        local response=$(api_call GET "https://api.airbyte.com/v1/connections?workspaceIds=$WORKSPACE_ID&limit=$limit&offset=$offset")
        
        if echo "$response" | grep -q "Unauthorized"; then
            echo "❌ Token inválido ou expirado"
            exit 1
        fi
        
        local page_count=$(echo "$response" | jq -r '.data | length // 0' 2>/dev/null)
        
        # Garante que page_count seja um número
        if [ -z "$page_count" ] || ! [[ "$page_count" =~ ^[0-9]+$ ]]; then
            page_count=0
        fi
        
        if [ "$page_count" -gt 0 ]; then
            echo "$response" | jq -r '.data[] | "  🔗 \(.connectionId) - \(.name)"' 2>/dev/null
            total_found=$((total_found + page_count))
        fi
        
        if [ "$page_count" -lt "$limit" ]; then
            has_more=false
        else
            offset=$((offset + limit))
        fi
    done
    
    echo "Total de conexões encontradas: $total_found"
    echo ""
}

# Função para obter IDs de conexões para deletar
get_connections_to_delete() {
    local all_connections=""
    local limit=100
    local offset=0
    local has_more=true
    
    while [ "$has_more" = true ]; do
        local response=$(api_call GET "https://api.airbyte.com/v1/connections?workspaceIds=$WORKSPACE_ID&limit=$limit&offset=$offset")
        
        if echo "$response" | grep -q "Unauthorized"; then
            echo "❌ Token inválido ou expirado"
            exit 1
        fi
        
        local page_connections=$(echo "$response" | jq -r '.data[]?.connectionId // empty' 2>/dev/null)
        local page_count=$(echo "$response" | jq -r '.data | length // 0' 2>/dev/null)
        
        # Garante que page_count seja um número
        if [ -z "$page_count" ] || ! [[ "$page_count" =~ ^[0-9]+$ ]]; then
            page_count=0
        fi
        
        if [ -n "$page_connections" ] && [ "$page_connections" != "" ]; then
            if [ -z "$all_connections" ]; then
                all_connections="$page_connections"
            else
                all_connections="$all_connections"$'\n'"$page_connections"
            fi
        fi
        
        if [ "$page_count" -lt "$limit" ]; then
            has_more=false
        else
            offset=$((offset + limit))
        fi
    done
    
    echo "$all_connections"
}

# Listar conexões encontradas
list_all_connections

# Perguntar se quer deletar todas as conexões
read -p "Deseja deletar TODAS as conexões listadas acima? (y/N): " delete_connections
if [[ $delete_connections =~ ^[Yy]$ ]]; then
    CONNECTION_IDS=$(get_connections_to_delete)
    
    if [ -z "$CONNECTION_IDS" ]; then
        echo "✅ Nenhuma conexão encontrada para deletar"
    else
        echo "🗑️  Deletando conexões..."
        for id in $CONNECTION_IDS; do
            # Skip empty lines or non-UUID strings
            if [[ $id =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
                echo "  Deletando conexão: $id"
                HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "https://api.airbyte.com/v1/connections/$id" \
                    -H "Authorization: Bearer $TOKEN" \
                    -H "Content-Type: application/json")
                
                if [ "$HTTP_STATUS" = "204" ]; then
                    echo "    ✅ Conexão $id deletada (HTTP $HTTP_STATUS)"
                else
                    echo "    ❌ Erro ao deletar conexão $id (HTTP $HTTP_STATUS)"
                fi
            fi
        done
    fi
else
    echo "⏭️  Pulando deleção de conexões"
fi

echo ""
echo "📋 Listando fontes existentes (com paginação)..."

# Função para listar todos os sources com paginação
list_all_sources() {
    local limit=100
    local offset=0
    local has_more=true
    local total_found=0
    local disponibilidade_count=0
    
    echo "📋 Sources encontrados:"
    
    while [ "$has_more" = true ]; do
        local response=$(api_call GET "https://api.airbyte.com/v1/sources?workspaceIds=$WORKSPACE_ID&limit=$limit&offset=$offset")
        
        if [ -z "$response" ] || echo "$response" | grep -q "Unauthorized"; then
            echo "❌ Erro na requisição ou token inválido"
            break
        fi
        
        local page_count=$(echo "$response" | jq -r '.data | length // 0 // 0 // 0' 2>/dev/null)
        
        # Garante que page_count seja um número
        if [ -z "$page_count" ] || ! [[ "$page_count" =~ ^[0-9]+$ ]]; then
            page_count=0
        fi
        
        # Garante que page_count seja um número
        if [ -z "$page_count" ] || ! [[ "$page_count" =~ ^[0-9]+$ ]]; then
            page_count=0
        fi
        
        # Garante que page_count seja um número
        if [ -z "$page_count" ] || ! [[ "$page_count" =~ ^[0-9]+$ ]]; then
            page_count=0
        fi
        
        if [ "$page_count" -gt 0 ]; then
            # Mostrar todos os sources com indicação especial para DISPONIBILIDADE_USINA
            echo "$response" | jq -r '.data[] | if (.name | startswith("DISPONIBILIDADE_USINA")) then "  🎯 \(.sourceId) - \(.name)" else "  📁 \(.sourceId) - \(.name)" end' 2>/dev/null
            
            local page_disponibilidade=$(echo "$response" | jq -r '.data | map(select(.name | startswith("DISPONIBILIDADE_USINA"))) | length' 2>/dev/null)
            disponibilidade_count=$((disponibilidade_count + page_disponibilidade))
            total_found=$((total_found + page_count))
        fi
        
        if [ "$page_count" -lt "$limit" ]; then
            has_more=false
        else
            offset=$((offset + limit))
        fi
    done
    
    echo ""
    echo "Total de sources encontrados: $total_found"
    echo "Sources DISPONIBILIDADE_USINA (🎯): $disponibilidade_count"
    echo "Outros sources (📁): $((total_found - disponibilidade_count))"
    echo ""
}

# Função para obter IDs de sources para deletar
get_sources_to_delete() {
    local filter_type="$1"  # "all" ou "disponibilidade"
    local all_sources=""
    local limit=100
    local offset=0
    local has_more=true
    
    while [ "$has_more" = true ]; do
        local response=$(api_call GET "https://api.airbyte.com/v1/sources?workspaceIds=$WORKSPACE_ID&limit=$limit&offset=$offset")
        
        if [ -z "$response" ] || echo "$response" | grep -q "Unauthorized"; then
            echo "❌ Erro na requisição ou token inválido"
            break
        fi
        
        local page_sources=""
        if [ "$filter_type" = "all" ]; then
            page_sources=$(echo "$response" | jq -r '.data[]?.sourceId // empty' 2>/dev/null)
        else
            page_sources=$(echo "$response" | jq -r '.data[]? | select(.name | startswith("DISPONIBILIDADE_USINA")) | .sourceId' 2>/dev/null)
        fi
        
        local page_count=$(echo "$response" | jq -r '.data | length // 0' 2>/dev/null)
        
        # Garante que page_count seja um número
        if [ -z "$page_count" ] || ! [[ "$page_count" =~ ^[0-9]+$ ]]; then
            page_count=0
        fi
        
        if [ -n "$page_sources" ] && [ "$page_sources" != "" ]; then
            if [ -z "$all_sources" ]; then
                all_sources="$page_sources"
            else
                all_sources="$all_sources"$'\n'"$page_sources"
            fi
        fi
        
        if [ "$page_count" -lt "$limit" ]; then
            has_more=false
        else
            offset=$((offset + limit))
        fi
    done
    
    echo "$all_sources"
}

# Listar sources encontrados
list_all_sources

# Perguntar qual tipo de sources deletar
echo "Opções de deleção de sources:"
echo "1) Deletar apenas sources DISPONIBILIDADE_USINA (🎯)"
echo "2) Deletar TODOS os sources (🎯 + 📁)"
echo "3) Não deletar nenhum source"
read -p "Escolha uma opção (1/2/3): " source_option

SOURCE_IDS=""
if [ "$source_option" = "1" ]; then
    echo "🎯 Coletando sources DISPONIBILIDADE_USINA..."
    SOURCE_IDS=$(get_sources_to_delete "disponibilidade")
    DELETE_TYPE="DISPONIBILIDADE_USINA"
elif [ "$source_option" = "2" ]; then
    read -p "⚠️  ATENÇÃO: Isso deletará TODOS os sources! Confirma? (y/N): " confirm_all
    if [[ $confirm_all =~ ^[Yy]$ ]]; then
        echo "🗑️  Coletando TODOS os sources..."
        SOURCE_IDS=$(get_sources_to_delete "all")
        DELETE_TYPE="TODOS"
    else
        echo "⏭️  Cancelado pelo usuário"
        source_option="3"
    fi
elif [ "$source_option" = "3" ]; then
    echo "⏭️  Pulando deleção de sources"
else
    echo "❌ Opção inválida. Pulando deleção de sources"
    source_option="3"
fi

if [ "$source_option" != "3" ] && [ -n "$SOURCE_IDS" ]; then
    echo "🗑️  Deletando sources $DELETE_TYPE..."
    for id in $SOURCE_IDS; do
        # Skip empty lines or non-UUID strings
        if [[ $id =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
            echo "  Deletando source: $id"
            HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "https://api.airbyte.com/v1/sources/$id" \
                -H "Authorization: Bearer $TOKEN" \
                -H "Content-Type: application/json")
            
            if [ "$HTTP_STATUS" = "204" ]; then
                echo "    ✅ Source $id deletado (HTTP $HTTP_STATUS)"
            else
                echo "    ❌ Erro ao deletar source $id (HTTP $HTTP_STATUS)"
            fi
        fi
    done
elif [ "$source_option" != "3" ]; then
    echo "✅ Nenhum source encontrado para deletar"
fi

echo ""
echo "🎉 Limpeza concluída!"
echo ""
echo "📋 Resumo final (verificando todas as páginas):"

# Contar conexões restantes
total_connections=0
offset=0
limit=100
while true; do
    response=$(api_call GET "https://api.airbyte.com/v1/connections?workspaceIds=$WORKSPACE_ID&limit=$limit&offset=$offset")
    page_count=$(echo "$response" | jq -r '.data | length // 0' 2>/dev/null)
    
    # Garante que page_count seja um número
    if [ -z "$page_count" ] || ! [[ "$page_count" =~ ^[0-9]+$ ]]; then
        page_count=0
    fi
    
    if [ "$page_count" = "0" ]; then
        break
    fi
    
    total_connections=$((total_connections + page_count))
    
    if [ "$page_count" -lt "$limit" ]; then
        break
    fi
    offset=$((offset + limit))
done

# Contar sources restantes
total_sources=0
total_disponibilidade=0
offset=0
while true; do
    response=$(api_call GET "https://api.airbyte.com/v1/sources?workspaceIds=$WORKSPACE_ID&limit=$limit&offset=$offset")
    page_count=$(echo "$response" | jq -r '.data | length // 0' 2>/dev/null)
    
    # Garante que page_count seja um número
    if [ -z "$page_count" ] || ! [[ "$page_count" =~ ^[0-9]+$ ]]; then
        page_count=0
    fi
    
    if [ "$page_count" = "0" ]; then
        break
    fi
    
    page_sources_count=$(echo "$response" | jq -r '.data | map(select(.name | startswith("DISPONIBILIDADE_USINA"))) | length' 2>/dev/null)
    total_sources=$((total_sources + page_count))
    total_disponibilidade=$((total_disponibilidade + page_sources_count))
    
    if [ "$page_count" -lt "$limit" ]; then
        break
    fi
    offset=$((offset + limit))
done

echo "Conexões restantes: $total_connections"
echo "Sources restantes: $total_sources"
echo "Sources DISPONIBILIDADE_USINA restantes: $total_disponibilidade"