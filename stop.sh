#!/bin/bash

# =============================================================================
# Script de Parada do Projeto Inteligência Energética
# =============================================================================
# Este script para todos os serviços Docker do projeto
# =============================================================================

set -e  # Sair em caso de erro

echo "=========================================="
echo "Inteligência Energética - Shutdown Script"
echo "=========================================="
echo ""

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Função para imprimir mensagens coloridas
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Determinar comando docker-compose
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

print_info "Usando comando: $DOCKER_COMPOSE"
echo ""

# Função para parar um serviço
stop_service() {
    local service_name=$1
    local service_path=$2
    local remove_volumes=$3

    if [ -d "$service_path" ] && [ -f "$service_path/docker-compose.yml" -o -f "$service_path/docker-compose.yaml" ]; then
        print_info "Parando $service_name..."
        cd "$service_path"

        if [ "$remove_volumes" = true ]; then
            $DOCKER_COMPOSE down -v
            print_info "$service_name parado e volumes removidos!"
        else
            $DOCKER_COMPOSE down
            print_info "$service_name parado com sucesso!"
        fi

        cd - > /dev/null
        echo ""
    else
        print_warning "Diretório ou docker-compose não encontrado para $service_name em $service_path"
        echo ""
    fi
}

# Verificar se deve remover volumes
REMOVE_VOLUMES=false
if [ "$1" = "--volumes" ] || [ "$1" = "-v" ]; then
    print_warning "Modo de remoção de volumes ativado. Todos os dados serão perdidos!"
    read -p "Tem certeza que deseja continuar? (s/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Ss]$ ]]; then
        REMOVE_VOLUMES=true
        print_info "Volumes serão removidos."
    else
        print_info "Volumes serão mantidos."
    fi
    echo ""
fi

# Parar os serviços
print_info "Parando serviços do projeto..."
echo ""

# Metabase
stop_service "Metabase" "docker/metabase" $REMOVE_VOLUMES

# DBT
stop_service "DBT" "docker/dbt" $REMOVE_VOLUMES

# Airflow
stop_service "Airflow" "docker/airflow" $REMOVE_VOLUMES

# Verificar containers restantes
echo ""
print_info "Verificando containers em execução..."
RUNNING_CONTAINERS=$(docker ps -q | wc -l)

if [ "$RUNNING_CONTAINERS" -eq 0 ]; then
    print_info "Nenhum container em execução."
else
    print_warning "Ainda existem $RUNNING_CONTAINERS container(s) em execução:"
    docker ps --format "table {{.Names}}\t{{.Status}}"
fi

echo ""
print_info "Processo de parada concluído!"

if [ "$REMOVE_VOLUMES" = false ]; then
    echo ""
    print_info "Dica: Use './stop.sh --volumes' ou './stop.sh -v' para remover também os volumes e limpar todos os dados."
fi

echo ""