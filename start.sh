#!/bin/bash

# =============================================================================
# Script de Inicialização do Projeto Inteligência Energética
# =============================================================================
# Este script inicia todos os serviços Docker necessários para o projeto
# =============================================================================

set -e  # Sair em caso de erro

echo "=========================================="
echo "Inteligência Energética - Startup Script"
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

# Verificar se o Docker está instalado
if ! command -v docker &> /dev/null; then
    print_error "Docker não está instalado. Por favor, instale o Docker antes de continuar."
    exit 1
fi

# Verificar se o Docker Compose está instalado
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    print_error "Docker Compose não está instalado. Por favor, instale o Docker Compose antes de continuar."
    exit 1
fi

# Determinar comando docker-compose
if docker compose version &> /dev/null; then
    DOCKER_COMPOSE="docker compose"
else
    DOCKER_COMPOSE="docker-compose"
fi

print_info "Usando comando: $DOCKER_COMPOSE"
echo ""

# Função para iniciar um serviço
start_service() {
    local service_name=$1
    local service_path=$2

    if [ -d "$service_path" ] && [ -f "$service_path/docker-compose.yml" -o -f "$service_path/docker-compose.yaml" ]; then
        print_info "Iniciando $service_name..."
        cd "$service_path"
        $DOCKER_COMPOSE up -d
        cd - > /dev/null
        print_info "$service_name iniciado com sucesso!"
        echo ""
    else
        print_warning "Diretório ou docker-compose não encontrado para $service_name em $service_path"
        echo ""
    fi
}

# Iniciar os serviços
print_info "Iniciando serviços do projeto..."
echo ""

# Airflow
start_service "Airflow" "docker/airflow"

# DBT
start_service "DBT" "docker/dbt"

# Metabase
start_service "Metabase" "docker/metabase"

# Verificar status dos containers
echo ""
print_info "Verificando status dos containers..."
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
echo ""

# Informações de acesso
echo "=========================================="
print_info "Serviços disponíveis:"
echo "=========================================="
echo "• Airflow: http://localhost:8081"
echo "  - Usuário: airflow / Senha: airflow (padrão)"
echo ""
echo "• Metabase: http://localhost:3000"
echo "  - Configure na primeira execução"
echo ""
echo "• DBT: Execute comandos via container"
echo "  - Consulte docker/dbt/README.md"
echo "=========================================="
echo ""

print_info "Todos os serviços foram iniciados com sucesso!"
print_warning "Aguarde alguns minutos para que todos os serviços estejam completamente prontos."
echo ""