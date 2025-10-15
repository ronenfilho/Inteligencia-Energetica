#!/bin/bash

# Script de atalhos para comandos dbt via Docker
# Uso: ./dbt_docker.sh [comando]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

dbt_docker() {
    docker-compose -f "$COMPOSE_FILE" run --rm dbt "$@"
}

case "$1" in
    run)
        echo "🚀 Executando dbt run..."
        dbt_docker run "${@:2}"
        ;;
    test)
        echo "🧪 Executando dbt test..."
        dbt_docker test "${@:2}"
        ;;
    build)
        echo "🔨 Executando dbt build..."
        dbt_docker build "${@:2}"
        ;;
    debug)
        echo "🔍 Executando dbt debug..."
        dbt_docker debug "${@:2}"
        ;;
    deps)
        echo "📦 Instalando dependências dbt..."
        dbt_docker deps "${@:2}"
        ;;
    compile)
        echo "⚙️ Compilando projeto dbt..."
        dbt_docker compile "${@:2}"
        ;;
    seed)
        echo "🌱 Carregando seeds..."
        dbt_docker seed "${@:2}"
        ;;
    snapshot)
        echo "📸 Executando snapshots..."
        dbt_docker snapshot "${@:2}"
        ;;
    docs)
        if [ "$2" = "generate" ]; then
            echo "📚 Gerando documentação..."
            dbt_docker docs generate "${@:3}"
        elif [ "$2" = "serve" ]; then
            echo "🌐 Servindo documentação..."
            docker-compose -f "$COMPOSE_FILE" run --rm -p 8080:8080 dbt docs serve --port 8080 "${@:3}"
        else
            echo "📚 Gerando e servindo documentação..."
            dbt_docker docs generate
            docker-compose -f "$COMPOSE_FILE" run --rm -p 8080:8080 dbt docs serve --port 8080
        fi
        ;;
    clean)
        echo "🧹 Limpando arquivos temporários..."
        dbt_docker clean "${@:2}"
        ;;
    ls)
        echo "📋 Listando recursos..."
        dbt_docker ls "${@:2}"
        ;;
    shell)
        echo "💻 Abrindo shell no container..."
        docker-compose -f "$COMPOSE_FILE" run --rm dbt bash
        ;;
    *)
        echo "❓ Uso: $0 {run|test|build|debug|deps|compile|seed|snapshot|docs|clean|ls|shell} [args]"
        echo ""
        echo "Comandos principais:"
        echo "  run      - Executa os modelos dbt"
        echo "  test     - Executa os testes dbt"
        echo "  build    - Executa build (run + test)"
        echo "  debug    - Verifica a configuração do projeto"
        echo ""
        echo "Comandos adicionais:"
        echo "  deps     - Instala dependências do packages.yml"
        echo "  compile  - Compila o projeto sem executar"
        echo "  seed     - Carrega arquivos CSV como tabelas"
        echo "  snapshot - Executa snapshots"
        echo "  docs     - Gera e serve documentação (use 'docs generate' ou 'docs serve')"
        echo "  clean    - Remove arquivos temporários"
        echo "  ls       - Lista recursos do projeto"
        echo "  shell    - Abre shell interativo no container"
        echo ""
        echo "Exemplos:"
        echo "  $0 run"
        echo "  $0 run --select model_name"
        echo "  $0 test --select tag:staging"
        echo "  $0 build --full-refresh"
        exit 1
        ;;
esac
