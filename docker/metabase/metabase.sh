#!/bin/bash

# Script de gerenciamento do Metabase
# Uso: ./metabase.sh [comando]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

case "$1" in
    start)
        echo "🚀 Iniciando Metabase..."
        docker-compose -f "$COMPOSE_FILE" up -d
        echo ""
        echo "✅ Metabase iniciado!"
        echo "📊 Acesse em: http://localhost:3000"
        echo ""
        echo "💡 Primeira vez? Configure o Metabase através da interface web."
        ;;
    stop)
        echo "⏹️  Parando Metabase..."
        docker-compose -f "$COMPOSE_FILE" stop
        echo "✅ Metabase parado!"
        ;;
    restart)
        echo "🔄 Reiniciando Metabase..."
        docker-compose -f "$COMPOSE_FILE" restart
        echo "✅ Metabase reiniciado!"
        ;;
    down)
        echo "🗑️  Removendo containers do Metabase..."
        docker-compose -f "$COMPOSE_FILE" down
        echo "✅ Containers removidos!"
        ;;
    logs)
        echo "📋 Exibindo logs do Metabase..."
        docker-compose -f "$COMPOSE_FILE" logs -f metabase
        ;;
    logs-db)
        echo "📋 Exibindo logs do banco de dados..."
        docker-compose -f "$COMPOSE_FILE" logs -f metabase-db
        ;;
    status)
        echo "📊 Status dos serviços:"
        docker-compose -f "$COMPOSE_FILE" ps
        ;;
    backup)
        echo "💾 Fazendo backup do banco de dados..."
        BACKUP_DIR="$SCRIPT_DIR/backups"
        mkdir -p "$BACKUP_DIR"
        BACKUP_FILE="$BACKUP_DIR/metabase_backup_$(date +%Y%m%d_%H%M%S).sql"
        
        docker-compose -f "$COMPOSE_FILE" exec -T metabase-db pg_dump -U metabase metabase > "$BACKUP_FILE"
        
        if [ $? -eq 0 ]; then
            echo "✅ Backup criado: $BACKUP_FILE"
        else
            echo "❌ Erro ao criar backup!"
            exit 1
        fi
        ;;
    restore)
        if [ -z "$2" ]; then
            echo "❌ Uso: $0 restore <arquivo_backup.sql>"
            exit 1
        fi
        
        if [ ! -f "$2" ]; then
            echo "❌ Arquivo não encontrado: $2"
            exit 1
        fi
        
        echo "🔄 Restaurando backup: $2"
        docker-compose -f "$COMPOSE_FILE" exec -T metabase-db psql -U metabase metabase < "$2"
        
        if [ $? -eq 0 ]; then
            echo "✅ Backup restaurado com sucesso!"
        else
            echo "❌ Erro ao restaurar backup!"
            exit 1
        fi
        ;;
    shell)
        echo "💻 Abrindo shell no container do Metabase..."
        docker-compose -f "$COMPOSE_FILE" exec metabase bash
        ;;
    shell-db)
        echo "💻 Abrindo shell do PostgreSQL..."
        docker-compose -f "$COMPOSE_FILE" exec metabase-db psql -U metabase metabase
        ;;
    update)
        echo "⬆️  Atualizando Metabase..."
        docker-compose -f "$COMPOSE_FILE" pull
        docker-compose -f "$COMPOSE_FILE" up -d
        echo "✅ Metabase atualizado!"
        ;;
    clean)
        echo "🧹 Limpando volumes e dados (ATENÇÃO: Isso vai apagar todos os dados!)..."
        read -p "Tem certeza? Digite 'sim' para confirmar: " confirm
        if [ "$confirm" = "sim" ]; then
            docker-compose -f "$COMPOSE_FILE" down -v
            echo "✅ Volumes removidos!"
        else
            echo "❌ Operação cancelada."
        fi
        ;;
    setup)
        echo "⚙️  Configurando ambiente..."
        
        # Criar arquivo .env se não existir
        if [ ! -f "$SCRIPT_DIR/.env" ]; then
            cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
            echo "✅ Arquivo .env criado. Configure as variáveis antes de iniciar."
        else
            echo "ℹ️  Arquivo .env já existe."
        fi
        
        # Criar diretório de backups
        mkdir -p "$SCRIPT_DIR/backups"
        echo "✅ Diretório de backups criado."
        
        echo ""
        echo "🎯 Próximos passos:"
        echo "1. Edite o arquivo .env com suas configurações"
        echo "2. Execute: $0 start"
        echo "3. Acesse: http://localhost:3000"
        ;;
    *)
        echo "❓ Uso: $0 {start|stop|restart|down|logs|logs-db|status|backup|restore|shell|shell-db|update|clean|setup}"
        echo ""
        echo "Comandos disponíveis:"
        echo "  start      - Inicia o Metabase"
        echo "  stop       - Para o Metabase"
        echo "  restart    - Reinicia o Metabase"
        echo "  down       - Remove os containers"
        echo "  logs       - Exibe logs do Metabase"
        echo "  logs-db    - Exibe logs do banco de dados"
        echo "  status     - Mostra status dos serviços"
        echo "  backup     - Faz backup do banco de dados"
        echo "  restore    - Restaura um backup (uso: restore <arquivo>)"
        echo "  shell      - Abre shell no container do Metabase"
        echo "  shell-db   - Abre shell do PostgreSQL"
        echo "  update     - Atualiza o Metabase para última versão"
        echo "  clean      - Remove volumes e dados (CUIDADO!)"
        echo "  setup      - Configura o ambiente inicial"
        echo ""
        echo "Exemplos:"
        echo "  $0 setup"
        echo "  $0 start"
        echo "  $0 logs"
        echo "  $0 backup"
        exit 1
        ;;
esac
