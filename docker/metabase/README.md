# Metabase Docker Setup

Este diretório contém a configuração Docker para executar o Metabase com PostgreSQL como banco de dados de aplicação.

## 📋 O que é Metabase?

Metabase é uma ferramenta open-source de Business Intelligence que permite criar dashboards, visualizações e análises de dados de forma simples e intuitiva.

## 🚀 Instalação Rápida

### 1. Configurar o ambiente

```bash
cd /home/decode/workspace/Inteligencia-Energetica/docker/metabase

# Configurar ambiente inicial
./metabase.sh setup

# Editar arquivo .env com suas configurações (opcional)
nano .env
```

### 2. Iniciar o Metabase

```bash
./metabase.sh start
```

### 3. Acessar o Metabase

Abra seu navegador e acesse: **http://localhost:3000**

Na primeira vez, você precisará:
1. Criar uma conta de administrador
2. Configurar a conexão com seu banco de dados (Snowflake, PostgreSQL, etc.)
3. Começar a criar queries e dashboards

## 📦 Serviços incluídos

- **metabase** - Aplicação Metabase (porta 3000)
- **metabase-db** - Banco de dados PostgreSQL (interno)

## 🎯 Comandos disponíveis

### Gerenciamento básico

```bash
# Iniciar serviços
./metabase.sh start

# Parar serviços
./metabase.sh stop

# Reiniciar serviços
./metabase.sh restart

# Verificar status
./metabase.sh status

# Ver logs
./metabase.sh logs

# Ver logs do banco de dados
./metabase.sh logs-db
```

### Backup e restore

```bash
# Fazer backup
./metabase.sh backup

# Restaurar backup
./metabase.sh restore backups/metabase_backup_20241015_143000.sql
```

### Manutenção

```bash
# Atualizar para última versão
./metabase.sh update

# Abrir shell no container
./metabase.sh shell

# Abrir shell do PostgreSQL
./metabase.sh shell-db

# Remover todos os dados (CUIDADO!)
./metabase.sh clean
```

## ⚙️ Configuração

### Arquivo .env

O arquivo `.env` contém as configurações do Metabase:

```bash
# Banco de dados do Metabase
MB_DB_TYPE=postgres
MB_DB_DBNAME=metabase
MB_DB_USER=metabase
MB_DB_PASS=metabase_password

# Timezone
MB_JAVA_TIMEZONE=America/Sao_Paulo

# Porta de acesso
METABASE_PORT=3000
```

### Configurar conexão com Snowflake

1. Acesse o Metabase (http://localhost:3000)
2. Vá em **Settings > Admin > Databases**
3. Clique em **Add database**
4. Selecione **Snowflake**
5. Configure:
   - **Display name**: Nome da conexão
   - **Account**: Seu account do Snowflake (ex: FQIMVRH-BR39779)
   - **User**: Usuário (ex: SVC_PIPELINE_USER_IE)
   - **Password**: Senha
   - **Warehouse**: Warehouse (ex: IE_TRANSFORM_WH)
   - **Database**: Database (ex: IE_DB)
   - **Schema**: Schema padrão (ex: CORE)

### Configurar e-mail (opcional)

Para enviar alertas e relatórios por e-mail, descomente e configure no `.env`:

```bash
MB_EMAIL_SMTP_HOST=smtp.gmail.com
MB_EMAIL_SMTP_PORT=587
MB_EMAIL_SMTP_USERNAME=seu-email@gmail.com
MB_EMAIL_SMTP_PASSWORD=sua-senha-app
MB_EMAIL_FROM_ADDRESS=seu-email@gmail.com
MB_EMAIL_SMTP_SECURITY=tls
```

## 📂 Estrutura

```
docker/metabase/
├── docker-compose.yml    # Configuração do Docker Compose
├── metabase.sh          # Script de gerenciamento
├── .env.example         # Exemplo de variáveis de ambiente
├── .env                 # Variáveis de ambiente (criado no setup)
├── backups/             # Diretório para backups
└── README.md            # Esta documentação
```

## 💾 Volumes

Os dados são persistidos em volumes Docker:

- `metabase-data` - Dados da aplicação Metabase
- `metabase-db-data` - Dados do PostgreSQL

## 🔒 Segurança

### Produção

Para ambientes de produção:

1. **Altere as senhas padrão**:
   ```bash
   MB_DB_PASS=senha_forte_aqui
   POSTGRES_PASSWORD=senha_forte_aqui
   ```

2. **Use HTTPS**: Configure um reverse proxy (nginx/traefik) com SSL

3. **Restrinja acesso**: Configure firewall ou rede privada

4. **Faça backups regulares**:
   ```bash
   # Adicionar ao crontab
   0 2 * * * /path/to/metabase.sh backup
   ```

## 🐛 Troubleshooting

### Metabase não inicia

1. Verifique se as portas estão disponíveis:
   ```bash
   netstat -tulpn | grep 3000
   ```

2. Verifique os logs:
   ```bash
   ./metabase.sh logs
   ```

### Erro de conexão com banco de dados

1. Verifique se o PostgreSQL está rodando:
   ```bash
   ./metabase.sh status
   ```

2. Verifique os logs do banco:
   ```bash
   ./metabase.sh logs-db
   ```

### Resetar Metabase

Se precisar começar do zero:

```bash
./metabase.sh down
./metabase.sh clean
./metabase.sh start
```

## 📚 Recursos úteis

- [Documentação oficial do Metabase](https://www.metabase.com/docs/latest/)
- [Conectar ao Snowflake](https://www.metabase.com/docs/latest/databases/connections/snowflake)
- [Guia de instalação](https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker)

## 🎨 Próximos passos

Após o Metabase estar rodando:

1. **Configure fontes de dados**
   - Snowflake
   - PostgreSQL
   - Outros bancos

2. **Crie suas primeiras queries**
   - Use o editor SQL
   - Ou use o query builder visual

3. **Monte dashboards**
   - Organize visualizações
   - Configure filtros
   - Compartilhe com a equipe

4. **Configure alertas**
   - Envie relatórios por e-mail
   - Crie alertas baseados em métricas

## 📊 Integração com o projeto

Este Metabase pode se conectar diretamente aos dados processados pelo dbt no Snowflake:

1. Configure a conexão com Snowflake (veja acima)
2. Aponte para o database `IE_DB` e schema `CORE`
3. Você verá todas as tabelas/views criadas pelo dbt
4. Comece a criar análises e dashboards!

## 🔄 Workflow completo

```
Airbyte → Snowflake (RAW) → dbt (CORE) → Metabase (Visualização)
```

1. **Airbyte**: Extrai dados das fontes
2. **Snowflake RAW**: Armazena dados brutos
3. **dbt**: Transforma dados (schema CORE)
4. **Metabase**: Visualiza e analisa dados
