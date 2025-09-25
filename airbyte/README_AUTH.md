# Configuração de Autenticação Automática - Airbyte API

## Problema Resolvido

Os tokens de acesso do Airbyte expiram em apenas **3 minutos**, o que tornava necessário gerar um novo token manualmente a cada execução. Agora o sistema gera automaticamente novos tokens usando as credenciais da aplicação (`client_id` e `client_secret`).

## Como Funciona

1. **Antes**: Precisava gerar token manualmente e passar como parâmetro
2. **Agora**: Usa credenciais da aplicação para gerar tokens automaticamente

## Configuração Inicial

### 1. Criar Aplicação no Airbyte Cloud

1. Acesse [Airbyte Cloud](https://cloud.airbyte.com)
2. Vá em **Settings** → **Account** → **Applications**
3. Clique em **"Create an application"**
4. Dê um nome para sua aplicação (ex: "Inteligencia-Energetica-API")
5. Copie o `client_id` e `client_secret` gerados

### 2. Configurar Variáveis no Airflow

No Airflow, configure as seguintes variáveis em **Admin** → **Variables**:

```
AIRBYTE_CLIENT_ID = seu_client_id_aqui
AIRBYTE_CLIENT_SECRET = seu_client_secret_aqui
AIRBYTE_WORKSPACE_ID = 71262590-7a33-4874-8be1-d80cc8125c1c
AIRBYTE_DESTINATION_ID = a0a784b5-de8a-42dc-a02f-328ba96e644d
```

## Como Usar

### Script de Cleanup

**Antes (v1):**
```bash
./cleanup_airbyte.sh eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**Antes (v2):**
```bash
./cleanup_airbyte.sh seu_client_id seu_client_secret
```

**Agora (v3 - com .env):**
```bash
# 1. Configure o arquivo .env uma única vez
cp .env.example .env
# Edite o .env com suas credenciais

# 2. Execute o script sem parâmetros
./cleanup_airbyte.sh
```

### DAG do Airflow

**Antes:** DAG v5 com token fixo que expirava
**Agora:** DAG v6 com renovação automática de token

## Vantagens da Nova Abordagem

1. **✅ Sem Expiração Manual**: Tokens são renovados automaticamente
2. **✅ Mais Seguro**: Credenciais ficam nas variáveis do Airflow
3. **✅ Menos Manutenção**: Não precisa gerar token a cada execução
4. **✅ Operação Contínua**: Scripts podem rodar por longos períodos

## Arquivos Atualizados

- `cleanup_airbyte.sh` - Script de limpeza com auto-renovação de token
- `airbyte_create_sources_for_2024_v6.py` - DAG com gerenciamento automático de token

## Exemplo de Uso

```bash
# 1. Configurar o arquivo .env (apenas uma vez)
cd /home/decode/workspace/Inteligencia-Energetica/airbyte
cp .env.example .env

# Editar o .env com suas credenciais:
# AIRBYTE_CLIENT_ID=e81f2e01-b900-4da3-9822-73a5fa2f17db
# AIRBYTE_CLIENT_SECRET=4NLhMUBF7vygiZQ8C6cplijbXtBzQEGx
# AIRBYTE_WORKSPACE_ID=71262590-7a33-4874-8be1-d80cc8125c1c
# AIRBYTE_DESTINATION_ID=a0a784b5-de8a-42dc-a02f-328ba96e644d

# 2. Executar o script de cleanup (simples!)
./cleanup_airbyte.sh

# 3. Executar o DAG v6 no Airflow (com variáveis configuradas)
```

## Segurança

- **Nunca** commite credenciais no código
- Use variáveis de ambiente ou sistema de secrets do Airflow
- As credenciais da aplicação são mais seguras que tokens temporários
- Revogue aplicações que não estão sendo usadas

## Troubleshooting

### Erro de Autenticação
```bash
❌ Erro ao gerar access token
```
**Solução**: Verifique se o `client_id` e `client_secret` estão corretos

### Token Inválido
```bash
"error": "Unauthorized"
```
**Solução**: As credenciais são renovadas automaticamente a cada chamada

### Workspace Não Encontrado
```bash
"error": "Workspace not found"
```
**Solução**: Verifique se o `WORKSPACE_ID` está correto nas variáveis