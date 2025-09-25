# Configuração de Sincronização - Airflow Docker

## Problema Resolvido

O Airflow estava executando via Docker e lendo DAGs de um diretório diferente do projeto principal, causando:
- ❌ DAGs criadas no projeto não apareciam no Airflow UI
- ❌ Necessidade de copiar arquivos manualmente
- ❌ Falta de sincronização automática

## Solução Implementada

### 1. Configuração do Volume Docker

Modificamos o `docker-compose.yaml` para apontar diretamente para o diretório do projeto:

**Antes:**
```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
```

**Depois:**
```yaml
volumes:
  - /home/decode/workspace/Inteligencia-Energetica/airflow/dags:/opt/airflow/dags
```

### 2. Vantagens da Solução

✅ **Sincronização Automática**: Qualquer alteração no projeto é refletida imediatamente no Docker
✅ **Centralizado**: Todas as DAGs ficam no projeto principal
✅ **Versionamento**: DAGs são versionadas junto com o código do projeto
✅ **Colaboração**: Time trabalha no mesmo diretório, sem duplicação
✅ **Profissional**: Solução definitiva e escalável

### 3. Estrutura Final

```
Inteligencia-Energetica/
├── airflow/
│   ├── dags/
│   │   ├── airbyte_create_sources_for_2024_v4.py
│   │   ├── airbyte_create_sources_for_2024_v5.py
│   │   └── airbyte_create_sources_for_2024_v6.py ✅ AGORA SINCRONIZADO
│   └── test_conection.txt
└── ...
```

### 4. Como Usar

1. **Criar/Editar DAGs**: Trabalhe normalmente no diretório `Inteligencia-Energetica/airflow/dags/`
2. **Visualização**: DAGs aparecem automaticamente no Airflow UI (http://localhost:8081)
3. **Deploy**: Mudanças são refletidas sem necessidade de restart (apenas aguardar o dag-processor)

### 5. Backup e Rollback

**Arquivo de Backup:**
```bash
/home/decode/workspace/airflow/docker-compose.yaml.backup
```

**Para Reverter (se necessário):**
```bash
cd /home/decode/workspace/airflow
cp docker-compose.yaml.backup docker-compose.yaml
docker-compose down && docker-compose up -d
```

### 6. Monitoramento

**Verificar logs do dag-processor:**
```bash
docker logs airflow-airflow-dag-processor-1 --tail 20
```

**Verificar DAGs no container:**
```bash
docker exec airflow-airflow-dag-processor-1 ls -la /opt/airflow/dags/
```

## Status da Implementação

✅ **DAG v6 detectada e carregada com sucesso**
✅ **Sincronização automática funcionando**
✅ **Solução definitiva implementada**

A DAG `airbyte_create_sources_for_2024_v6` agora está disponível no Airflow UI e pronta para execução!