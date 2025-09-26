from __future__ import annotations

"""
DAG para criação automática de sources e connections no Airbyte para dados GERACAO_USINA_2

VARIÁVEIS NECESSÁRIAS NO AIRFLOW:
- AIRBYTE_WORKSPACE_ID: ID do workspace do Airbyte
- AIRBYTE_CLIENT_ID: Client ID da aplicação Airbyte  
- AIRBYTE_CLIENT_SECRET: Client Secret da aplicação Airbyte
- AIRBYTE_DESTINATION_ID: ID do destino Snowflake no Airbyte

Para configurar as variáveis no Airflow:
1. Acesse Admin > Variables na UI do Airflow
2. Clique em "+" para criar uma nova variável
3. Adicione cada variável com seu respectivo valor

NOTA: Estas variáveis já devem estar configuradas se a v1 estiver funcionando.
"""

import requests
import json
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context

dag = DAG(
    dag_id='aaa_GERACAO_USINA_2_v2',
    schedule=None,  # Execução única, não recorrente
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['airbyte', 'sources', 'historical', 'energy', 'geracao', 'yearly-monthly'],
    description='Criar sources no Airbyte para GERACAO_USINA_2 - v2 com lógica para dados anuais (2000-2021) e mensais (2022+)',
    default_args={
        'owner': 'data_team',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'start_date': datetime(2025, 1, 1),
    },
    params={
        "start_date": "2025-01-01",  # Data de início da captura (formato YYYY-MM-DD)
        "end_date": "auto",          # "auto" para até mês atual (exclui futuros) ou YYYY-MM-DD para data específica
        "force_recreate": False,     # Se True, recria sources mesmo se já existirem
        "trigger_sync": True,        # Se True, inicia sincronização automática após criar connections
        "include_historical": True,  # Se True, inclui dados históricos (2000-2021)
        "include_recent": True       # Se True, inclui dados recentes (2022+)
    }
)

def get_access_token():
    """Gera um novo access token usando client credentials"""
    try:
        client_id = Variable.get("AIRBYTE_CLIENT_ID", default_var=None)
        client_secret = Variable.get("AIRBYTE_CLIENT_SECRET", default_var=None)
        
        if not client_id or not client_secret:
            raise Exception("❌ Variáveis AIRBYTE_CLIENT_ID e AIRBYTE_CLIENT_SECRET devem ser configuradas no Airflow")
            
    except Exception as e:
        raise Exception(f"Erro ao obter variáveis do Airflow: {str(e)}")
    
    response = requests.post(
        "https://api.airbyte.com/v1/applications/token",
        headers={"Content-Type": "application/json"},
        json={
            "client_id": client_id,
            "client_secret": client_secret
        }
    )
    
    if response.status_code != 200:
        raise Exception(f"Erro ao obter access token: {response.status_code} - {response.text}")
    
    return response.json()["access_token"]

def make_api_request(method, endpoint, data=None):
    """Faz uma requisição à API do Airbyte com token refresh automático"""
    access_token = get_access_token()
    base_url = "https://api.airbyte.com/v1"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    url = f"{base_url}{endpoint}"
    
    if method.upper() == "GET":
        response = requests.get(url, headers=headers)
    elif method.upper() == "POST":
        response = requests.post(url, headers=headers, json=data)
    elif method.upper() == "DELETE":
        response = requests.delete(url, headers=headers)
    else:
        raise ValueError(f"Método HTTP não suportado: {method}")
    
    return response

def check_connection_exists(connection_name, workspace_id):
    """Verifica se uma connection com o nome especificado já existe"""
    try:
        response = make_api_request("GET", f"/workspaces/{workspace_id}/connections")
        
        if response.status_code == 200:
            connections = response.json().get("data", [])
            for connection in connections:
                if connection.get("name") == connection_name:
                    return connection.get("connectionId")
        
        return None
        
    except Exception as e:
        print(f"❌ Erro ao verificar connection existente: {str(e)}")
        return None

def delete_conflicting_connections(stream_name, workspace_id):
    """Deleta connections que possuem streams conflitantes"""
    deleted_count = 0
    
    try:
        # Listar todas as connections do workspace
        response = make_api_request("GET", f"/workspaces/{workspace_id}/connections")
        
        if response.status_code != 200:
            print(f"❌ Erro ao listar connections: {response.status_code}")
            return deleted_count
        
        connections = response.json().get("data", [])
        print(f"🔍 Verificando {len(connections)} connections para conflitos com stream: {stream_name}")
        
        for connection in connections:
            connection_id = connection.get("connectionId")
            connection_name = connection.get("name", "Unknown")
            
            try:
                # Obter detalhes da connection incluindo streams
                detail_response = make_api_request("GET", f"/connections/{connection_id}")
                
                if detail_response.status_code == 200:
                    connection_detail = detail_response.json()
                    
                    # Verificar se esta connection tem o stream conflitante
                    streams = connection_detail.get("syncCatalog", {}).get("streams", [])
                    
                    has_conflicting_stream = False
                    for stream in streams:
                        stream_config = stream.get("config", {})
                        if stream_config.get("selected", False):
                            stream_stream = stream.get("stream", {})
                            current_stream_name = stream_stream.get("name", "")
                            
                            if current_stream_name == stream_name:
                                has_conflicting_stream = True
                                break
                    
                    if has_conflicting_stream:
                        print(f"🗑️ Deletando connection conflitante: {connection_name} ({connection_id})")
                        
                        # Deletar a connection
                        delete_response = make_api_request("DELETE", f"/connections/{connection_id}")
                        
                        if delete_response.status_code in [200, 204]:
                            print(f"✅ Connection deletada: {connection_name}")
                            deleted_count += 1
                        else:
                            print(f"❌ Erro ao deletar connection {connection_name}: {delete_response.status_code}")
                    
                else:
                    print(f"⚠️ Não foi possível obter detalhes da connection {connection_id}")
                    
            except Exception as e:
                print(f"❌ Erro ao processar connection {connection_id}: {str(e)}")
                continue

        if deleted_count > 0:
            print(f"🔄 Aguardando 30 segundos para estabilizar após {deleted_count} deleções...")
            import time
            time.sleep(30)
        
        return deleted_count
        
    except Exception as e:
        print(f"❌ Erro ao deletar connections conflitantes: {str(e)}")
        return deleted_count

def generate_file_list(start_date, end_date, include_historical=True, include_recent=True):
    """
    Gera lista de arquivos baseada na regra:
    - 2000-2021: arquivos anuais (CSV)
    - 2022+: arquivos mensais (XLSX)
    """
    files = []
    
    # Processar datas
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    
    if end_date == "auto":
        end_dt = datetime.now()
    else:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    start_year = start_dt.year
    start_month = start_dt.month
    end_year = end_dt.year
    end_month = end_dt.month
    
    # Dados históricos: 2000-2021 (arquivos anuais)
    if include_historical:
        historical_start_year = max(2000, start_year)
        historical_end_year = min(2021, end_year)
        
        for year in range(historical_start_year, historical_end_year + 1):
            # Se for o primeiro ano, verificar se a data de início permite incluir o ano todo
            if year == start_year and start_month > 1:
                continue  # Pula se começou no meio do ano (dados anuais não fazem sentido)
            
            # Se for o último ano, verificar se a data de fim permite incluir o ano todo
            if year == end_year and end_month < 12:
                continue  # Pula se termina no meio do ano (dados anuais não fazem sentido)
                
            files.append({
                'year': year,
                'month': None,
                'filename': f'GERACAO_USINA-2_{year}.csv',
                'url': f'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{year}.csv',
                'stream_name': f'geracao_usina_2_{year}',
                'type': 'yearly'
            })
    
    # Dados recentes: 2022+ (arquivos mensais)
    if include_recent:
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        recent_start_year = max(2022, start_year)
        recent_end_year = min(current_year, end_year)
        
        for year in range(recent_start_year, recent_end_year + 1):
            # Determinar mês inicial e final para este ano
            month_start = start_month if year == start_year else 1
            
            if year == end_year:
                month_end = end_month
            elif year == current_year:
                month_end = current_month  # Não processa meses futuros do ano atual
            else:
                month_end = 12
            
            for month in range(month_start, month_end + 1):
                files.append({
                    'year': year,
                    'month': month,
                    'filename': f'GERACAO_USINA-2_{year}_{month:02d}.xlsx',
                    'url': f'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{year}_{month:02d}.xlsx',
                    'stream_name': f'geracao_usina_2_{year}_{month:02d}',
                    'type': 'monthly'
                })
    
    return files

@task(dag=dag)
def check_configuration():
    """Verificar se todas as variáveis necessárias estão configuradas"""
    print("🔍 Verificando configuração das variáveis do Airflow...")
    
    required_vars = {
        "AIRBYTE_WORKSPACE_ID": "ID do workspace do Airbyte",
        "AIRBYTE_CLIENT_ID": "Client ID da aplicação Airbyte",
        "AIRBYTE_CLIENT_SECRET": "Client Secret da aplicação Airbyte", 
        "AIRBYTE_DESTINATION_ID": "ID do destino Snowflake no Airbyte"
    }
    
    missing_vars = []
    configured_vars = []
    
    for var_name, description in required_vars.items():
        try:
            value = Variable.get(var_name, default_var=None)
            if value:
                configured_vars.append(f"✅ {var_name}: {description}")
            else:
                missing_vars.append(f"❌ {var_name}: {description}")
        except Exception:
            missing_vars.append(f"❌ {var_name}: {description}")
    
    print("\n📋 Status das variáveis:")
    for var in configured_vars:
        print(f"  {var}")
    for var in missing_vars:
        print(f"  {var}")
    
    if missing_vars:
        print(f"\n⚠️ {len(missing_vars)} variável(eis) não configurada(s)")
        print("\n📝 Para configurar as variáveis:")
        print("1. Acesse Admin > Variables na UI do Airflow")
        print("2. Clique em '+' para criar uma nova variável")
        print("3. Adicione cada variável com seu respectivo valor")
        print("\n🔗 Exemplo de valores:")
        print("- AIRBYTE_WORKSPACE_ID: 71262590-7a33-4874-8be1-d80cc8125c1c")
        print("- AIRBYTE_CLIENT_ID: seu_client_id_aqui")
        print("- AIRBYTE_CLIENT_SECRET: seu_client_secret_aqui")
        print("- AIRBYTE_DESTINATION_ID: 778daa7c-feaf-4db6-96f3-70fd645acc77")
        
        raise Exception(f"❌ Configuração incompleta: {len(missing_vars)} variável(eis) ausente(s)")
    
    print(f"\n✅ Todas as {len(required_vars)} variáveis estão configuradas!")
    return True

@task(dag=dag)
def create_sources_task():
    """Cria sources no Airbyte para dados históricos e recentes"""
    try:
        context = get_current_context()
        params = context.get('params', {})
    except Exception as e:
        print(f"Erro ao obter contexto, usando parâmetros padrão: {e}")
        params = {}
    
    start_date = params.get('start_date', '2025-01-01')
    end_date = params.get('end_date', 'auto')
    force_recreate = params.get('force_recreate', False)
    include_historical = params.get('include_historical', True)
    include_recent = params.get('include_recent', True)
    
    print(f"🚀 Iniciando criação de sources para GERACAO_USINA_2")
    print(f"📅 Período: {start_date} - {end_date}")
    print(f"📊 Incluir históricos (2000-2021): {include_historical}")
    print(f"📊 Incluir recentes (2022+): {include_recent}")
    
    # Configurações do Airbyte
    try:
        workspace_id = Variable.get("AIRBYTE_WORKSPACE_ID", default_var=None)
        if not workspace_id:
            raise Exception("❌ Variável AIRBYTE_WORKSPACE_ID deve ser configurada no Airflow")
    except Exception as e:
        print(f"❌ Erro ao obter workspace_id: {str(e)}")
        print("📋 Variáveis necessárias no Airflow:")
        print("   - AIRBYTE_WORKSPACE_ID: ID do workspace do Airbyte")
        print("   - AIRBYTE_CLIENT_ID: Client ID da aplicação Airbyte")
        print("   - AIRBYTE_CLIENT_SECRET: Client Secret da aplicação Airbyte")
        print("   - AIRBYTE_DESTINATION_ID: ID do destino Snowflake no Airbyte")
        raise
    
    # Gerar lista de arquivos
    files_to_process = generate_file_list(start_date, end_date, include_historical, include_recent)
    
    print(f"📁 Total de arquivos a processar: {len(files_to_process)}")
    
    # Contadores para relatório
    created_sources = 0
    skipped_sources = 0
    errors = 0
    sources_created = []
    
    for file_info in files_to_process:
        try:
            year = file_info['year']
            month = file_info['month']
            filename = file_info['filename']
            url = file_info['url']
            stream_name = file_info['stream_name']
            file_type = file_info['type']
            
            type_label = "📅 ANUAL" if file_type == 'yearly' else "📆 MENSAL"
            period_label = str(year) if file_type == 'yearly' else f"{year}-{month:02d}"
            
            print(f"\n{type_label} Processando {period_label}: {filename}")
            
            # Nome da source no Airbyte
            source_name = f"GERACAO_USINA_2_{stream_name}"
            
            # Verificar se source já existe
            existing_source_id = check_source_exists(source_name, workspace_id)
            
            if existing_source_id and not force_recreate:
                print(f"⏭️ Source já existe: {existing_source_id}")
                sources_created.append({
                    "file_info": file_info,
                    "source_id": existing_source_id,
                    "source_name": source_name,
                    "status": "existing"
                })
                skipped_sources += 1
                continue
            elif existing_source_id and force_recreate:
                print(f"🗑️ Deletando source existente para recriar: {existing_source_id}")
                try:
                    delete_response = make_api_request("DELETE", f"/sources/{existing_source_id}")
                    if delete_response.status_code == 204:
                        print(f"✅ Source deletado com sucesso")
                    else:
                        print(f"⚠️ Não foi possível deletar source: {delete_response.status_code}")
                except Exception as e:
                    print(f"⚠️ Erro ao deletar source: {str(e)}")
            
            # Configuração da source baseada no tipo de arquivo
            if file_type == 'yearly':
                # Arquivo CSV anual
                source_config = {
                    "sourceType": "file",
                    "configuration": {
                        "dataset_name": stream_name,
                        "format": {
                            "filetype": "csv",
                            "delimiter": ",",
                            "quote_char": '"',
                            "escape_char": "\\",
                            "encoding": "utf8",
                            "double_quote": True,
                            "newlines_in_values": False,
                            "block_size": 10000,
                            "additional_reader_options": {},
                            "advanced_options": {},
                            "skip_rows_before_header": 0,
                            "skip_rows_after_header": 0
                        },
                        "provider": {
                            "storage": "HTTPS",
                            "public_url": url,
                            "user_agent": "Airbyte/1.0"
                        }
                    }
                }
            else:
                # Arquivo XLSX mensal
                source_config = {
                    "sourceType": "file",
                    "configuration": {
                        "dataset_name": stream_name,
                        "format": {
                            "filetype": "excel",
                            "skip_rows_before_header": 0,
                            "skip_rows_after_header": 0,
                            "header_definition": {
                                "header_definition_type": "From First Row"
                            }
                        },
                        "provider": {
                            "storage": "HTTPS",
                            "public_url": url,
                            "user_agent": "Airbyte/1.0"
                        }
                    }
                }
            
            # Criar source
            source_response = make_api_request("POST", "/sources", {
                "name": source_name,
                "workspaceId": workspace_id,
                **source_config
            })
            
            if source_response.status_code == 200:
                source_data = source_response.json()
                source_id = source_data["sourceId"]
                print(f"✅ Source criada: {source_name} ({source_id})")
                
                sources_created.append({
                    "file_info": file_info,
                    "source_id": source_id,
                    "source_name": source_name,
                    "status": "created"
                })
                created_sources += 1
            else:
                print(f"❌ Erro ao criar source: {source_response.status_code} - {source_response.text}")
                errors += 1
                
        except Exception as e:
            print(f"❌ Erro ao processar {filename}: {str(e)}")
            errors += 1
            continue
    
    # Relatório final
    print(f"\n" + "="*60)
    print(f"📊 RELATÓRIO SOURCES - GERACAO_USINA_2 v2")
    print(f"="*60)
    print(f"✅ Sources criadas: {created_sources}")
    print(f"⏭️ Sources já existiam: {skipped_sources}")
    print(f"❌ Erros: {errors}")
    print(f"📁 Total processado: {len(files_to_process)}")
    print(f"="*60)
    
    return sources_created

def check_source_exists(source_name, workspace_id):
    """Verifica se um source já existe no workspace"""
    try:
        response = make_api_request("GET", f"/workspaces/{workspace_id}/sources")
        
        if response.status_code == 200:
            sources = response.json().get("data", [])
            for source in sources:
                if source.get("name") == source_name:
                    return source.get("sourceId")
        return None
    except Exception as e:
        print(f"❌ Erro ao verificar sources existentes: {str(e)}")
        return None

@task(dag=dag)
def create_connections_task(sources_created):
    """Criar connections para os sources criados"""
    if not sources_created:
        print("❌ Nenhum source foi processado na etapa anterior")
        return []
    
    try:
        workspace_id = Variable.get("AIRBYTE_WORKSPACE_ID", default_var=None)
        destination_id = Variable.get("AIRBYTE_DESTINATION_ID", default_var=None)
        
        if not workspace_id:
            raise Exception("❌ Variável AIRBYTE_WORKSPACE_ID deve ser configurada no Airflow")
        if not destination_id:
            raise Exception("❌ Variável AIRBYTE_DESTINATION_ID deve ser configurada no Airflow")
            
    except Exception as e:
        print(f"❌ Erro ao obter variáveis: {str(e)}")
        print("📋 Variáveis necessárias no Airflow:")
        print("   - AIRBYTE_WORKSPACE_ID: ID do workspace do Airbyte")
        print("   - AIRBYTE_DESTINATION_ID: ID do destino Snowflake no Airbyte")
        raise
    
    connections_created = []
    
    print(f"🔗 Iniciando criação de connections...")
    print(f"📍 Destination ID: {destination_id}")
    
    for source_info in sources_created:
        source_id = source_info["source_id"]
        source_name = source_info["source_name"]
        file_info = source_info["file_info"]
        source_status = source_info.get("status", "unknown")
        
        year = file_info['year']
        month = file_info['month']
        stream_name = file_info['stream_name']
        file_type = file_info['type']
        
        period_label = str(year) if file_type == 'yearly' else f"{year}-{month:02d}"
        
        connection_name = f"GERACAO_USINA_2_{period_label}_to_Snowflake"
        
        print(f"🔄 Processando connection para {period_label} ({source_id}) - Status: {source_status}")
        
        # Verificar se connection já existe
        existing_connection_id = check_connection_exists(connection_name, workspace_id)
        
        if existing_connection_id:
            print(f"⏭️ Connection já existe: {existing_connection_id}")
            connections_created.append({
                "source_info": source_info,
                "connection_id": existing_connection_id,
                "connection_name": connection_name,
                "status": "existing"
            })
            continue
        
        # Verificar conflitos com streams
        print(f"🔍 Verificando conflitos para stream: {stream_name}")
        deleted_count = delete_conflicting_connections(stream_name, workspace_id)
        if deleted_count > 0:
            print(f"🗑️ Removidas {deleted_count} connections conflitantes")
        
        # Aguardar um pouco para a source ser processada
        import time
        time.sleep(2)
        
        # Obter schema da source
        schema_response = make_api_request("POST", f"/sources/{source_id}/discover_schema", {})
        
        if schema_response.status_code == 200:
            catalog = schema_response.json()["catalog"]
            print(f"📋 Schema descoberto para {source_name}")
            
            # Configurar streams para sincronização
            configured_streams = []
            for stream in catalog["streams"]:
                stream_config = {
                    "stream": stream["stream"],
                    "config": {
                        "syncMode": "full_refresh",
                        "destinationSyncMode": "overwrite",
                        "selected": True,
                        "fieldSelectionEnabled": False,
                        "selectedFields": []
                    }
                }
                configured_streams.append(stream_config)
            
            # Criar connection
            connection_response = make_api_request("POST", "/connections", {
                "name": connection_name,
                "sourceId": source_id,
                "destinationId": destination_id,
                "configurations": {
                    "streams": configured_streams
                },
                "syncCatalog": {
                    "streams": configured_streams
                },
                "status": "active",
                "scheduleType": "manual"
            })
            
            if connection_response.status_code == 200:
                connection_data = connection_response.json()
                connection_id = connection_data["connectionId"]
                print(f"✅ Connection criada: {connection_name} ({connection_id})")
                
                connections_created.append({
                    "source_info": source_info,
                    "connection_id": connection_id,
                    "connection_name": connection_name,
                    "status": "created"
                })
            else:
                print(f"❌ Erro ao criar connection: {connection_response.status_code} - {connection_response.text}")
        else:
            print(f"❌ Erro ao descobrir schema: {schema_response.status_code} - {schema_response.text}")
    
    print(f"\n📊 Resumo: {len(connections_created)} connections processadas")
    return connections_created

@task(dag=dag)
def trigger_initial_sync(connections_created):
    """Dispara sincronização inicial para todas as connections criadas"""
    try:
        context = get_current_context()
        params = context.get("params", {})
    except Exception as e:
        print(f"Erro ao obter contexto, usando parâmetros padrão: {e}")
        params = {}
    trigger_sync = params.get("trigger_sync", True)
    
    if not trigger_sync:
        print("⏭️ Sync automático desabilitado via parâmetro 'trigger_sync': False")
        return []
    
    if not connections_created:
        print("❌ Nenhuma connection foi criada")
        return []
    
    print(f"🚀 Disparando sincronização inicial para {len(connections_created)} connections...")
    
    sync_results = []
    
    for connection_info in connections_created:
        connection_id = connection_info["connection_id"]
        connection_name = connection_info["connection_name"]
        
        print(f"🔄 Iniciando sync para {connection_name} ({connection_id})...")
        
        try:
            # Usar endpoint correto para iniciar sync
            sync_data = {
                "connectionId": connection_id,
                "jobType": "sync"
            }
            
            response = make_api_request("POST", "/jobs", sync_data)
            
            if response.status_code in [200, 201]:
                job_info = response.json()
                job_id = job_info.get("jobId") or job_info.get("id")
                job_status = job_info.get("status", "unknown")
                print(f"✅ Sync job criado: {job_id} (status: {job_status})")
                sync_results.append({
                    "connection_id": connection_id,
                    "connection_name": connection_name,
                    "job_id": job_id,
                    "status": "started",
                    "job_status": job_status
                })
            else:
                print(f"❌ Erro ao iniciar sync: {response.status_code}")
                print(f"Resposta: {response.text}")
                sync_results.append({
                    "connection_id": connection_id,
                    "connection_name": connection_name,
                    "status": "failed"
                })
                
        except Exception as e:
            print(f"❌ Exceção ao iniciar sync: {str(e)}")
            sync_results.append({
                "connection_id": connection_id,
                "connection_name": connection_name,
                "status": "error",
                "error": str(e)
            })
    
    successful_syncs = len([s for s in sync_results if s.get('status') == 'started'])
    print(f"\n🎉 Resumo: {successful_syncs} sincronizações iniciadas com sucesso!")
    
    return sync_results

# Definir as tasks e dependências
config_check = check_configuration()
sources_created = create_sources_task()
connections_created = create_connections_task(sources_created)
sync_results = trigger_initial_sync(connections_created)

# Definir dependências explicitamente
config_check >> sources_created >> connections_created >> sync_results