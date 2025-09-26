from __future__ import annotations

import pendulum
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
    catchup=False,
    tags=['airbyte', 'sources', 'historical', 'energy', 'geracao', 'yearly-monthly'],
    description='Criar sources no Airbyte para GERACAO_USINA_2 - v2 com lógica para dados anuais (2000-2021) e mensais (2022+)',
    default_args={
        'owner': 'data_team',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=2),
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
    client_id = Variable.get("AIRBYTE_CLIENT_ID")
    client_secret = Variable.get("AIRBYTE_CLIENT_SECRET")
    
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

@task
def create_sources_and_connections():
    """Cria sources e connections no Airbyte para dados históricos e recentes"""
    context = get_current_context()
    params = context['params']
    
    start_date = params.get('start_date', '2025-01-01')
    end_date = params.get('end_date', 'auto')
    force_recreate = params.get('force_recreate', False)
    trigger_sync = params.get('trigger_sync', True)
    include_historical = params.get('include_historical', True)
    include_recent = params.get('include_recent', True)
    
    print(f"🚀 Iniciando criação de sources para GERACAO_USINA_2")
    print(f"📅 Período: {start_date} - {end_date}")
    print(f"📊 Incluir históricos (2000-2021): {include_historical}")
    print(f"📊 Incluir recentes (2022+): {include_recent}")
    
    # Configurações do Airbyte
    workspace_id = Variable.get("AIRBYTE_WORKSPACE_ID")
    destination_id = Variable.get("AIRBYTE_DESTINATION_ID")  # Snowflake destination
    
    # Gerar lista de arquivos
    files_to_process = generate_file_list(start_date, end_date, include_historical, include_recent)
    
    print(f"📁 Total de arquivos a processar: {len(files_to_process)}")
    
    # Contadores para relatório
    created_sources = 0
    created_connections = 0
    skipped_sources = 0
    errors = 0
    
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
            
            # Verificar se já existe connection com este stream
            if not force_recreate:
                print(f"🔍 Verificando conflitos para stream: {stream_name}")
                deleted_count = delete_conflicting_connections(stream_name, workspace_id)
                if deleted_count > 0:
                    print(f"🗑️ Removidas {deleted_count} connections conflitantes")
            
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
                created_sources += 1
                
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
                    connection_name = f"GERACAO_USINA_2_{period_label}_to_Snowflake"
                    
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
                        created_connections += 1
                        
                        # Iniciar sincronização se solicitado
                        if trigger_sync:
                            sync_response = make_api_request("POST", f"/connections/{connection_id}/sync", {})
                            if sync_response.status_code == 200:
                                job_id = sync_response.json()["jobId"]
                                print(f"🔄 Sincronização iniciada: {job_id}")
                            else:
                                print(f"⚠️ Erro ao iniciar sincronização: {sync_response.status_code}")
                    else:
                        print(f"❌ Erro ao criar connection: {connection_response.status_code} - {connection_response.text}")
                        errors += 1
                else:
                    print(f"❌ Erro ao descobrir schema: {schema_response.status_code} - {schema_response.text}")
                    errors += 1
            else:
                print(f"❌ Erro ao criar source: {source_response.status_code} - {source_response.text}")
                errors += 1
                
        except Exception as e:
            print(f"❌ Erro ao processar {filename}: {str(e)}")
            errors += 1
            continue
    
    # Relatório final
    print(f"\n" + "="*60)
    print(f"📊 RELATÓRIO FINAL - GERACAO_USINA_2 v2")
    print(f"="*60)
    print(f"✅ Sources criadas: {created_sources}")
    print(f"🔗 Connections criadas: {created_connections}")
    print(f"⏭️ Sources ignoradas: {skipped_sources}")
    print(f"❌ Erros: {errors}")
    print(f"📁 Total processado: {len(files_to_process)}")
    print(f"="*60)
    
    return {
        "created_sources": created_sources,
        "created_connections": created_connections,
        "skipped_sources": skipped_sources,
        "errors": errors,
        "total_files": len(files_to_process)
    }

# Definir as tasks
create_task = create_sources_and_connections()