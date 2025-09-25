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
    dag_id='aaa_DISPONIBILIDADE_USINA_v8',
    schedule=None,  # Execução única, não recorrente
    catchup=False,
    tags=['airbyte', 'sources', '2025', 'energy', 'one-time'],
    description='Criar sources e connections no Airbyte com período configurável - v7 execução única após 5 minutos',
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
        "end_date": "auto",          # "auto" para data atual ou YYYY-MM-DD para data específica
        "force_recreate": False,     # Se True, recria sources mesmo se já existirem
        "trigger_sync": True         # Se True, inicia sincronização automática após criar connections
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
        
        print(f"🎯 Total de connections conflitantes deletadas: {deleted_count}")
        return deleted_count
        
    except Exception as e:
        print(f"❌ Erro ao deletar connections conflitantes: {str(e)}")
        return deleted_count

def generate_month_list(start_date_str, end_date_str):
    """Gera lista de meses entre duas datas"""
    from datetime import datetime
    
    # Parse das datas
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    
    if end_date_str == "auto":
        end_date = datetime.now()
    else:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    months = []
    current = start_date.replace(day=1)  # Começar no primeiro dia do mês
    
    while current <= end_date:
        month_code = current.strftime("%Y-%m")
        month_name = current.strftime("%B %Y")
        
        # Tradução dos meses para português
        month_translations = {
            "January": "Janeiro", "February": "Fevereiro", "March": "Março",
            "April": "Abril", "May": "Maio", "June": "Junho",
            "July": "Julho", "August": "Agosto", "September": "Setembro",
            "October": "Outubro", "November": "Novembro", "December": "Dezembro"
        }
        
        for en, pt in month_translations.items():
            month_name = month_name.replace(en, pt)
        
        months.append((month_code, month_name))
        
        # Avançar para o próximo mês
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months

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

def check_connection_exists(connection_name, workspace_id):
    """Verifica se uma connection já existe no workspace"""
    try:
        response = make_api_request("GET", f"/workspaces/{workspace_id}/connections")
        
        if response.status_code == 200:
            connections = response.json().get("data", [])
            for connection in connections:
                if connection.get("name") == connection_name:
                    return connection.get("connectionId")
        return None
    except Exception as e:
        print(f"❌ Erro ao verificar connections existentes: {str(e)}")
        return None

def delete_conflicting_connections(stream_name, workspace_id):
    """Deleta connections que conflitam com o stream especificado"""
    try:
        response = make_api_request("GET", f"/workspaces/{workspace_id}/connections")
        
        if response.status_code == 200:
            connections = response.json().get("data", [])
            deleted_count = 0
            
            for connection in connections:
                connection_id = connection.get("connectionId")
                # Verificar se esta connection usa o stream conflitante
                # (Simplificado: deletar connections com nome similar)
                if stream_name in connection.get("name", ""):
                    print(f"🗑️ Deletando connection conflitante: {connection.get('name')} ({connection_id})")
                    try:
                        delete_response = make_api_request("DELETE", f"/connections/{connection_id}")
                        if delete_response.status_code == 204:
                            print(f"✅ Connection deletada com sucesso")
                            deleted_count += 1
                        else:
                            print(f"⚠️ Não foi possível deletar connection: {delete_response.status_code}")
                    except Exception as delete_error:
                        print(f"⚠️ Erro ao deletar connection: {str(delete_error)}")
            
            return deleted_count
        return 0
    except Exception as e:
        print(f"❌ Erro ao deletar connections conflitantes: {str(e)}")
        return 0

@task
def create_sources_task():
    """Criar sources para o período configurado"""
    context = get_current_context()
    params = context['params']
    
    # Obter parâmetros da execução
    start_date = params.get('start_date', '2025-01-01')
    end_date = params.get('end_date', 'auto')
    force_recreate = params.get('force_recreate', False)
    
    print(f"🗓️ Período de captura: {start_date} até {end_date}")
    print(f"🔄 Recriar sources existentes: {force_recreate}")
    
    workspace_id = Variable.get("airbyte_workspace_id")
    
    # Usar definitionId da variável ou valor padrão
    try:
        definition_id = Variable.get("airbyte_source_definition_id")
        print(f"📋 Usando definitionId da variável: {definition_id}")
    except:
        definition_id = "778daa7c-feaf-4db6-96f3-70fd645acc77"
        print(f"📋 Usando definitionId padrão: {definition_id}")
    
    # Gerar lista de meses
    months = generate_month_list(start_date, end_date)
    sources_created = []
    
    print(f"🏗️ Iniciando criação de {len(months)} sources...")
    print(f"📍 Workspace ID: {workspace_id}")
    print(f"🔧 Definition ID: {definition_id}")
    
    for month_code, month_name in months:
        source_name = f"DISPONIBILIDADE_USINA_{month_code}"
        print(f"🔄 Processando source para {month_name} ({month_code})...")
        
        # Verificar se o source já existe
        existing_source_id = check_source_exists(source_name, workspace_id)
        
        if existing_source_id and not force_recreate:
            print(f"⏭️ Source já existe para {month_name}: {existing_source_id}")
            sources_created.append({
                "month": month_code,
                "name": month_name,
                "source_id": existing_source_id,
                "status": "existing"
            })
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
        
        # URL do arquivo CSV no S3 da ONS
        year_month = month_code.replace("-", "_")  # Converter 2025-01 para 2025_01
        s3_url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/disponibilidade_usina_ho/DISPONIBILIDADE_USINA_{year_month}.csv"
        print(f"🔗 URL S3: {s3_url}")
        
        source_data = {
            "name": source_name,
            "workspaceId": workspace_id,
            "definitionId": definition_id,
            "configuration": {
                "dataset_name": f"DISPONIBILIDADE_USINA_{month_code}",
                "format": "csv",
                "url": s3_url,
                "provider": {
                    "storage": "HTTPS",
                    "user_agent": False
                },
                "reader_options": "{ \"delimiter\": \";\" }"
            }
        }
        
        try:
            response = make_api_request("POST", "/sources", source_data)
            
            if response.status_code == 200:
                source_info = response.json()
                source_id = source_info.get("sourceId")
                print(f"✅ Source criado com sucesso para {month_name}: {source_id}")
                sources_created.append({
                    "month": month_code,
                    "name": month_name,
                    "source_id": source_id,
                    "status": "created"
                })
            else:
                print(f"❌ Erro ao criar source para {month_name}: {response.status_code}")
                print(f"Resposta: {response.text}")
                
        except Exception as e:
            print(f"❌ Exceção ao criar source para {month_name}: {str(e)}")
    
    print(f"\n📊 Resumo: {len(sources_created)} sources processados")
    
    # Estatísticas
    created_count = len([s for s in sources_created if s.get('status') == 'created'])
    existing_count = len([s for s in sources_created if s.get('status') == 'existing'])
    
    print(f"🆕 Criados: {created_count}")
    print(f"📋 Já existiam: {existing_count}")
    
    return sources_created

@task
def create_connections_task(sources_created):
    """Criar connections para os sources criados"""
    if not sources_created:
        print("❌ Nenhum source foi processado na etapa anterior")
        return []
    
    workspace_id = Variable.get("airbyte_workspace_id")
    destination_id = Variable.get("airbyte_destination_id_snowflake")
    
    connections_created = []
    
    print(f"🔗 Iniciando criação de connections...")
    print(f"📍 Destination ID: {destination_id}")
    
    for source_info in sources_created:
        source_id = source_info["source_id"]
        month_name = source_info["name"]
        month_code = source_info["month"]
        source_status = source_info.get("status", "unknown")
        
        connection_name = f"Connection_DISPONIBILIDADE_USINA_{month_code}"
        stream_name = f"DISPONIBILIDADE_USINA_{month_code}"
        
        print(f"🔄 Processando connection para {month_name} ({source_id}) - Status: {source_status}")
        
        # Verificar se connection já existe
        existing_connection_id = check_connection_exists(connection_name, workspace_id)
        
        if existing_connection_id:
            print(f"⏭️ Connection já existe para {month_name}: {existing_connection_id}")
            connections_created.append({
                "month": month_code,
                "name": month_name,
                "source_id": source_id,
                "connection_id": existing_connection_id,
                "schedule": "existing"
            })
            continue
        
        # Configuração da connection para execução manual (sem agendamento recorrente)
        connection_data = {
            "name": connection_name,
            "sourceId": source_id,
            "destinationId": destination_id,
            "syncMode": "full_refresh",
            "schedule": {
                "scheduleType": "manual"
            },
            "status": "active"
        }
        
        # Tentar criar connection com handling de conflitos
        success = False
        attempt = 1
        max_attempts = 3
        
        while not success and attempt <= max_attempts:
            print(f"🔄 Tentativa {attempt}/{max_attempts} para {month_name}...")
            
            try:
                response = make_api_request("POST", "/connections", connection_data)
                
                if response.status_code == 200:
                    connection_info = response.json()
                    connection_id = connection_info.get("connectionId")
                    print(f"✅ Connection criada para {month_name}: {connection_id}")
                    connections_created.append({
                        "month": month_code,
                        "name": month_name,
                        "source_id": source_id,
                        "connection_id": connection_id,
                        "schedule": "manual"
                    })
                    success = True
                    
                elif response.status_code == 400 and "conflicting" in response.text.lower():
                    print(f"⚠️ Conflito detectado para {month_name} - tentativa {attempt}")
                    
                    if attempt == 1:
                        # Primeira tentativa: deletar connections conflitantes
                        print(f"� Buscando connections conflitantes para stream: {stream_name}")
                        deleted_count = delete_conflicting_connections(stream_name, workspace_id)
                        print(f"🗑️ {deleted_count} connections conflitantes deletadas")
                        
                    elif attempt == 2:
                        # Segunda tentativa: configuração minimalista
                        print(f"🔄 Usando configuração minimalista...")
                        connection_data = {
                            "name": connection_name,
                            "sourceId": source_id,
                            "destinationId": destination_id
                        }
                    
                    attempt += 1
                    
                else:
                    print(f"❌ Erro na tentativa {attempt} para {month_name}: {response.status_code}")
                    print(f"Resposta: {response.text}")
                    attempt += 1
                    
            except Exception as e:
                print(f"❌ Exceção na tentativa {attempt} para {month_name}: {str(e)}")
                attempt += 1
        
        if not success:
            print(f"❌ Todas as {max_attempts} tentativas falharam para {month_name}")
    
    print(f"\n📊 Resumo: {len(connections_created)} connections criadas")
    
    # Estatísticas de configuração
    manual_count = len([c for c in connections_created if c.get('schedule') == 'manual'])
    minimal_count = len(connections_created) - manual_count
    
    print(f"📋 Configuração completa: {manual_count}")
    print(f"⚡ Configuração minimalista: {minimal_count}")
    
    return connections_created

@task
def trigger_initial_sync(connections_created):
    """Dispara sincronização inicial para todas as connections criadas"""
    context = get_current_context()
    params = context.get("params", {})
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
        month_name = connection_info["name"]
        
        print(f"🔄 Iniciando sync para {month_name} ({connection_id})...")
        
        try:
            # Usar endpoint correto /jobs para iniciar sync
            sync_data = {
                "connectionId": connection_id,
                "jobType": "sync"
            }
            
            response = make_api_request("POST", "/jobs", sync_data)
            
            if response.status_code in [200, 201]:
                job_info = response.json()
                job_id = job_info.get("jobId") or job_info.get("id")
                job_status = job_info.get("status", "unknown")
                print(f"✅ Sync job criado para {month_name}: {job_id} (status: {job_status})")
                sync_results.append({
                    "connection_id": connection_id,
                    "month_name": month_name,
                    "job_id": job_id,
                    "status": "started",
                    "job_status": job_status
                })
            else:
                print(f"❌ Erro ao iniciar sync para {month_name}: {response.status_code}")
                print(f"Resposta: {response.text}")
                sync_results.append({
                    "connection_id": connection_id,
                    "month_name": month_name,
                    "status": "failed"
                })
                
        except Exception as e:
            print(f"❌ Exceção ao iniciar sync para {month_name}: {str(e)}")
            sync_results.append({
                "connection_id": connection_id,
                "month_name": month_name,
                "status": "error",
                "error": str(e)
            })
    
    successful_syncs = len([s for s in sync_results if s.get('status') == 'started'])
    print(f"\n🎉 Resumo: {successful_syncs} sincronizações iniciadas com sucesso!")
    
    return sync_results

# Definir e executar as tasks
with dag:
    sources_created = create_sources_task()
    connections_created = create_connections_task(sources_created)
    sync_results = trigger_initial_sync(connections_created)
    
    # Definir dependências
    sources_created >> connections_created >> sync_results