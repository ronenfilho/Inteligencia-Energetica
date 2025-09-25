#foi
from __future__ import annotations

import pendulum
import requests
import json
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.models import Variable

dag = DAG(
    dag_id='airbyte_create_sources_for_2024_v6',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # Executar manualmente
    catchup=False,
    tags=['airbyte', 'sources', '2025', 'energy'],
    description='Criar sources e connections no Airbyte para dados de 2025 - v6 com auto token refresh',
    default_args={
        'owner': 'data_team',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': pendulum.duration(minutes=5),
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

@task
def create_sources_task():
    """Criar sources para todos os meses de 2025"""
    workspace_id = Variable.get("airbyte_workspace_id")
    
    # Usar definitionId da variável ou valor padrão
    try:
        definition_id = Variable.get("airbyte_source_definition_id")
        print(f"📋 Usando definitionId da variável: {definition_id}")
    except:
        definition_id = "778daa7c-feaf-4db6-96f3-70fd645acc77"
        print(f"📋 Usando definitionId padrão: {definition_id}")
    
    sources_created = []
    
    # Meses de 2025
    months = [
        ("2025-01", "Janeiro"),
        ("2025-02", "Fevereiro"),
        ("2025-03", "Março"),
        ("2025-04", "Abril"),
        ("2025-05", "Maio"),
        ("2025-06", "Junho"),
        ("2025-07", "Julho"),
        ("2025-08", "Agosto"),
        ("2025-09", "Setembro"),
        ("2025-10", "Outubro"),
        ("2025-11", "Novembro"),
        ("2025-12", "Dezembro")
    ]
    
    print(f"🏗️ Iniciando criação de {len(months)} sources para 2025...")
    print(f"📍 Workspace ID: {workspace_id}")
    print(f"🔧 Definition ID: {definition_id}")
    
    for month_code, month_name in months:
        print(f"🔄 Criando source para {month_name} ({month_code})...")
        
        # URL do arquivo CSV no S3 da ONS
        year_month = month_code.replace("-", "_")  # Converter 2025-01 para 2025_01
        s3_url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/disponibilidade_usina_ho/DISPONIBILIDADE_USINA_{year_month}.csv"
        print(f"🔗 URL S3 gerada: {s3_url}")
        
        source_data = {
            "name": f"DISPONIBILIDADE_USINA_{month_code}",
            "workspaceId": workspace_id,
            "definitionId": definition_id,  # File connector definition ID
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
                    "source_id": source_id
                })
            else:
                print(f"❌ Erro ao criar source para {month_name}: {response.status_code}")
                print(f"Resposta: {response.text}")
                
        except Exception as e:
            print(f"❌ Exceção ao criar source para {month_name}: {str(e)}")
    
    print(f"\n📊 Resumo: {len(sources_created)} sources criados com sucesso")
    return sources_created

@task
def create_connections_task(sources_created):
    """Criar connections para os sources criados"""
    if not sources_created:
        print("❌ Nenhum source foi criado na etapa anterior")
        return []
    
    workspace_id = Variable.get("airbyte_workspace_id")
    destination_id = Variable.get("airbyte_destination_id_snowflake")  # a0a784b5-de8a-42dc-a02f-328ba96e644d
    
    connections_created = []
    
    for source_info in sources_created:
        source_id = source_info["source_id"]
        month_name = source_info["name"]
        month_code = source_info["month"]
        
        print(f"🔄 Criando connection para {month_name} ({source_id})...")
        
        # Usar configuração simplificada que funciona na maioria dos casos
        print(f"🔧 Criando connection com configuração otimizada...")
        
        connection_data = {
            "name": f"Connection_DISPONIBILIDADE_USINA_{month_code}",
            "sourceId": source_id,
            "destinationId": destination_id,
            "syncMode": "full_refresh",
            "schedule": {
                "scheduleType": "manual"
            }
        }
        
        # Tentar criar connection com diferentes configurações
        success = False
        
        # Tentativa 1: Configuração completa
        try:
            response = make_api_request("POST", "/connections", connection_data)
            
            if response.status_code == 200:
                connection_info = response.json()
                connection_id = connection_info.get("connectionId")
                print(f"✅ Connection criada com sucesso para {month_name}: {connection_id}")
                connections_created.append({
                    "month": month_code,
                    "name": month_name,
                    "source_id": source_id,
                    "connection_id": connection_id
                })
                success = True
            else:
                print(f"❌ Primeira tentativa falhou para {month_name}: {response.status_code}")
                print(f"Resposta: {response.text}")
                
        except Exception as e:
            print(f"❌ Exceção na primeira tentativa para {month_name}: {str(e)}")
        
        # Tentativa 2: Configuração minimalista
        if not success:
            print(f"🔄 Tentando configuração minimalista para {month_name}...")
            minimal_config = {
                "name": f"Connection_DISPONIBILIDADE_USINA_{month_code}",
                "sourceId": source_id,
                "destinationId": destination_id
            }
            
            try:
                response = make_api_request("POST", "/connections", minimal_config)
                
                if response.status_code == 200:
                    connection_info = response.json()
                    connection_id = connection_info.get("connectionId")
                    print(f"✅ Connection minimalista criada para {month_name}: {connection_id}")
                    connections_created.append({
                        "month": month_code,
                        "name": month_name,
                        "source_id": source_id,
                        "connection_id": connection_id
                    })
                else:
                    print(f"❌ Segunda tentativa também falhou para {month_name}: {response.status_code}")
                    print(f"Resposta: {response.text}")
                    
            except Exception as e:
                print(f"❌ Todas as tentativas falharam para {month_name}: {str(e)}")
    
    print(f"\n📊 Resumo: {len(connections_created)} connections criadas com sucesso")
    return connections_created

# Definir e executar as tasks
with dag:
    sources_created = create_sources_task()
    connections_created = create_connections_task(sources_created)
    
    # Definir dependências
    sources_created >> connections_created