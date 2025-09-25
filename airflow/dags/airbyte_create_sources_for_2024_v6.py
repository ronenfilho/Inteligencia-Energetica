from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import json

# Configurações padrão do DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airbyte_create_sources_for_2024_v6',
    default_args=default_args,
    description='Criar sources e connections no Airbyte para dados de 2024 - v6 com auto token refresh',
    schedule_interval=None,  # Executar manualmente
    catchup=False,
    tags=['airbyte', 'sources', '2024', 'energy'],
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

def create_sources_task():
    """Criar sources para todos os meses de 2024"""
    workspace_id = Variable.get("AIRBYTE_WORKSPACE_ID")
    
    sources_created = []
    
    # Meses de 2024
    months = [
        ("2024-01", "Janeiro"),
        ("2024-02", "Fevereiro"),
        ("2024-03", "Março"),
        ("2024-04", "Abril"),
        ("2024-05", "Maio"),
        ("2024-06", "Junho"),
        ("2024-07", "Julho"),
        ("2024-08", "Agosto"),
        ("2024-09", "Setembro"),
        ("2024-10", "Outubro"),
        ("2024-11", "Novembro"),
        ("2024-12", "Dezembro")
    ]
    
    for month_code, month_name in months:
        print(f"🔄 Criando source para {month_name} ({month_code})...")
        
        source_data = {
            "name": f"DISPONIBILIDADE_USINA_{month_code}",
            "workspaceId": workspace_id,
            "definitionId": "cf40079c-1369-4157-8497-f621b98cb4eb",  # REST API definition ID
            "configuration": {
                "name": f"DISPONIBILIDADE_USINA_{month_code}",
                "url_base": "https://dadosabertos.ons.org.br/api/3/action/datastore_search",
                "path": "/",
                "authenticator": {
                    "type": "NoAuth"
                },
                "request_parameters": {
                    "resource_id": "b1bd71e7-d0ad-4214-9053-cbd58e9564a7",
                    "filters": json.dumps({"dat_referencia": month_code}),
                    "limit": "1000000"
                },
                "paginator": {
                    "type": "OffsetIncrement",
                    "page_size": 1000000
                },
                "request_headers": {}
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

def create_connections_task(**context):
    """Criar connections para os sources criados"""
    # Recupera a lista de sources da task anterior
    sources_created = context['task_instance'].xcom_pull(task_ids='create_sources')
    
    if not sources_created:
        print("❌ Nenhum source foi criado na etapa anterior")
        return []
    
    workspace_id = Variable.get("AIRBYTE_WORKSPACE_ID")
    destination_id = Variable.get("AIRBYTE_DESTINATION_ID")  # a0a784b5-de8a-42dc-a02f-328ba96e644d
    
    connections_created = []
    
    for source_info in sources_created:
        source_id = source_info["source_id"]
        month_name = source_info["name"]
        month_code = source_info["month"]
        
        print(f"🔄 Criando connection para {month_name} ({source_id})...")
        
        connection_data = {
            "name": f"Connection_DISPONIBILIDADE_USINA_{month_code}",
            "sourceId": source_id,
            "destinationId": destination_id,
            "configurations": {
                "streams": [
                    {
                        "name": f"DISPONIBILIDADE_USINA_{month_code}",
                        "syncMode": "full_refresh_overwrite"
                    }
                ]
            }
        }
        
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
            else:
                print(f"❌ Erro ao criar connection para {month_name}: {response.status_code}")
                print(f"Resposta: {response.text}")
                
        except Exception as e:
            print(f"❌ Exceção ao criar connection para {month_name}: {str(e)}")
    
    print(f"\n📊 Resumo: {len(connections_created)} connections criadas com sucesso")
    return connections_created

# Definir as tasks
create_sources = PythonOperator(
    task_id='create_sources',
    python_callable=create_sources_task,
    dag=dag,
)

create_connections = PythonOperator(
    task_id='create_connections',
    python_callable=create_connections_task,
    dag=dag,
)

# Definir dependências
create_sources >> create_connections