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
    dag_id='aaa_GERACAO_USINA_2_v3',
    schedule=None,  # Execução única, não recorrente
    catchup=False,
    tags=['airbyte', 'sources', '2025', 'energy', 'geracao', 'one-time'],
    description='Criar sources e connections no Airbyte para GERACAO_USINA_2 - v2 execução única',
    default_args={
        'owner': 'data_team',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=2),
    },
    params={
        "start_date": "2022-01-01",  # Data de início da captura (formato YYYY-MM-DD) - 2022+ para dados mensais
        "end_date": "auto",          # "auto" para até mês atual (exclui futuros) ou YYYY-MM-DD para data específica
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
    
    # Log da requisição sendo feita
    print(f"🌐 API Request: {method} {url}")
    if data and method.upper() == "POST":
        print(f"📄 Request Body: {json.dumps(data, indent=2)}")
    
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

def generate_optimized_period_list(start_date_str, end_date_str):
    """Gera lista otimizada de períodos baseada na regra de agrupamento da ONS"""
    from datetime import datetime
    
    # Parse das datas
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    
    if end_date_str == "auto":
        # Usar data atual, mas limitar ao mês atual (não incluir meses futuros)
        end_date = datetime.now()
        print(f"📅 Data atual detectada: {end_date.strftime('%Y-%m-%d')}")
    else:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    periods = []
    
    # Tradução dos meses para português
    month_translations = {
        "January": "Janeiro", "February": "Fevereiro", "March": "Março",
        "April": "Abril", "May": "Maio", "June": "Junho",
        "July": "Julho", "August": "Agosto", "September": "Setembro",
        "October": "Outubro", "November": "Novembro", "December": "Dezembro"
    }
    
    # 1. PROCESSAR ANOS 2000-2021 (arquivo anual) - apenas um source por ano
    print("📊 Analisando período 2000-2021 (arquivos anuais)...")
    start_year = max(2000, start_date.year)  # Não processar antes de 2000
    end_year_legacy = min(2021, end_date.year)  # Não processar depois de 2021 para arquivos anuais
    
    for year in range(start_year, end_year_legacy + 1):
        # Para arquivos anuais, usar Janeiro como representante (month_code será usado apenas no nome)
        year_code = f"{year}-01"  # Usar Janeiro como padrão
        year_name = f"Ano {year}"
        periods.append((year_code, year_name, "annual"))
    
    # 2. PROCESSAR ANOS 2022+ (arquivo mensal) - um source por mês
    print("📊 Analisando período 2022+ (arquivos mensais)...")
    if end_date.year >= 2022:
        # Começar em 2022 ou na data de início, o que for maior
        monthly_start = datetime(max(2022, start_date.year), 1, 1)
        if start_date.year >= 2022:
            monthly_start = start_date.replace(day=1)
        
        current = monthly_start
        current_month = datetime.now().replace(day=1)  # Mês atual para comparação
        
        while current <= end_date:
            # Se for "auto", não incluir meses futuros (posteriores ao mês atual)
            if end_date_str == "auto" and current > current_month:
                print(f"⏹️ Parando em {current.strftime('%Y-%m')} - mês futuro não incluído")
                break
                
            month_code = current.strftime("%Y-%m")
            month_name = current.strftime("%B %Y")
            
            # Aplicar tradução
            for en, pt in month_translations.items():
                month_name = month_name.replace(en, pt)
            
            periods.append((month_code, month_name, "monthly"))
            
            # Avançar para o próximo mês
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
    
    # Log informativo dos períodos que serão processados
    if periods:
        annual_count = len([p for p in periods if p[2] == "annual"])
        monthly_count = len([p for p in periods if p[2] == "monthly"])
        
        print(f"📋 Períodos otimizados que serão processados:")
        print(f"   📅 Arquivos anuais (2000-2021): {annual_count} sources")
        print(f"   📅 Arquivos mensais (2022+): {monthly_count} sources")
        print(f"   📊 Total otimizado: {len(periods)} sources (vs {len(periods) if len(periods) < 50 else '300+'} sources no método anterior)")
        
        if end_date_str == "auto":
            print(f"🚫 Meses futuros excluídos automaticamente (data atual: {datetime.now().strftime('%Y-%m')})")
    
    return periods

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

@task
def explore_api_structure_task():
    """Explora a estrutura da API do Airbyte para entender as chaves e endpoints disponíveis"""
    print("🔍 EXPLORANDO ESTRUTURA DA API DO AIRBYTE")
    print("=" * 60)
    
    # Usar as variáveis que funcionam no v7
    try:
        workspace_id = Variable.get("airbyte_workspace_id")
        print(f"✅ Workspace ID (v7): {workspace_id}")
    except:
        workspace_id = Variable.get("AIRBYTE_WORKSPACE_ID")
        print(f"✅ Workspace ID (maiúsculo): {workspace_id}")
    
    results = {}
    
    # 1. TESTAR ENDPOINTS DE LISTAGEM
    print("\n🌐 TESTANDO ENDPOINTS DE LISTAGEM:")
    print("-" * 40)
    
    endpoints_to_test = [
        ("/workspaces", "Listar todos os workspaces"),
        (f"/workspaces/{workspace_id}", "Detalhes do workspace"),
        (f"/workspaces/{workspace_id}/sources", "Sources do workspace"),
        (f"/workspaces/{workspace_id}/destinations", "Destinations do workspace"),
        (f"/workspaces/{workspace_id}/connections", "Connections do workspace"),
        ("/source-definitions", "Definições de sources disponíveis"),
        ("/destination-definitions", "Definições de destinations disponíveis")
    ]
    
    for endpoint, description in endpoints_to_test:
        try:
            print(f"🔄 Testando: {description}")
            print(f"   Endpoint: GET {endpoint}")
            
            response = make_api_request("GET", endpoint)
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Sucesso ({response.status_code})")
                
                # Analisar estrutura da resposta
                if isinstance(data, dict):
                    if "data" in data:
                        items_count = len(data["data"]) if isinstance(data["data"], list) else "objeto"
                        print(f"   � Itens encontrados: {items_count}")
                        
                        # Se é uma lista, mostrar as chaves do primeiro item
                        if isinstance(data["data"], list) and len(data["data"]) > 0:
                            first_item = data["data"][0]
                            if isinstance(first_item, dict):
                                keys = list(first_item.keys())
                                print(f"   🔑 Chaves principais: {', '.join(keys[:5])}{'...' if len(keys) > 5 else ''}")
                                
                                # Guardar resultado para análise
                                results[description] = {
                                    "endpoint": endpoint,
                                    "status": response.status_code,
                                    "count": len(data["data"]),
                                    "sample_keys": keys,
                                    "sample_item": first_item
                                }
                    else:
                        keys = list(data.keys())
                        print(f"   🔑 Chaves da resposta: {', '.join(keys[:5])}{'...' if len(keys) > 5 else ''}")
                        results[description] = {
                            "endpoint": endpoint,
                            "status": response.status_code,
                            "keys": keys,
                            "data": data
                        }
                
            elif response.status_code == 403:
                print(f"   ❌ Forbidden ({response.status_code}) - Sem permissão")
                results[description] = {"endpoint": endpoint, "status": response.status_code, "error": "Forbidden"}
                
            elif response.status_code == 404:
                print(f"   ⚠️ Not Found ({response.status_code}) - Endpoint não existe")
                results[description] = {"endpoint": endpoint, "status": response.status_code, "error": "Not Found"}
                
            else:
                print(f"   ❌ Erro ({response.status_code}): {response.text[:100]}...")
                results[description] = {"endpoint": endpoint, "status": response.status_code, "error": response.text[:200]}
                
        except Exception as e:
            print(f"   💥 Exceção: {str(e)}")
            results[description] = {"endpoint": endpoint, "error": str(e)}
        
        print()
    
    # 2. ANALISAR SOURCES EXISTENTES EM DETALHES
    print("\n🎯 ANÁLISE DETALHADA DE SOURCES:")
    print("-" * 40)
    
    try:
        sources_response = make_api_request("GET", f"/workspaces/{workspace_id}/sources")
        if sources_response.status_code == 200:
            sources_data = sources_response.json()
            sources = sources_data.get("data", [])
            
            print(f"📊 Total de sources encontrados: {len(sources)}")
            
            if sources:
                print("\n🔍 ESTRUTURA DE UM SOURCE:")
                sample_source = sources[0]
                for key, value in sample_source.items():
                    value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                    print(f"   {key}: {value_preview}")
                
                # Procurar sources que começam com GERACAO_USINA
                geracao_sources = [s for s in sources if s.get("name", "").startswith("GERACAO_USINA")]
                print(f"\n🎯 Sources GERACAO_USINA encontrados: {len(geracao_sources)}")
                
                for source in geracao_sources[:3]:  # Mostrar apenas os 3 primeiros
                    print(f"   📍 {source.get('name', 'N/A')}")
                    print(f"      ID: {source.get('sourceId', 'N/A')}")
                    print(f"      Tipo: {source.get('sourceType', 'N/A')}")
        else:
            print(f"❌ Erro ao listar sources: {sources_response.status_code}")
            
    except Exception as e:
        print(f"💥 Erro ao analisar sources: {str(e)}")
    
    # 3. TESTAR DEFINIÇÕES DE SOURCES
    print("\n🧩 DEFINIÇÕES DE SOURCES DISPONÍVEIS:")
    print("-" * 40)
    
    try:
        definitions_response = make_api_request("GET", "/source-definitions")
        if definitions_response.status_code == 200:
            definitions_data = definitions_response.json()
            definitions = definitions_data.get("data", [])
            
            print(f"📊 Total de definições: {len(definitions)}")
            
            # Procurar definições relacionadas a HTTP/File
            file_definitions = [d for d in definitions if "file" in d.get("name", "").lower() or "http" in d.get("name", "").lower()]
            
            print(f"\n🗂️ Definições File/HTTP encontradas: {len(file_definitions)}")
            for defn in file_definitions[:5]:  # Mostrar apenas as 5 primeiras
                print(f"   📁 {defn.get('name', 'N/A')}")
                print(f"      ID: {defn.get('sourceDefinitionId', 'N/A')}")
                print(f"      Versão: {defn.get('dockerImageTag', 'N/A')}")
                
        else:
            print(f"❌ Erro ao listar definições: {definitions_response.status_code}")
            
    except Exception as e:
        print(f"💥 Erro ao analisar definições: {str(e)}")
    
    print("\n" + "=" * 60)
    print("✅ EXPLORAÇÃO DA API CONCLUÍDA")
    
    return results

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
    
    workspace_id = Variable.get("AIRBYTE_WORKSPACE_ID")
    
    # Usar definitionId da variável ou valor padrão
    try:
        definition_id = Variable.get("airbyte_source_definition_id")
        print(f"📋 Usando definitionId da variável: {definition_id}")
    except Exception:
        definition_id = "778daa7c-feaf-4db6-96f3-70fd645acc77"
        print(f"📋 Usando definitionId padrão: {definition_id}")
    
    # Gerar lista otimizada de períodos
    periods = generate_optimized_period_list(start_date, end_date)
    sources_created = []
    
    print(f"🏗️ Iniciando criação de {len(periods)} sources...")
    print(f"📍 Workspace ID: {workspace_id}")
    print(f"🔧 Definition ID: {definition_id}")
    
    for month_code, month_name, period_type in periods:
        # Nomeação inteligente baseada no tipo de período
        if period_type == "annual":
            year = int(month_code.split("-")[0])
            source_name = f"GERACAO_USINA_2_{year}_ANUAL"
            dataset_name = f"GERACAO_USINA_2_{year}"
        else:  # monthly
            source_name = f"GERACAO_USINA_2_{month_code}"
            dataset_name = f"GERACAO_USINA_2_{month_code}"
        
        print(f"🔄 Processando source para {month_name} ({month_code}) - Tipo: {period_type}")
        
        # Verificar se o source já existe
        existing_source_id = check_source_exists(source_name, workspace_id)
        
        if existing_source_id and not force_recreate:
            print(f"⏭️ Source já existe para {month_name}: {existing_source_id}")
            sources_created.append({
                "month": month_code,
                "name": month_name,
                "source_id": existing_source_id,
                "status": "existing",
                "period_type": period_type
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
        
        # URL do arquivo CSV no S3 da ONS - GERACAO_USINA_2
        year = int(month_code.split("-")[0])
        
        if period_type == "annual":
            # Para anos 2000-2021: formato GERACAO_USINA-2_2020.csv (arquivo anual)
            s3_url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{year}.csv"
            print(f"🔗 URL S3 (arquivo anual): {s3_url}")
        else:  # monthly
            # Para anos 2022+: formato GERACAO_USINA-2_2025_09.csv (arquivo mensal)
            year_month = month_code.replace("-", "_")  # Converter 2025-01 para 2025_01
            s3_url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{year_month}.csv"
            print(f"🔗 URL S3 (arquivo mensal): {s3_url}")
        
        source_data = {
            "name": source_name,
            "workspaceId": workspace_id,
            "definitionId": definition_id,
            "configuration": {
                "dataset_name": dataset_name,
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
                    "status": "created",
                    "period_type": period_type
                })
            else:
                print(f"❌ Erro ao criar source para {month_name}: {response.status_code}")
                print(f"Resposta: {response.text}")
                
        except Exception as e:
            print(f"❌ Exceção ao criar source para {month_name}: {str(e)}")
    
    print(f"\n📊 Resumo: {len(sources_created)} sources processados")
    
    # Estatísticas por status
    created_count = len([s for s in sources_created if s.get('status') == 'created'])
    existing_count = len([s for s in sources_created if s.get('status') == 'existing'])
    
    # Estatísticas por tipo de período
    annual_count = len([s for s in sources_created if s.get('period_type') == 'annual'])
    monthly_count = len([s for s in sources_created if s.get('period_type') == 'monthly'])
    
    print(f"🆕 Criados: {created_count}")
    print(f"📋 Já existiam: {existing_count}")
    print(f"📅 Arquivos anuais (2000-2021): {annual_count}")
    print(f"📅 Arquivos mensais (2022+): {monthly_count}")
    print(f"🎯 Otimização aplicada: Evitou criação de sources duplicados para arquivos anuais")
    
    return sources_created

@task
def create_connections_task(sources_created):
    """Criar connections para os sources criados"""
    if not sources_created:
        print("❌ Nenhum source foi processado na etapa anterior")
        return []
    
    # Debug: Verificar quais variáveis existem
    print("🔍 Verificando variáveis disponíveis...")
    
    variables_to_check = [
        "airbyte_workspace_id",
        "airbyte_destination_id_snowflake", 
        "AIRBYTE_WORKSPACE_ID",
        "AIRBYTE_DESTINATION_ID"
    ]
    
    for var_name in variables_to_check:
        try:
            value = Variable.get(var_name)
            print(f"   ✅ {var_name}: {value}")
        except:
            print(f"   ❌ {var_name}: não encontrada")
    
    # Usar as mesmas variáveis do v7 que funciona
    workspace_id = Variable.get("airbyte_workspace_id")
    destination_id = Variable.get("airbyte_destination_id_snowflake")
    print("📋 Usando variáveis no formato v7 (que funciona): airbyte_workspace_id, airbyte_destination_id_snowflake")
    
    connections_created = []
    
    print(f"🔗 Iniciando criação de connections...")
    print(f"📍 Workspace ID: {workspace_id}")
    print(f"📍 Destination ID: {destination_id}")
    
    # Verificar se os IDs são válidos
    if not workspace_id or not destination_id:
        print("❌ Variáveis de workspace_id ou destination_id não configuradas!")
        print("📋 Variáveis necessárias:")
        print("   Formato v7: airbyte_workspace_id, airbyte_destination_id_snowflake")
        print("   OU formato maiúsculo: AIRBYTE_WORKSPACE_ID, AIRBYTE_DESTINATION_ID")
        return []
    
    # Validar formato dos IDs (devem ser UUIDs)
    import re
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    
    if not re.match(uuid_pattern, workspace_id, re.IGNORECASE):
        print(f"⚠️ Workspace ID não parece ser um UUID válido: {workspace_id}")
    else:
        print(f"✅ Workspace ID formato válido")
        
    if not re.match(uuid_pattern, destination_id, re.IGNORECASE):
        print(f"⚠️ Destination ID não parece ser um UUID válido: {destination_id}")
    else:
        print(f"✅ Destination ID formato válido")
    
    # Tentar validar se o workspace e destination existem
    print("🔍 Validando se workspace e destination existem...")
    
    try:
        # Verificar workspace
        workspace_response = make_api_request("GET", f"/workspaces/{workspace_id}")
        if workspace_response.status_code == 200:
            print("✅ Workspace existe e é acessível")
        else:
            print(f"❌ Erro ao acessar workspace: {workspace_response.status_code}")
            
        # Listar todos os destinations disponíveis no workspace
        print("🔍 Listando destinations disponíveis no workspace...")
        destinations_response = make_api_request("GET", f"/workspaces/{workspace_id}/destinations")
        
        if destinations_response.status_code == 200:
            destinations = destinations_response.json().get("data", [])
            print(f"📋 Encontrados {len(destinations)} destinations no workspace:")
            
            snowflake_destinations = []
            for dest in destinations:
                dest_id = dest.get("destinationId", "N/A")
                dest_name = dest.get("name", "Sem nome")
                dest_type = dest.get("destinationType", "N/A")
                
                print(f"   🎯 {dest_name}")
                print(f"      ID: {dest_id}")
                print(f"      Tipo: {dest_type}")
                
                if "snowflake" in dest_type.lower():
                    snowflake_destinations.append({
                        "id": dest_id,
                        "name": dest_name,
                        "type": dest_type
                    })
            
            if snowflake_destinations:
                print(f"\n❄️ Destinations Snowflake encontrados: {len(snowflake_destinations)}")
                for sf_dest in snowflake_destinations:
                    print(f"   📍 Use este ID: {sf_dest['id']}")
                    print(f"      Nome: {sf_dest['name']}")
                    
                # Se há destinations Snowflake, sugerir usar o primeiro
                if len(snowflake_destinations) == 1:
                    correct_dest_id = snowflake_destinations[0]['id']
                    print(f"\n💡 SUGESTÃO: Configure a variável com o ID correto:")
                    print(f"   AIRBYTE_DESTINATION_ID = {correct_dest_id}")
                    
                    # Tentar usar o ID correto automaticamente
                    print(f"🔄 Tentando usar destination correto automaticamente...")
                    destination_id = correct_dest_id
                    
        else:
            print(f"❌ Erro ao listar destinations: {destinations_response.status_code}")
            
        # Verificar destination específico
        destination_response = make_api_request("GET", f"/destinations/{destination_id}")
        if destination_response.status_code == 200:
            dest_info = destination_response.json()
            print(f"✅ Destination existe: {dest_info.get('name', 'sem nome')}")
        else:
            print(f"❌ Erro ao acessar destination: {destination_response.status_code}")
            if destination_response.status_code == 403:
                print("   Possível problema: destination_id incorreto ou sem permissão")
            elif destination_response.status_code == 404:
                print("   Possível problema: destination_id não existe")
                
    except Exception as e:
        print(f"⚠️ Erro na validação: {e}")
    
    for source_info in sources_created:
        source_id = source_info["source_id"]
        month_name = source_info["name"]
        month_code = source_info["month"]
        source_status = source_info.get("status", "unknown")
        
        connection_name = f"Connection_GERACAO_USINA_2_{month_code}"
        stream_name = f"GERACAO_USINA_2_{month_code}"
        
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
        
        # Tentar criar connection - apenas uma tentativa
        print(f"🔄 Criando connection para {month_name}...")
        
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
                
            else:
                print(f"❌ Erro ao criar connection para {month_name}: {response.status_code}")
                print(f"📄 Resposta completa: {response.text}")
                
                # Debug adicional para erro 403
                if response.status_code == 403:
                    print("🔍 ANÁLISE DO ERRO 403:")
                    print(f"   Source ID: {source_id}")
                    print(f"   Destination ID: {destination_id}")
                    print(f"   Workspace ID: {workspace_id}")
                    print(f"   Connection Name: {connection_name}")
                    
                    # Verificar se é um problema de permissão ou IDs inválidos
                    try:
                        response_json = response.json()
                        if "message" in response_json:
                            print(f"   Mensagem de erro: {response_json['message']}")
                        if "data" in response_json and response_json["data"]:
                            print(f"   Dados adicionais: {response_json['data']}")
                    except:
                        pass
                
                elif response.status_code == 400:
                    print("🔍 ANÁLISE DO ERRO 400:")
                    if "conflicting" in response.text.lower():
                        print("   Possível problema: Stream/connection conflitante")
                        print(f"   Stream name: {stream_name}")
                    else:
                        print("   Possível problema: Estrutura da requisição")
                        
        except Exception as e:
            print(f"❌ Exceção ao criar connection para {month_name}: {str(e)}")
    
    print(f"\n📊 Resumo: {len(connections_created)} connections criadas")
    
    # Estatísticas de criação
    created_count = len([c for c in connections_created if c.get('schedule') == 'manual'])
    existing_count = len([c for c in connections_created if c.get('schedule') == 'existing'])
    
    print(f"🆕 Connections criadas: {created_count}")
    print(f"📋 Connections já existiam: {existing_count}")
    
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