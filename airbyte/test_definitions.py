#!/usr/bin/env python3
"""
Script para descobrir o definitionId correto do REST API connector
"""
import os
import requests

def get_access_token():
    """Gera um novo access token"""
    # Ler diretamente do arquivo .env
    client_id = "e81f2e01-b900-4da3-9822-73a5fa2f17db"
    client_secret = "4NLhMUBF7vygiZQ8C6cplijbXtBzQEGx"
    
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

def list_source_definitions():
    """Lista todas as definições de source disponíveis"""
    workspace_id = "71262590-7a33-4874-8be1-d80cc8125c1c"
    access_token = get_access_token()
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Tentar diferentes endpoints para listar definições
    endpoints = [
        "/source_definitions",
        "/source-definitions", 
        "/connectors/sources",
        f"/workspaces/{workspace_id}/source_definitions"
    ]
    
    for endpoint in endpoints:
        print(f"\n🔍 Tentando endpoint: {endpoint}")
        response = requests.get(f"https://api.airbyte.com/v1{endpoint}", headers=headers)
        
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Sucesso! Dados encontrados:")
            
            # Procurar por REST API ou HTTP connector
            if 'data' in data:
                for definition in data['data']:
                    name = definition.get('name', '').lower()
                    if 'rest' in name or 'http' in name or 'api' in name:
                        print(f"🎯 Encontrado: {definition.get('name')} - ID: {definition.get('sourceDefinitionId')}")
            else:
                print(f"Estrutura: {list(data.keys()) if isinstance(data, dict) else type(data)}")
        else:
            print(f"❌ Erro: {response.text}")

if __name__ == "__main__":
    try:
        list_source_definitions()
    except Exception as e:
        print(f"❌ Erro: {e}")