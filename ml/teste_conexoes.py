import os
import boto3
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

def test_aws_connection():
    """Testa a conexão com AWS"""
    print("🔗 Testando conexão AWS...")
    try:
        # Criar cliente S3
        s3_client = boto3.client('s3')
        
        # Listar buckets
        response = s3_client.list_buckets()
        print(f"✅ AWS conectado! Buckets disponíveis:")
        for bucket in response['Buckets']:
            print(f"   - {bucket['Name']}")
        
        return s3_client
    except Exception as e:
        print(f"❌ Erro na conexão AWS: {e}")
        return None

def test_snowflake_connection():
    """Testa a conexão com Snowflake"""
    print("\n🔗 Testando conexão Snowflake...")
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        print("✅ Snowflake conectado!")
        return conn
    except Exception as e:
        print(f"❌ Erro na conexão Snowflake: {e}")
        return None

def main():
    """Função principal para testar ambas as conexões"""
    print("🚀 Iniciando testes de conectividade...")
    
    # Testar AWS
    s3_client = test_aws_connection()
    
    # Testar Snowflake
    snowflake_conn = test_snowflake_connection()
    
    if s3_client and snowflake_conn:
        print("\n🎉 Todas as conexões estão funcionando!")
        print("\n📋 Próximos passos possíveis:")
        print("   1. Extrair dados do S3")
        print("   2. Processar dados com pandas")
        print("   3. Treinar modelos com scikit-learn")
        print("   4. Usar Amazon SageMaker para ML avançado")
        print("   5. Salvar resultados no Snowflake")
        
        # Fechar conexão Snowflake
        snowflake_conn.close()
        print("\n🔒 Conexões fechadas")
    else:
        print("\n❌ Algumas conexões falharam. Verifique as configurações.")

if __name__ == "__main__":
    main()