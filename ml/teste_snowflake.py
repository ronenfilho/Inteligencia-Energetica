import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

print("🔗 Conectando ao Snowflake...")
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
)
print("✅ Conexão estabelecida com sucesso!")

try:
    # Usar a tabela correta baseada no projeto
    table_name = "IE_DB.STAGING.stg_usina_geracao"
    
    query = f"SELECT val_geracao_mw as geracao_mwh, id_ons as id_usina, instante as medicao_data_hora FROM {table_name} LIMIT 10"
    print(f"🔍 Executando query: {query}")
    
    df = pd.read_sql(query, conn)
    print(f"📊 Resultado ({len(df)} registros):")
    print(df.head())
    
except Exception as e:
    print(f"❌ Erro: {e}")
finally:
    conn.close()
    print("🔒 Conexão fechada")
