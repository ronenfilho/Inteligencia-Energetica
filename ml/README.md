# Estrutura ML Profissional - Energia Solar Goiás

## 📋 Visão Geral

Pipeline completo de Machine Learning para previsão de geração de energia fotovoltaica no estado de Goiás, utilizando dados do Snowflake e Amazon SageMaker.

## 🎯 Objetivos

- **Previsão**: Geração de energia solar para próximos 30 dias
- **Algoritmos**: Random Forest, XGBoost e Regressão Linear
- **Dados**: Energia fotovoltaica em Goiás (IE_DB.STAGING.stg_usina_geracao)
- **Deploy**: Amazon SageMaker para produção

## 📁 Estrutura de Arquivos

```
ml/
├── test_ml_pipeline.py        # 🎯 ARQUIVO PRINCIPAL - Execute este!
├── solar_ml_pipeline.py       # Classes e funções do pipeline
├── sagemaker_training.py      # Script para treinamento SageMaker
├── sagemaker_pipeline.py      # Pipeline completo SageMaker (produção)
├── teste_snowflake.py         # Teste conexão Snowflake apenas
├── teste_conexoes.py          # Teste todas conexões
├── .env                       # Variáveis de ambiente
├── README.md                  # Esta documentação
└── resultados/                # CSVs gerados automaticamente
    ├── previsoes_energia_solar_30_dias.csv
    ├── performance_modelos.csv
    └── resumo_previsoes_por_usina.csv
```

## 🚀 Como Executar

### 1. Teste Principal (RECOMENDADO)
```bash
cd ml
python test_ml_pipeline.py
```
**Executa todos os testes: conectividade, dados, ML, previsões e exporta CSVs**

### 2. Pipeline Completo com SageMaker (Produção)
```bash
python sagemaker_pipeline.py
```
**Para deploy em produção na AWS**

### 3. Testes Individuais
```bash
python teste_conexoes.py        # Apenas conectividade
python teste_snowflake.py       # Apenas Snowflake
```

## 📊 Features Utilizadas

### Temporais
- `ano`, `mes`, `dia`, `hora`, `dia_semana`, `dia_ano`
- `hora_sin/cos`, `mes_sin/cos`, `dia_ano_sin/cos` (encoding cíclico)

### Lags (Valores Anteriores)
- `geracao_lag_1` (1 hora atrás)
- `geracao_lag_2` (2 horas atrás)
- `geracao_lag_3` (3 horas atrás)
- `geracao_lag_24` (1 dia atrás)
- `geracao_lag_168` (1 semana atrás)

### Médias Móveis
- `geracao_ma_3` (3 horas)
- `geracao_ma_6` (6 horas)
- `geracao_ma_12` (12 horas)
- `geracao_ma_24` (24 horas)

### Categóricas
- `periodo_encoded` (manhã=1, tarde=2, noite=0)

## 🎯 Modelos Implementados

1. **Random Forest** - Modelo ensemble robusto
2. **XGBoost** - Gradient boosting de alta performance
3. **Linear Regression** - Baseline linear com scaling

## 📈 Métricas de Avaliação

- **R²** (Coeficiente de Determinação)
- **RMSE** (Root Mean Square Error)
- **MAE** (Mean Absolute Error)
- **MSE** (Mean Square Error)

## 🔧 Configurações Necessárias

### Arquivo .env
```properties
# Snowflake
SNOWFLAKE_USER="SVC_PIPELINE_USER_IE"
SNOWFLAKE_PASSWORD="sua_senha"
SNOWFLAKE_ACCOUNT="sua_conta"
SNOWFLAKE_WAREHOUSE="IE_TRANSFORM_WH"
SNOWFLAKE_DATABASE="IE_DB"
SNOWFLAKE_SCHEMA="STAGING"

# AWS
AWS_REGION="us-east-1"
S3_BUCKET="seu-bucket"
ROLE_ARN="arn:aws:iam::123456789012:role/SageMakerExecutionRole"
```

### AWS Credentials (~/.aws/credentials)
```ini
[default]
aws_access_key_id=SEU_ACCESS_KEY
aws_secret_access_key=SEU_SECRET_KEY
aws_session_token=SEU_SESSION_TOKEN
```

## 📦 Dependências

```bash
pip install snowflake-connector-python boto3 sagemaker pandas numpy scikit-learn xgboost matplotlib seaborn python-dotenv
```

## 💡 Fluxo de Execução

### Local Pipeline
1. **Conexão** → Snowflake + AWS
2. **Extração** → Dados de energia solar GO
3. **Feature Engineering** → Criação de features temporais e lags
4. **Treinamento** → 3 modelos de regressão
5. **Avaliação** → Métricas de performance
6. **Previsão** → 30 dias futuros
7. **Armazenamento** → Resultados no S3

### SageMaker Pipeline
1. **Preparação** → Upload dados para S3
2. **Treinamento** → Instância ml.m5.large
3. **Deploy** → Endpoint ml.t2.medium
4. **Inferência** → Previsões via endpoint
5. **Monitoramento** → Logs e métricas CloudWatch

## 🎉 Output Esperado

### Previsões CSV
```csv
id_usina,medicao_data_hora,geracao_mwh,modelo_usado
USINA001,2025-01-01 00:00:00,0.0,random_forest
USINA001,2025-01-01 01:00:00,0.0,random_forest
USINA001,2025-01-01 06:00:00,125.5,random_forest
...
```

### Performance Report
```
Model Performance:
                MAE      MSE     RMSE       R2
random_forest  45.2   3250.8   57.0    0.85
xgboost        48.1   3456.2   58.8    0.84
linear_reg     52.3   3890.5   62.4    0.82
```

## 🛠️ Troubleshooting

### Conexão Snowflake
- Verificar credenciais no .env
- Testar com teste_snowflake.py

### AWS/SageMaker
- Verificar IAM role permissions
- Confirmar região us-east-1
- Testar com teste_conexoes.py

### Dados Vazios
- Verificar filtros: Fotovoltaica + GO
- Conferir período de datas
- Executar análise exploratória

## 🔒 Segurança

- **Nunca** commitar arquivos .env
- Usar IAM roles com permissões mínimas
- Deletar endpoints SageMaker após uso
- Criptografar dados sensíveis no S3

## 💰 Custos AWS

### SageMaker Training
- ml.m5.large: ~$0.10/hora
- Tempo médio: 15-30 minutos

### SageMaker Endpoint
- ml.t2.medium: ~$0.05/hora
- **IMPORTANTE**: Deletar após uso!

### S3 Storage
- Dados + modelos: <$0.01/GB/mês

## 📞 Suporte

Para dúvidas ou problemas:
1. Verificar logs de execução
2. Testar conexões individuais
3. Revisar configurações AWS/Snowflake
4. Consultar documentação SageMaker