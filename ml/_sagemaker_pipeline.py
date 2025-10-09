"""
Script de Deploy e Execução para SageMaker
Integração completa com pipeline de ML para energia solar
"""

import os
import boto3
import sagemaker
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.sklearn.model import SKLearnModel
from sagemaker.inputs import TrainingInput
import pandas as pd
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
from solar_ml_pipeline import SolarEnergyMLPipeline

class SageMakerMLPipeline:
    """
    Pipeline de ML usando Amazon SageMaker para energia solar
    """
    
    def __init__(self):
        load_dotenv()
        self.session = sagemaker.Session()
        self.role = os.getenv('ROLE_ARN')
        self.bucket = os.getenv('S3_BUCKET', 'ronen.filho')
        self.region = os.getenv('AWS_REGION', 'us-east-1')
        self.pipeline = SolarEnergyMLPipeline()
        
    def prepare_data_for_sagemaker(self, df: pd.DataFrame, test_size: float = 0.2) -> tuple:
        """
        Prepara dados no formato esperado pelo SageMaker
        """
        print("📋 Preparando dados para SageMaker...")
        
        # Feature engineering
        df_features = self.pipeline.feature_engineering(df)
        
        # Preparar features
        feature_cols = [col for col in df_features.columns if col not in [
            'geracao_mwh', 'id_usina', 'medicao_data_hora', 'periodo'
        ]]
        
        # Criar dataset final
        dataset = df_features[feature_cols + ['geracao_mwh']].copy()
        dataset = dataset.dropna()
        
        # Split temporal
        split_index = int(len(dataset) * (1 - test_size))
        train_data = dataset.iloc[:split_index]
        val_data = dataset.iloc[split_index:]
        
        print(f"📊 Dados de treino: {len(train_data)} registros")
        print(f"📊 Dados de validação: {len(val_data)} registros")
        
        return train_data, val_data, feature_cols
    
    def upload_data_to_s3(self, train_data: pd.DataFrame, val_data: pd.DataFrame) -> tuple:
        """
        Faz upload dos dados para S3
        """
        print("☁️ Fazendo upload dos dados para S3...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Salvar localmente
        train_path = f"/tmp/train_{timestamp}.csv"
        val_path = f"/tmp/validation_{timestamp}.csv"
        
        train_data.to_csv(train_path, index=False)
        val_data.to_csv(val_path, index=False)
        
        # Upload para S3
        s3_client = boto3.client('s3')
        
        train_s3_key = f"ml-data/train_{timestamp}.csv"
        val_s3_key = f"ml-data/validation_{timestamp}.csv"
        
        s3_client.upload_file(train_path, self.bucket, train_s3_key)
        s3_client.upload_file(val_path, self.bucket, val_s3_key)
        
        train_s3_path = f"s3://{self.bucket}/{train_s3_key}"
        val_s3_path = f"s3://{self.bucket}/{val_s3_key}"
        
        print(f"✅ Dados de treino: {train_s3_path}")
        print(f"✅ Dados de validação: {val_s3_path}")
        
        return train_s3_path, val_s3_path
    
    def train_model_with_sagemaker(self, train_s3_path: str, val_s3_path: str) -> str:
        """
        Treina modelo usando SageMaker
        """
        print("🎯 Iniciando treinamento no SageMaker...")
        
        # Configurar estimator
        sklearn_estimator = SKLearn(
            entry_point='sagemaker_training.py',
            source_dir='.',
            role=self.role,
            instance_type='ml.m5.large',
            framework_version='1.2-1',
            py_version='py3',
            hyperparameters={
                'n-estimators': 200,
                'max-depth': 15,
                'random-state': 42
            }
        )
        
        # Definir inputs
        train_input = TrainingInput(train_s3_path, content_type='text/csv')
        val_input = TrainingInput(val_s3_path, content_type='text/csv')
        
        # Treinar
        sklearn_estimator.fit({
            'train': train_input,
            'validation': val_input
        })
        
        print("✅ Treinamento concluído!")
        return sklearn_estimator.model_data
    
    def deploy_model(self, model_data: str) -> str:
        """
        Deploy do modelo treinado
        """
        print("🚀 Fazendo deploy do modelo...")
        
        # Criar modelo
        sklearn_model = SKLearnModel(
            model_data=model_data,
            role=self.role,
            entry_point='sagemaker_training.py',
            source_dir='.',
            framework_version='1.2-1',
            py_version='py3'
        )
        
        # Deploy
        predictor = sklearn_model.deploy(
            instance_type='ml.t2.medium',
            initial_instance_count=1
        )
        
        endpoint_name = predictor.endpoint_name
        print(f"✅ Modelo deployed! Endpoint: {endpoint_name}")
        
        return endpoint_name
    
    def predict_with_endpoint(self, endpoint_name: str, data: np.ndarray) -> np.ndarray:
        """
        Faz previsões usando endpoint do SageMaker
        """
        print(f"🔮 Fazendo previsões com endpoint: {endpoint_name}")
        
        # Criar predictor
        predictor = sagemaker.predictor.Predictor(
            endpoint_name=endpoint_name,
            serializer=sagemaker.serializers.CSVSerializer(),
            deserializer=sagemaker.deserializers.CSVDeserializer()
        )
        
        # Fazer previsão
        predictions = predictor.predict(data)
        
        return np.array(predictions)
    
    def generate_30_day_forecast(self, endpoint_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Gera previsão para 30 dias usando endpoint do SageMaker
        """
        print("📈 Gerando previsão para 30 dias...")
        
        # Preparar dados base
        df_features = self.pipeline.feature_engineering(df)
        
        # Lógica similar ao método original, mas usando endpoint
        # Por simplicidade, vou usar o método local e depois podemos adaptar
        predictions_df = self.pipeline.predict_next_30_days(df_features)
        
        return predictions_df
    
    def run_complete_pipeline(self):
        """
        Executa pipeline completo de ML
        """
        print("🚀 Iniciando pipeline completo de ML com SageMaker...")
        
        try:
            # 1. Conectar e extrair dados
            if not self.pipeline.connect_snowflake() or not self.pipeline.connect_aws():
                return
            
            df = self.pipeline.extract_data('2023-01-01', '2024-12-31')
            if df.empty:
                print("❌ Nenhum dado extraído")
                return
            
            # 2. Preparar dados
            train_data, val_data, feature_cols = self.prepare_data_for_sagemaker(df)
            
            # 3. Upload para S3
            train_s3_path, val_s3_path = self.upload_data_to_s3(train_data, val_data)
            
            # 4. Treinar com SageMaker
            model_data = self.train_model_with_sagemaker(train_s3_path, val_s3_path)
            
            # 5. Deploy modelo
            endpoint_name = self.deploy_model(model_data)
            
            # 6. Gerar previsões
            predictions_df = self.generate_30_day_forecast(endpoint_name, df)
            
            # 7. Salvar resultados
            s3_path = self.pipeline.save_predictions_to_s3(predictions_df)
            
            print(f"\n🎉 Pipeline SageMaker concluído!")
            print(f"🔗 Endpoint ativo: {endpoint_name}")
            print(f"📊 Previsões salvas: {s3_path}")
            print(f"💰 Lembre-se de deletar o endpoint quando não precisar mais!")
            
            return {
                'endpoint_name': endpoint_name,
                'model_data': model_data,
                'predictions_path': s3_path
            }
            
        except Exception as e:
            print(f"❌ Erro no pipeline: {e}")
        finally:
            self.pipeline.close_connections()

def main():
    """Executar pipeline SageMaker"""
    sagemaker_pipeline = SageMakerMLPipeline()
    result = sagemaker_pipeline.run_complete_pipeline()
    
    if result:
        print("\n📋 Resumo do Pipeline:")
        for key, value in result.items():
            print(f"   {key}: {value}")

if __name__ == "__main__":
    main()