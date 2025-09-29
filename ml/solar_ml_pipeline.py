"""
Configuração e estrutura principal para ML com dados de energia solar
Projeto: Previsão de Geração Fotovoltaica - Goiás
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple, Dict, Any
import warnings
warnings.filterwarnings('ignore')

# Data processing
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# ML Algorithms
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from xgboost import XGBRegressor

# AWS & Snowflake
import boto3
import snowflake.connector
import sagemaker
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.inputs import TrainingInput
from dotenv import load_dotenv

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns

class SolarEnergyMLPipeline:
    """
    Pipeline completo para Machine Learning com dados de energia solar
    """
    
    def __init__(self):
        load_dotenv()
        self.snowflake_conn = None
        self.sagemaker_session = None
        self.s3_client = None
        self.scaler = StandardScaler()
        self.models = {}
        self.model_performance = {}
        
    def connect_snowflake(self) -> bool:
        """Conecta ao Snowflake"""
        try:
            self.snowflake_conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            )
            print("✅ Snowflake conectado com sucesso!")
            return True
        except Exception as e:
            print(f"❌ Erro na conexão Snowflake: {e}")
            return False
    
    def connect_aws(self) -> bool:
        """Conecta aos serviços AWS"""
        try:
            self.s3_client = boto3.client('s3')
            self.sagemaker_session = sagemaker.Session()
            print("✅ AWS conectado com sucesso!")
            return True
        except Exception as e:
            print(f"❌ Erro na conexão AWS: {e}")
            return False
    
    def extract_data(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Extrai dados do Snowflake para energia solar em Goiás
        """
        base_query = """
        SELECT 
            val_geracao_mw as geracao_mwh,
            id_ons as id_usina,
            instante as medicao_data_hora,
            EXTRACT(YEAR FROM instante) as ano,
            EXTRACT(MONTH FROM instante) as mes,
            EXTRACT(DAY FROM instante) as dia,
            EXTRACT(HOUR FROM instante) as hora,
            DAYOFWEEK(instante) as dia_semana,
            DAYOFYEAR(instante) as dia_ano
        FROM IE_DB.STAGING.stg_usina_geracao
        WHERE 
            nom_tipocombustivel = 'Fotovoltaica'
            AND ID_ESTADO = 'GO'
        """
        
        if start_date and end_date:
            base_query += f" AND DATE(instante) BETWEEN '{start_date}' AND '{end_date}'"
            
        base_query += " ORDER BY instante"
        
        print(f"🔍 Extraindo dados: {base_query}")
        
        try:
            df = pd.read_sql(base_query, self.snowflake_conn)
            print(f"📊 Dados extraídos: {len(df)} registros")
            return df
        except Exception as e:
            print(f"❌ Erro na extração: {e}")
            return pd.DataFrame()
    
    def feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engenharia de features para dados de energia solar
        """
        print("🔧 Realizando feature engineering...")
        
        df = df.copy()
        
        # Normalizar nomes das colunas (Snowflake retorna em maiúsculo)
        column_mapping = {
            'MEDICAO_DATA_HORA': 'medicao_data_hora',
            'GERACAO_MWH': 'geracao_mwh',
            'ID_USINA': 'id_usina',
            'ANO': 'ano',
            'MES': 'mes',
            'DIA': 'dia',
            'HORA': 'hora',
            'DIA_SEMANA': 'dia_semana',
            'DIA_ANO': 'dia_ano'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]
        
        # Converter data
        df['medicao_data_hora'] = pd.to_datetime(df['medicao_data_hora'])
        
        # Features temporais
        df['hora_sin'] = np.sin(2 * np.pi * df['hora'] / 24)
        df['hora_cos'] = np.cos(2 * np.pi * df['hora'] / 24)
        df['mes_sin'] = np.sin(2 * np.pi * df['mes'] / 12)
        df['mes_cos'] = np.cos(2 * np.pi * df['mes'] / 12)
        df['dia_ano_sin'] = np.sin(2 * np.pi * df['dia_ano'] / 365)
        df['dia_ano_cos'] = np.cos(2 * np.pi * df['dia_ano'] / 365)
        
        # Features de lag (valores anteriores)
        df = df.sort_values(['id_usina', 'medicao_data_hora'])
        for lag in [1, 2, 3, 24, 168]:  # 1h, 2h, 3h, 1dia, 1semana
            df[f'geracao_lag_{lag}'] = df.groupby('id_usina')['geracao_mwh'].shift(lag)
        
        # Médias móveis
        for window in [3, 6, 12, 24]:
            df[f'geracao_ma_{window}'] = df.groupby('id_usina')['geracao_mwh'].rolling(window).mean().reset_index(0, drop=True)
        
        # Features de período (manhã, tarde, noite)
        df['periodo'] = pd.cut(df['hora'], 
                              bins=[0, 6, 12, 18, 24], 
                              labels=['noite', 'manha', 'tarde', 'noite_tardia'],
                              include_lowest=True)
        df['periodo_encoded'] = df['periodo'].astype(str).map({
            'noite': 0, 'manha': 1, 'tarde': 2, 'noite_tardia': 0
        })
        
        # Dropar linhas com NaN (devido aos lags)
        df = df.dropna()
        
        print(f"✅ Features criadas. Dataset final: {len(df)} registros")
        return df
    
    def prepare_training_data(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, list]:
        """
        Prepara dados para treinamento
        """
        print("📋 Preparando dados para treinamento...")
        
        # Features para treino (excluindo target e colunas não numéricas)
        exclude_cols = [
            'geracao_mwh', 'id_usina', 'medicao_data_hora', 'periodo',
            'ID_USINA', 'MEDICAO_DATA_HORA', 'GERACAO_MWH'  # Versões em maiúsculo também
        ]
        
        feature_cols = [col for col in df.columns if col not in exclude_cols]
        
        # Verificar se todas as features são numéricas
        numeric_cols = []
        for col in feature_cols:
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                numeric_cols.append(col)
        
        X = df[numeric_cols].values
        y = df['geracao_mwh'].values
        
        print(f"📊 Features selecionadas: {len(numeric_cols)}")
        print(f"📊 Features: {numeric_cols}")
        print(f"📊 Exemplos de treinamento: {len(X)}")
        
        return X, y, numeric_cols
    
    def train_models(self, X: np.ndarray, y: np.ndarray) -> Dict[str, Any]:
        """
        Treina múltiplos modelos de regressão
        """
        print("🎯 Iniciando treinamento dos modelos...")
        
        # Configuração de validação temporal
        tscv = TimeSeriesSplit(n_splits=5)
        print(f"   🔄 Usando TimeSeriesSplit com {tscv.n_splits} folds para validação temporal...")
        
        # Split final para modelo (80% treino, 20% teste)
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        
        # Normalizar dados
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Modelos a serem treinados
        models_config = {
            'random_forest': RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            ),
            'xgboost': XGBRegressor(
                n_estimators=100,
                max_depth=6,
                learning_rate=0.1,
                random_state=42
            ),
            'linear_regression': LinearRegression()
        }
        
        # Treinar cada modelo com validação temporal
        for name, model in models_config.items():
            print(f"🔄 Treinando {name} com TimeSeriesSplit...")
            
            # Validação cruzada temporal
            cv_scores_r2 = []
            cv_scores_mae = []
            cv_scores_rmse = []
            
            fold_num = 1
            for train_idx, test_idx in tscv.split(X):
                X_train_fold, X_test_fold = X[train_idx], X[test_idx]
                y_train_fold, y_test_fold = y[train_idx], y[test_idx]
                
                if name == 'linear_regression':
                    # Scaling apenas para Linear Regression
                    X_train_fold_scaled = self.scaler.fit_transform(X_train_fold)
                    X_test_fold_scaled = self.scaler.transform(X_test_fold)
                    model.fit(X_train_fold_scaled, y_train_fold)
                    y_pred_fold = model.predict(X_test_fold_scaled)
                else:
                    model.fit(X_train_fold, y_train_fold)
                    y_pred_fold = model.predict(X_test_fold)
                
                # Métricas do fold
                r2_fold = r2_score(y_test_fold, y_pred_fold)
                mae_fold = mean_absolute_error(y_test_fold, y_pred_fold)
                rmse_fold = np.sqrt(mean_squared_error(y_test_fold, y_pred_fold))
                
                cv_scores_r2.append(r2_fold)
                cv_scores_mae.append(mae_fold)
                cv_scores_rmse.append(rmse_fold)
                
                fold_num += 1
            
            # Treinar modelo final
            if name == 'linear_regression':
                X_train_scaled = self.scaler.fit_transform(X_train)
                X_test_scaled = self.scaler.transform(X_test)
                model.fit(X_train_scaled, y_train)
                y_pred = model.predict(X_test_scaled)
            else:
                model.fit(X_train, y_train)
                y_pred = model.predict(X_test)
            
            # Métricas finais
            mae_final = mean_absolute_error(y_test, y_pred)
            mse_final = mean_squared_error(y_test, y_pred)
            rmse_final = np.sqrt(mse_final)
            r2_final = r2_score(y_test, y_pred)
            
            self.models[name] = model
            self.model_performance[name] = {
                'CV_R2_mean': np.mean(cv_scores_r2),
                'CV_R2_std': np.std(cv_scores_r2),
                'CV_MAE_mean': np.mean(cv_scores_mae),
                'CV_MAE_std': np.std(cv_scores_mae),
                'CV_RMSE_mean': np.mean(cv_scores_rmse),
                'CV_RMSE_std': np.std(cv_scores_rmse),
                'Final_MAE': mae_final,
                'Final_MSE': mse_final,
                'Final_RMSE': rmse_final,
                'Final_R2': r2_final
            }
            
            print(f"   CV: R²={np.mean(cv_scores_r2):.4f}(±{np.std(cv_scores_r2):.4f}), RMSE={np.mean(cv_scores_rmse):.2f}(±{np.std(cv_scores_rmse):.2f})")
            print(f"   Final: R²={r2_final:.4f}, RMSE={rmse_final:.2f}, MAE={mae_final:.2f}")
        
        return self.models
    
    def evaluate_models(self) -> pd.DataFrame:
        """
        Avalia e compara performance dos modelos
        """
        print("📊 Avaliando performance dos modelos...")
        
        performance_df = pd.DataFrame(self.model_performance).T
        performance_df = performance_df.round(4)
        
        print("\n🏆 Ranking dos Modelos (por R² Final):")
        ranking = performance_df.sort_values('Final_R2', ascending=False)
        print(ranking[['CV_R2_mean', 'CV_R2_std', 'Final_R2', 'CV_MAE_mean', 'Final_MAE']])
        
        return ranking
    
    def predict_next_30_days(self, df: pd.DataFrame, usina_id: str = None) -> pd.DataFrame:
        """
        Faz previsões para os próximos 30 dias
        """
        print("🔮 Gerando previsões para os próximos 30 dias...")
        
        # Pegar último registro para cada usina
        if usina_id:
            last_records = df[df['id_usina'] == usina_id].tail(24)  # Últimas 24h
        else:
            last_records = df.groupby('id_usina').tail(24)
        
        # Gerar datas futuras
        last_date = df['medicao_data_hora'].max()
        future_dates = pd.date_range(
            start=last_date + timedelta(hours=1),
            periods=30*24,  # 30 dias x 24 horas
            freq='H'
        )
        
        # Usar XGBoost para previsões (mais robusto que linear regression)
        if 'xgboost' in self.models:
            best_model_name = 'xgboost'
            best_model = self.models['xgboost']
        else:
            best_model_name = max(self.model_performance.keys(), 
                                 key=lambda x: self.model_performance[x]['R2'])
            best_model = self.models[best_model_name]
        
        print(f"🎯 Usando modelo: {best_model_name}")
        
        # Criar estrutura para previsões
        predictions = []
        
        # Para cada usina
        for usina in last_records['id_usina'].unique():
            usina_data = last_records[last_records['id_usina'] == usina].copy()
            
            for future_date in future_dates:
                # Extrair features temporais
                hour = future_date.hour
                month = future_date.month
                day = future_date.day
                day_of_week = future_date.dayofweek + 1
                day_of_year = future_date.dayofyear
                
                # Features básicas
                features = {
                    'ano': future_date.year,
                    'mes': month,
                    'dia': day,
                    'hora': hour,
                    'dia_semana': day_of_week,
                    'dia_ano': day_of_year,
                    'hora_sin': np.sin(2 * np.pi * hour / 24),
                    'hora_cos': np.cos(2 * np.pi * hour / 24),
                    'mes_sin': np.sin(2 * np.pi * month / 12),
                    'mes_cos': np.cos(2 * np.pi * month / 12),
                    'dia_ano_sin': np.sin(2 * np.pi * day_of_year / 365),
                    'dia_ano_cos': np.cos(2 * np.pi * day_of_year / 365),
                    'periodo_encoded': 1 if 6 <= hour < 12 else 2 if 12 <= hour < 18 else 0
                }
                
                # Features de lag (simplificado - usar últimos valores)
                last_values = usina_data['geracao_mwh'].tail(168).values  # Última semana
                if len(last_values) >= 168:
                    features.update({
                        'geracao_lag_1': last_values[-1],
                        'geracao_lag_2': last_values[-2],
                        'geracao_lag_3': last_values[-3],
                        'geracao_lag_24': last_values[-24] if len(last_values) >= 24 else last_values[-1],
                        'geracao_lag_168': last_values[-168] if len(last_values) >= 168 else last_values[-1]
                    })
                else:
                    features.update({
                        'geracao_lag_1': last_values[-1] if len(last_values) > 0 else 0,
                        'geracao_lag_2': last_values[-2] if len(last_values) > 1 else 0,
                        'geracao_lag_3': last_values[-3] if len(last_values) > 2 else 0,
                        'geracao_lag_24': last_values[-1] if len(last_values) > 0 else 0,
                        'geracao_lag_168': last_values[-1] if len(last_values) > 0 else 0
                    })
                
                # Médias móveis (simplificado)
                features.update({
                    'geracao_ma_3': np.mean(last_values[-3:]) if len(last_values) >= 3 else np.mean(last_values),
                    'geracao_ma_6': np.mean(last_values[-6:]) if len(last_values) >= 6 else np.mean(last_values),
                    'geracao_ma_12': np.mean(last_values[-12:]) if len(last_values) >= 12 else np.mean(last_values),
                    'geracao_ma_24': np.mean(last_values[-24:]) if len(last_values) >= 24 else np.mean(last_values)
                })
                
                # Fazer previsão (XGBoost não precisa de scaling)
                feature_vector = np.array(list(features.values())).reshape(1, -1)
                prediction = best_model.predict(feature_vector)[0]
                
                # Garantir que previsão não seja negativa
                prediction = max(0, prediction)
                
                predictions.append({
                    'id_usina': usina,
                    'medicao_data_hora': future_date,
                    'geracao_mwh': prediction,
                    'modelo_usado': best_model_name
                })
        
        predictions_df = pd.DataFrame(predictions)
        print(f"✅ Previsões geradas: {len(predictions_df)} registros")
        
        return predictions_df
    
    def save_predictions_to_s3(self, predictions_df: pd.DataFrame, bucket: str = 'ronen.filho') -> str:
        """
        Salva previsões no S3
        """
        print("💾 Salvando previsões no S3...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"solar_predictions_go_{timestamp}.csv"
        
        try:
            predictions_df.to_csv(f"/tmp/{filename}", index=False)
            self.s3_client.upload_file(f"/tmp/{filename}", bucket, f"ml-predictions/{filename}")
            
            s3_path = f"s3://{bucket}/ml-predictions/{filename}"
            print(f"✅ Previsões salvas em: {s3_path}")
            
            return s3_path
        except Exception as e:
            print(f"❌ Erro ao salvar no S3: {e}")
            return ""
    
    def close_connections(self):
        """Fecha conexões"""
        if self.snowflake_conn:
            self.snowflake_conn.close()
        print("🔒 Conexões fechadas")

def main():
    """Função principal para executar o pipeline"""
    pipeline = SolarEnergyMLPipeline()
    
    # Conectar aos serviços
    if not pipeline.connect_snowflake() or not pipeline.connect_aws():
        return
    
    try:
        # 1. Extrair dados (2022 até atual)
        current_date = datetime.now().strftime('%Y-%m-%d')
        df = pipeline.extract_data('2022-01-01', current_date)
        if df.empty:
            print("❌ Nenhum dado extraído")
            return
        
        # 2. Feature engineering
        df_features = pipeline.feature_engineering(df)
        
        # 3. Preparar dados
        X, y, feature_cols = pipeline.prepare_training_data(df_features)
        
        # 4. Treinar modelos
        models = pipeline.train_models(X, y)
        
        # 5. Avaliar modelos
        performance = pipeline.evaluate_models()
        
        # 6. Fazer previsões
        predictions = pipeline.predict_next_30_days(df_features)
        
        # 7. Salvar no S3
        s3_path = pipeline.save_predictions_to_s3(predictions)
        
        print(f"\n🎉 Pipeline concluído com sucesso!")
        print(f"📊 Performance dos modelos:")
        print(performance)
        print(f"🔮 Previsões salvas em: {s3_path}")
        
    except Exception as e:
        print(f"❌ Erro no pipeline: {e}")
    finally:
        pipeline.close_connections()

if __name__ == "__main__":
    main()