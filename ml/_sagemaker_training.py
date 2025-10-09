"""
Script de treinamento para Amazon SageMaker
Focado em modelos de regressão para energia solar
"""

import os
import argparse
import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import json

def model_fn(model_dir):
    """Carrega modelo treinado para inferência no SageMaker"""
    model = joblib.load(os.path.join(model_dir, 'solar_model.joblib'))
    scaler = joblib.load(os.path.join(model_dir, 'scaler.joblib'))
    return {'model': model, 'scaler': scaler}

def input_fn(request_body, request_content_type):
    """Processa input para inferência"""
    if request_content_type == 'text/csv':
        df = pd.read_csv(request_body)
        return df.values
    else:
        raise ValueError(f"Unsupported content type: {request_content_type}")

def predict_fn(input_data, model_dict):
    """Faz predições"""
    model = model_dict['model']
    scaler = model_dict['scaler']
    
    # Normalizar se necessário
    if hasattr(model, 'predict'):
        scaled_data = scaler.transform(input_data)
        predictions = model.predict(scaled_data)
    else:
        predictions = model.predict(input_data)
    
    return predictions

def output_fn(prediction, content_type):
    """Formata output"""
    if content_type == 'text/csv':
        return ','.join(map(str, prediction))
    else:
        raise ValueError(f"Unsupported content type: {content_type}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    # SageMaker specific arguments
    parser.add_argument('--model-dir', type=str, default=os.environ.get('SM_MODEL_DIR'))
    parser.add_argument('--train', type=str, default=os.environ.get('SM_CHANNEL_TRAIN'))
    parser.add_argument('--validation', type=str, default=os.environ.get('SM_CHANNEL_VALIDATION'))
    
    # Hyperparameters
    parser.add_argument('--n-estimators', type=int, default=100)
    parser.add_argument('--max-depth', type=int, default=10)
    parser.add_argument('--random-state', type=int, default=42)
    
    args = parser.parse_args()
    
    # Carregar dados de treinamento
    train_data = pd.read_csv(os.path.join(args.train, 'train.csv'))
    
    # Preparar features e target
    feature_cols = [col for col in train_data.columns if col != 'geracao_mwh']
    X_train = train_data[feature_cols]
    y_train = train_data['geracao_mwh']
    
    # Normalizar features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    
    # Treinar modelo
    model = RandomForestRegressor(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        random_state=args.random_state,
        n_jobs=-1
    )
    
    model.fit(X_train_scaled, y_train)
    
    # Avaliar se temos dados de validação
    if args.validation:
        val_data = pd.read_csv(os.path.join(args.validation, 'validation.csv'))
        X_val = val_data[feature_cols]
        y_val = val_data['geracao_mwh']
        X_val_scaled = scaler.transform(X_val)
        
        y_pred = model.predict(X_val_scaled)
        
        # Métricas
        mae = mean_absolute_error(y_val, y_pred)
        mse = mean_squared_error(y_val, y_pred)
        r2 = r2_score(y_val, y_pred)
        
        metrics = {
            'mae': mae,
            'mse': mse,
            'rmse': np.sqrt(mse),
            'r2': r2
        }
        
        print(f"Validation Metrics: {metrics}")
    
    # Salvar modelo e scaler
    joblib.dump(model, os.path.join(args.model_dir, 'solar_model.joblib'))
    joblib.dump(scaler, os.path.join(args.model_dir, 'scaler.joblib'))
    
    print("Model saved successfully!")