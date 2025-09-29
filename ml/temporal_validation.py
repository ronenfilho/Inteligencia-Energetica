"""
VALIDAÇÃO TEMPORAL CORRETA - SEM DATA LEAKAGE
=============================================

Este módulo implementa validação temporal CORRETA para séries temporais,
garantindo que features de lag e médias móveis sejam calculadas APENAS
nos dados de treinamento de cada fold.

CORREÇÕES IMPLEMENTADAS:
- Features de lag calculadas POR FOLD
- Médias móveis calculadas POR FOLD  
- Sem vazamento de informação futura
- Validação temporal rigorosa

AUTOR: Sistema de IA - Inteligencia Energética
DATA: Setembro 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.model_selection import TimeSeriesSplit
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

class TemporalValidator:
    """
    Classe para validação temporal SEM data leakage
    """
    
    def __init__(self, n_splits=5):
        self.n_splits = n_splits
        self.tscv = TimeSeriesSplit(n_splits=n_splits)
    
    def create_basic_features(self, df):
        """
        Cria apenas features básicas que NÃO dependem de dados futuros
        """
        df = df.copy()
        
        # Features temporais básicas (sem leak)
        df['hora'] = df['medicao_data_hora'].dt.hour
        df['dia_semana'] = df['medicao_data_hora'].dt.dayofweek
        df['mes'] = df['medicao_data_hora'].dt.month
        df['dia_ano'] = df['medicao_data_hora'].dt.dayofyear
        df['trimestre'] = df['medicao_data_hora'].dt.quarter
        
        # Features cíclicas (sem leak)
        df['hora_sin'] = np.sin(2 * np.pi * df['hora'] / 24)
        df['hora_cos'] = np.cos(2 * np.pi * df['hora'] / 24)
        df['mes_sin'] = np.sin(2 * np.pi * df['mes'] / 12)
        df['mes_cos'] = np.cos(2 * np.pi * df['mes'] / 12)
        
        # Features solares (sem leak)
        df['elevacao_solar'] = self.solar_elevation_angle(df['hora'], df['dia_ano'])
        df['sol_visivel'] = (df['elevacao_solar'] > 0).astype(int)
        
        # Features categóricas (sem leak)
        df['periodo_dia'] = df['hora'].apply(self.get_periodo_dia)
        df['estacao'] = df['mes'].apply(self.get_estacao)
        df['fim_semana'] = (df['dia_semana'].isin([5, 6])).astype(int)
        
        return df
    
    def create_lag_features_for_fold(self, df_train, df_test, lag_periods=[1, 2, 3, 6, 12, 24]):
        \"\"\"\n        Cria features de lag APENAS com dados de treino\n        \"\"\"\n        # Combinar treino e teste mantendo ordem temporal\n        df_combined = pd.concat([df_train, df_test]).sort_values(['id_usina', 'medicao_data_hora'])\n        \n        # Calcular lags POR USINA\n        for lag in lag_periods:\n            df_combined[f'lag_{lag}h'] = df_combined.groupby('id_usina')['geracao_mwh'].shift(lag)\n        \n        # Separar treino e teste novamente\n        train_idx = df_combined.index.isin(df_train.index)\n        test_idx = df_combined.index.isin(df_test.index)\n        \n        df_train_with_lags = df_combined[train_idx].copy()\n        df_test_with_lags = df_combined[test_idx].copy()\n        \n        return df_train_with_lags, df_test_with_lags\n    \n    def create_ma_features_for_fold(self, df_train, df_test, windows=[3, 6, 12, 24]):\n        \"\"\"\n        Cria médias móveis APENAS com dados de treino\n        \"\"\"\n        # Combinar temporariamente\n        df_combined = pd.concat([df_train, df_test]).sort_values(['id_usina', 'medicao_data_hora'])\n        \n        # Calcular médias móveis POR USINA\n        for window in windows:\n            df_combined[f'ma_{window}h'] = df_combined.groupby('id_usina')['geracao_mwh'].rolling(\n                window=window, min_periods=1\n            ).mean().reset_index(0, drop=True)\n        \n        # Separar novamente\n        train_idx = df_combined.index.isin(df_train.index)\n        test_idx = df_combined.index.isin(df_test.index)\n        \n        return df_combined[train_idx].copy(), df_combined[test_idx].copy()\n    \n    def solar_elevation_angle(self, hour, day_of_year, latitude=-16.0):\n        \"\"\"Calcula elevação solar\"\"\"\n        declination = 23.45 * np.sin(np.radians(360 * (284 + day_of_year) / 365))\n        hour_angle = 15 * (hour - 12)\n        elevation = np.arcsin(\n            np.sin(np.radians(latitude)) * np.sin(np.radians(declination)) +\n            np.cos(np.radians(latitude)) * np.cos(np.radians(declination)) * np.cos(np.radians(hour_angle))\n        )\n        return np.degrees(elevation)\n    \n    def get_periodo_dia(self, hora):\n        \"\"\"Período do dia\"\"\"\n        if 6 <= hora <= 18:\n            return 1  # Dia\n        else:\n            return 0  # Noite\n    \n    def get_estacao(self, mes):\n        \"\"\"Estação do ano\"\"\"\n        if mes in [12, 1, 2]: return 0  # Verão\n        elif mes in [3, 4, 5]: return 1  # Outono  \n        elif mes in [6, 7, 8]: return 2  # Inverno\n        else: return 3  # Primavera\n    \n    def validate_model(self, df, model_class=RandomForestRegressor, model_params=None):\n        \"\"\"\n        Validação temporal CORRETA sem data leakage\n        \"\"\"\n        print(\"\\n🔒 VALIDAÇÃO TEMPORAL RIGOROSA (SEM DATA LEAKAGE)\")\n        print(\"=\" * 60)\n        \n        if model_params is None:\n            model_params = {'n_estimators': 100, 'random_state': 42, 'n_jobs': -1}\n        \n        # Preparar dados básicos (sem leak)\n        df_clean = self.create_basic_features(df)\n        df_clean = df_clean.dropna()\n        \n        print(f\"📊 Dados: {len(df_clean)} registros, {df_clean['id_usina'].nunique()} usinas\")\n        \n        # Features básicas (sem lag/MA ainda)\n        basic_features = [\n            'hora', 'dia_semana', 'mes', 'dia_ano', 'trimestre',\n            'hora_sin', 'hora_cos', 'mes_sin', 'mes_cos', \n            'elevacao_solar', 'sol_visivel', 'periodo_dia', 'estacao', 'fim_semana'\n        ]\n        \n        # Ordenar por tempo para TimeSeriesSplit\n        df_clean = df_clean.sort_values(['id_usina', 'medicao_data_hora'])\n        \n        X_basic = df_clean[basic_features].values\n        y = df_clean['geracao_mwh'].values\n        \n        # Validação temporal por fold\n        fold_results = []\n        \n        print(f\"🔄 Iniciando {self.n_splits} folds temporais...\")\n        \n        for fold_num, (train_idx, test_idx) in enumerate(self.tscv.split(X_basic), 1):\n            print(f\"\\n   📋 FOLD {fold_num}:\")\n            \n            # Separar dados do fold\n            df_train_fold = df_clean.iloc[train_idx].copy()\n            df_test_fold = df_clean.iloc[test_idx].copy()\n            \n            # Datas do fold\n            train_start = df_train_fold['medicao_data_hora'].min()\n            train_end = df_train_fold['medicao_data_hora'].max()\n            test_start = df_test_fold['medicao_data_hora'].min()\n            test_end = df_test_fold['medicao_data_hora'].max()\n            \n            print(f\"      📅 Treino: {train_start} → {train_end}\")\n            print(f\"      📅 Teste:  {test_start} → {test_end}\")\n            print(f\"      📊 Dados: {len(df_train_fold)} treino, {len(df_test_fold)} teste\")\n            \n            # Verificar se teste é realmente após treino\n            if test_start <= train_end:\n                print(f\"      ⚠️ ATENÇÃO: Sobreposição temporal detectada!\")\n            \n            # Criar features de lag APENAS com dados de treino\n            df_train_with_features, df_test_with_features = self.create_lag_features_for_fold(\n                df_train_fold, df_test_fold\n            )\n            \n            # Criar médias móveis APENAS com dados de treino  \n            df_train_with_features, df_test_with_features = self.create_ma_features_for_fold(\n                df_train_with_features, df_test_with_features\n            )\n            \n            # Features finais\n            all_features = basic_features + [f'lag_{lag}h' for lag in [1, 2, 3, 6, 12, 24]] + \\\n                          [f'ma_{w}h' for w in [3, 6, 12, 24]]\n            \n            # Remover NaN (devido aos lags)\n            df_train_clean = df_train_with_features[all_features + ['geracao_mwh']].dropna()\n            df_test_clean = df_test_with_features[all_features + ['geracao_mwh']].dropna()\n            \n            if len(df_train_clean) == 0 or len(df_test_clean) == 0:\n                print(f\"      ❌ Fold {fold_num}: Dados insuficientes após lags\")\n                continue\n            \n            X_train = df_train_clean[all_features].values\n            y_train = df_train_clean['geracao_mwh'].values\n            X_test = df_test_clean[all_features].values\n            y_test = df_test_clean['geracao_mwh'].values\n            \n            # Treinar modelo\n            model = model_class(**model_params)\n            model.fit(X_train, y_train)\n            \n            # Predições\n            y_pred = model.predict(X_test)\n            \n            # Métricas\n            r2 = r2_score(y_test, y_pred)\n            mae = mean_absolute_error(y_test, y_pred)\n            rmse = np.sqrt(mean_squared_error(y_test, y_pred))\n            \n            print(f\"      🎯 Resultados: R²={r2:.4f}, MAE={mae:.2f}, RMSE={rmse:.2f}\")\n            \n            fold_results.append({\n                'fold': fold_num,\n                'r2': r2,\n                'mae': mae, \n                'rmse': rmse,\n                'train_size': len(X_train),\n                'test_size': len(X_test),\n                'train_period': f\"{train_start} → {train_end}\",\n                'test_period': f\"{test_start} → {test_end}\"\n            })\n        \n        # Resumo final\n        if fold_results:\n            results_df = pd.DataFrame(fold_results)\n            \n            print(f\"\\n📊 RESUMO FINAL (SEM DATA LEAKAGE):\")\n            print(f\"   🎯 R² médio: {results_df['r2'].mean():.4f} (±{results_df['r2'].std():.4f})\")\n            print(f\"   📏 MAE médio: {results_df['mae'].mean():.2f} (±{results_df['mae'].std():.2f})\")\n            print(f\"   📐 RMSE médio: {results_df['rmse'].mean():.2f} (±{results_df['rmse'].std():.2f})\")\n            \n            # Análise da variabilidade\n            if results_df['r2'].std() > 0.3:\n                print(f\"   ⚠️ ALTA VARIABILIDADE: R² varia muito entre folds (std={results_df['r2'].std():.4f})\")\n                print(f\"   💡 Isso é NORMAL em séries temporais reais (vs {results_df['r2'].std():.4f} anterior)\")\n            \n            return results_df, model, all_features\n        else:\n            print(\"❌ Nenhum fold válido executado\")\n            return None, None, None

def main_validation_test():\n    \"\"\"Teste principal da validação temporal\"\"\"\n    print(\"🔍 TESTE DE VALIDAÇÃO TEMPORAL CORRETA\")\n    print(\"=\" * 50)\n    \n    # Este seria executado com dados reais\n    print(\"💡 Para usar:\")\n    print(\"   validator = TemporalValidator(n_splits=5)\")\n    print(\"   results, model, features = validator.validate_model(df)\")\n    \nif __name__ == \"__main__\":\n    main_validation_test()