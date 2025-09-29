"""
ENGENHARIA DE FEATURES AVANÇADA - ENERGIA SOLAR
==============================================

Este módulo contém todas as funções e classes para transformar dados brutos
de energia solar em features otimizadas para modelos de Machine Learning.

PRINCIPAIS FUNCIONALIDADES:
- Features temporais básicas e cíclicas
- Features solares baseadas em astronomia

AUTOR: Sistema de IA - Inteligencia Energética
DATA: Setembro 2025
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore')

# Carregar variáveis de ambiente
load_dotenv()

class SolarFeatureEngineering:
    """
    Classe principal para Engenharia de Features de Energia Solar
    """
    
    def __init__(self, latitude=None):
        """
        Inicializa o engenheiro de features
        
        Args:
            latitude (float): Latitude da região (default: lê do .env ou -16.0 para Goiás)
        """
        if latitude is None:
            # Tentar ler do arquivo .env
            try:
                self.latitude = float(os.getenv('LATITUDE_GOIAS', '-16.0'))
            except (ValueError, TypeError):
                self.latitude = -16.0
        else:
            self.latitude = latitude
        self.feature_categories = {
            'temporal_basic': [],
            'cyclic': [],
            'solar_astronomical': [],
            'categorical': [],
            'lag': [],
            'moving_average': [],
            'statistical': [],
            'temporal_variation': [],
            'interaction': [],
            'plant_specific': []
        }
    
    def solar_elevation_angle(self, hour, day_of_year, latitude=None):
        """
        Calcula ângulo de elevação solar aproximado
        
        Args:
            hour (pd.Series): Hora do dia (0-23)
            day_of_year (pd.Series): Dia do ano (1-365)
            latitude (float): Latitude em graus (default: self.latitude)
            
        Returns:
            pd.Series: Ângulo de elevação solar em graus
        """
        if latitude is None:
            latitude = self.latitude
            
        # Declinação solar
        declination = 23.45 * np.sin(np.radians(360 * (284 + day_of_year) / 365))
        # Ângulo horário
        hour_angle = 15 * (hour - 12)  # 15° por hora
        # Ângulo de elevação
        elevation = np.arcsin(
            np.sin(np.radians(latitude)) * np.sin(np.radians(declination)) +
            np.cos(np.radians(latitude)) * np.cos(np.radians(declination)) * np.cos(np.radians(hour_angle))
        )
        return np.degrees(elevation)
    
    def get_periodo_detalhado(self, hora):
        """
        Categoriza hora em período detalhado do dia
        
        Args:
            hora (int): Hora do dia (0-23)
            
        Returns:
            int: Código do período (0-6)
        """
        if 0 <= hora <= 5:
            return 0  # Madrugada
        elif 6 <= hora <= 8:
            return 1  # Manhã inicial
        elif 9 <= hora <= 11:
            return 2  # Manhã
        elif 12 <= hora <= 14:
            return 3  # Meio-dia
        elif 15 <= hora <= 17:
            return 4  # Tarde
        elif 18 <= hora <= 20:
            return 5  # Final tarde
        else:
            return 6  # Noite
    
    def get_estacao(self, mes):
        """
        Determina estação do ano baseada no mês (Hemisfério Sul)
        
        Args:
            mes (int): Mês (1-12)
            
        Returns:
            int: Código da estação (0-3)
        """
        if mes in [12, 1, 2]:
            return 0  # Verão
        elif mes in [3, 4, 5]:
            return 1  # Outono
        elif mes in [6, 7, 8]:
            return 2  # Inverno
        else:
            return 3  # Primavera
    
    def create_temporal_basic_features(self, df):
        """
        Cria features temporais básicas
        
        Args:
            df (pd.DataFrame): DataFrame com coluna 'medicao_data_hora'
            
        Returns:
            pd.DataFrame: DataFrame com novas features
        """
        print("      ⏰ Criando features temporais básicas...")
        
        # Features temporais básicas
        df['hora'] = df['medicao_data_hora'].dt.hour
        df['dia_semana'] = df['medicao_data_hora'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['mes'] = df['medicao_data_hora'].dt.month
        df['dia_ano'] = df['medicao_data_hora'].dt.dayofyear
        df['semana_ano'] = df['medicao_data_hora'].dt.isocalendar().week
        df['trimestre'] = df['medicao_data_hora'].dt.quarter
        
        # Registrar features criadas
        self.feature_categories['temporal_basic'].extend([
            'hora', 'dia_semana', 'mes', 'dia_ano', 'semana_ano', 'trimestre'
        ])
        
        return df
    
    def create_cyclic_features(self, df):
        """
        Cria features cíclicas usando sine/cosine
        
        Args:
            df (pd.DataFrame): DataFrame com features temporais
            
        Returns:
            pd.DataFrame: DataFrame com features cíclicas
        """
        print("      🔄 Criando features cíclicas (sin/cos)...")
        
        # Hora do dia (0-23) -> ciclo 24h
        df['hora_sin'] = np.sin(2 * np.pi * df['hora'] / 24)
        df['hora_cos'] = np.cos(2 * np.pi * df['hora'] / 24)
        
        # Dia da semana (0-6) -> ciclo semanal
        df['dia_semana_sin'] = np.sin(2 * np.pi * df['dia_semana'] / 7)
        df['dia_semana_cos'] = np.cos(2 * np.pi * df['dia_semana'] / 7)
        
        # Mês (1-12) -> ciclo anual
        df['mes_sin'] = np.sin(2 * np.pi * df['mes'] / 12)
        df['mes_cos'] = np.cos(2 * np.pi * df['mes'] / 12)
        
        # Dia do ano (1-365) -> ciclo anual
        df['dia_ano_sin'] = np.sin(2 * np.pi * df['dia_ano'] / 365)
        df['dia_ano_cos'] = np.cos(2 * np.pi * df['dia_ano'] / 365)
        
        # Registrar features criadas
        self.feature_categories['cyclic'].extend([
            'hora_sin', 'hora_cos', 'dia_semana_sin', 'dia_semana_cos',
            'mes_sin', 'mes_cos', 'dia_ano_sin', 'dia_ano_cos'
        ])
        
        return df
    
    def create_solar_astronomical_features(self, df):
        """
        Cria features baseadas em astronomia solar
        
        Args:
            df (pd.DataFrame): DataFrame com features temporais
            
        Returns:
            pd.DataFrame: DataFrame com features solares
        """
        print("      ☀️ Criando features astronômicas solares...")
        
        # Ângulo de elevação solar
        df['elevacao_solar'] = self.solar_elevation_angle(df['hora'], df['dia_ano'])
        df['sol_visivel'] = (df['elevacao_solar'] > 0).astype(int)  # 1 se sol está visível
        df['intensidade_solar'] = np.maximum(0, df['elevacao_solar'] / 90)  # Normalizado 0-1
        
        # Registrar features criadas
        self.feature_categories['solar_astronomical'].extend([
            'elevacao_solar', 'sol_visivel', 'intensidade_solar'
        ])
        
        return df
    
    def create_categorical_features(self, df):
        """
        Cria features categóricas avançadas
        
        Args:
            df (pd.DataFrame): DataFrame com features temporais
            
        Returns:
            pd.DataFrame: DataFrame com features categóricas
        """
        print("      📂 Criando features categóricas...")
        
        # Período do dia mais específico
        df['periodo_detalhado'] = df['hora'].apply(self.get_periodo_detalhado)
        
        # Estação do ano
        df['estacao'] = df['mes'].apply(self.get_estacao)
        
        # Fim de semana vs dia útil
        df['fim_semana'] = (df['dia_semana'].isin([5, 6])).astype(int)  # Sáb/Dom
        
        # Registrar features criadas
        self.feature_categories['categorical'].extend([
            'periodo_detalhado', 'estacao', 'fim_semana'
        ])
        
        return df
    
    def create_lag_features(self, df, lag_periods=[1, 2, 3, 6, 12, 24, 48, 168]):
        """
        Cria features de lag (valores anteriores) por usina
        
        Args:
            df (pd.DataFrame): DataFrame ordenado por usina e tempo
            lag_periods (list): Lista de períodos de lag em horas
            
        Returns:
            pd.DataFrame: DataFrame com features de lag
        """
        print("      📈 Criando features de lag...")
        
        # Garantir ordenação
        df = df.sort_values(['id_usina', 'medicao_data_hora'])
        
        # Criar lags para cada período
        for lag in lag_periods:
            df[f'geracao_lag_{lag}h'] = df.groupby('id_usina')['geracao_mwh'].shift(lag)
        
        # Registrar features criadas
        lag_features = [f'geracao_lag_{lag}h' for lag in lag_periods]
        self.feature_categories['lag'].extend(lag_features)
        
        return df
    
    def create_moving_average_features(self, df, windows=[3, 6, 12, 24, 48, 168]):
        """
        Cria features de médias móveis por usina
        
        Args:
            df (pd.DataFrame): DataFrame ordenado por usina e tempo
            windows (list): Lista de janelas em horas
            
        Returns:
            pd.DataFrame: DataFrame com médias móveis
        """
        print("      📊 Criando médias móveis...")
        
        # Criar médias móveis para cada janela
        for window in windows:
            df[f'geracao_ma_{window}h'] = df.groupby('id_usina')['geracao_mwh'].rolling(
                window=window, min_periods=1
            ).mean().reset_index(0, drop=True)
        
        # Registrar features criadas
        ma_features = [f'geracao_ma_{window}h' for window in windows]
        self.feature_categories['moving_average'].extend(ma_features)
        
        return df
    
    def create_statistical_features(self, df, windows=[6, 24, 168]):
        """
        Cria features estatísticas avançadas
        
        Args:
            df (pd.DataFrame): DataFrame com médias móveis
            windows (list): Lista de janelas para estatísticas
            
        Returns:
            pd.DataFrame: DataFrame com features estatísticas
        """
        print("      📏 Criando features estatísticas...")
        
        stat_features = []
        
        for window in windows:
            # Desvio padrão móvel
            col_std = f'geracao_std_{window}h'
            df[col_std] = df.groupby('id_usina')['geracao_mwh'].rolling(
                window=window, min_periods=1
            ).std().reset_index(0, drop=True)
            stat_features.append(col_std)
            
            # Diferença da média móvel
            col_diff = f'diff_ma_{window}h'
            df[col_diff] = df['geracao_mwh'] - df[f'geracao_ma_{window}h']
            stat_features.append(col_diff)
            
            # Percentual da média móvel
            col_pct = f'pct_ma_{window}h'
            df[col_pct] = df['geracao_mwh'] / (df[f'geracao_ma_{window}h'] + 1e-8)
            stat_features.append(col_pct)
        
        # Registrar features criadas
        self.feature_categories['statistical'].extend(stat_features)
        
        return df
    
    def create_temporal_variation_features(self, df):
        """
        Cria features de variação temporal
        
        Args:
            df (pd.DataFrame): DataFrame com features de lag
            
        Returns:
            pd.DataFrame: DataFrame com variações temporais
        """
        print("      📈 Criando features de variação temporal...")
        
        # Diferenças entre períodos
        df['diff_1h'] = df.groupby('id_usina')['geracao_mwh'].diff(1)
        df['diff_24h'] = df.groupby('id_usina')['geracao_mwh'].diff(24)
        df['diff_168h'] = df.groupby('id_usina')['geracao_mwh'].diff(168)
        
        # Taxa de mudança
        df['rate_change_1h'] = df['diff_1h'] / (df['geracao_lag_1h'] + 1e-8)
        df['rate_change_24h'] = df['diff_24h'] / (df['geracao_lag_24h'] + 1e-8)
        
        # Registrar features criadas
        self.feature_categories['temporal_variation'].extend([
            'diff_1h', 'diff_24h', 'diff_168h', 'rate_change_1h', 'rate_change_24h'
        ])
        
        return df
    
    def create_interaction_features(self, df):
        """
        Cria features de interação
        
        Args:
            df (pd.DataFrame): DataFrame com features básicas
            
        Returns:
            pd.DataFrame: DataFrame com interações
        """
        print("      🔗 Criando features de interação...")
        
        # Hora x Estação (interação importante para energia solar)
        df['hora_x_estacao'] = df['hora'] * df['estacao']
        df['elevacao_x_estacao'] = df['elevacao_solar'] * df['estacao']
        
        # Registrar features criadas
        self.feature_categories['interaction'].extend([
            'hora_x_estacao', 'elevacao_x_estacao'
        ])
        
        return df
    
    def create_plant_specific_features(self, df):
        """
        Cria features específicas por usina
        
        Args:
            df (pd.DataFrame): DataFrame com dados por usina
            
        Returns:
            pd.DataFrame: DataFrame com features específicas
        """
        print("      🏭 Criando features específicas por usina...")
        
        # Encoding da usina
        usina_mapping = {usina: idx for idx, usina in enumerate(df['id_usina'].unique())}
        df['usina_encoded'] = df['id_usina'].map(usina_mapping)
        
        # Capacidade relativa (baseada na média histórica de cada usina)
        usina_capacity = df.groupby('id_usina')['geracao_mwh'].mean()
        df['capacidade_relativa'] = df['id_usina'].map(usina_capacity)
        df['geracao_normalizada'] = df['geracao_mwh'] / df['capacidade_relativa']
        
        # Registrar features criadas
        self.feature_categories['plant_specific'].extend([
            'usina_encoded', 'capacidade_relativa', 'geracao_normalizada'
        ])
        
        return df
    
    def apply_all_features(self, df, verbose=True):
        """
        Aplica todas as transformações de features
        
        Args:
            df (pd.DataFrame): DataFrame com dados brutos
            verbose (bool): Imprimir progresso
            
        Returns:
            tuple: (df_transformed, feature_list, feature_categories)
        """
        if verbose:
            print("   🔧 Aplicando Engenharia de Features Avançada...")
        
        # 1. Features temporais básicas
        df = self.create_temporal_basic_features(df)
        
        # 2. Features cíclicas
        df = self.create_cyclic_features(df)
        
        # 3. Features solares astronômicas
        df = self.create_solar_astronomical_features(df)
        
        # 4. Features categóricas
        df = self.create_categorical_features(df)

        # Selecionar apenas features básicas criadas
        basic_feature_names = (
            self.feature_categories['temporal_basic'] + 
            self.feature_categories['cyclic'] + 
            self.feature_categories['solar_astronomical'] + 
            self.feature_categories['categorical']
        )
        
        # Manter colunas essenciais + features básicas
        essential_cols = ['geracao_mwh', 'id_usina', 'medicao_data_hora']
        keep_cols = essential_cols + basic_feature_names
        available_cols = [col for col in keep_cols if col in df.columns]
        
        df_basic = df[available_cols].copy()
        df_clean = df_basic.dropna()
        
        if verbose:
            print(f"   ✅ Features Básicas (SEM DATA LEAKAGE):")
            print(f"      📊 Total de features: {len(basic_feature_names)}")
            print(f"      � Dados: {len(df_clean)} registros")
            print(f"      🔧 Categorias:")
            print(f"         • Temporais básicas: {len(self.feature_categories['temporal_basic'])}")
            print(f"         • Cíclicas: {len(self.feature_categories['cyclic'])}")
            print(f"         • Solares: {len(self.feature_categories['solar_astronomical'])}")
            print(f"         • Categóricas: {len(self.feature_categories['categorical'])}")
            print(f"      ⚠️ R² esperado: 30-50% (realista para energia solar)")
        
        return df_clean, basic_feature_names, self.feature_categories
      
    def get_feature_summary(self):
        """
        Retorna resumo das features criadas
        
        Returns:
            dict: Dicionário com informações das features
        """
        total_features = sum(len(features) for features in self.feature_categories.values())
        
        return {
            'total_features': total_features,
            'categories': self.feature_categories,
            'category_counts': {
                category: len(features) 
                for category, features in self.feature_categories.items()
            }
        }

if __name__ == "__main__":
    print("📊 Módulo de Engenharia de Features para Energia Solar")
    print("🔧 Use a classe SolarFeatureEngineering ou a função apply_solar_feature_engineering")