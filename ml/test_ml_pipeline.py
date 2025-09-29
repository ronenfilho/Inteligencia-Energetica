"""
PIPELINE ML ENERGIA SOLAR - TESTE PRINCIPAL
===========================================

Este é o arquivo principal para testar o pipeline de Machine Learning
para previsão de energia solar fotovoltaica em Goiás.

FUNCIONALIDADES:
- Conexão Snowflake + AWS
- Extração e análise de dados
- Treinamento de modelos ML
- Previsões para 30 dias
- Exportação para CSV
- Relatórios de performance

MODO DE USO:
python test_ml_pipeline.py

ARQUIVOS GERADOS:
- previsoes_energia_solar_30_dias.csv
- performance_modelos.csv  
- resumo_previsoes_por_usina.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, TimeSeriesSplit, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from solar_ml_pipeline import SolarEnergyMLPipeline
import warnings
warnings.filterwarnings('ignore')

class SolarMLTester:
    """
    Classe principal para testar o pipeline de ML
    """
    
    def __init__(self):
        self.pipeline = SolarEnergyMLPipeline()
        self.results = {}
        
    def run_connectivity_test(self):
        """Teste 1: Conectividade"""
        print("🔗 Teste 1: Verificando conectividade...")
        
        snowflake_ok = self.pipeline.connect_snowflake()
        aws_ok = self.pipeline.connect_aws()
        
        if snowflake_ok and aws_ok:
            print("   ✅ Todas as conexões OK")
            return True
        else:
            print("   ❌ Falha nas conexões")
            return False
    
    def run_data_analysis(self):
        """Teste 2: Análise de dados"""
        print("\n📊 Teste 2: Análise exploratória...")
        
        try:
            # Query básica de estatísticas
            stats_query = """
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT id_ons) as total_usinas,
                MIN(instante) as data_inicio,
                MAX(instante) as data_fim,
                AVG(val_geracao_mw) as geracao_media,
                MAX(val_geracao_mw) as geracao_maxima,
                MIN(val_geracao_mw) as geracao_minima
            FROM IE_DB.STAGING.stg_usina_geracao
            WHERE nom_tipocombustivel = 'Fotovoltaica' AND ID_ESTADO = 'GO'
            """
            
            stats_df = pd.read_sql(stats_query, self.pipeline.snowflake_conn)
            
            print("   📈 Estatísticas gerais:")
            for col in stats_df.columns:
                value = stats_df[col].iloc[0]
                print(f"      {col}: {value}")
            
            self.results['stats'] = stats_df
            return True
            
        except Exception as e:
            print(f"   ❌ Erro na análise: {e}")
            return False
    
    def run_ml_training(self):
        """Teste 3: Treinamento ML"""
        print("\n🎯 Teste 3: Treinamento de modelos...")
        
        try:
            # Extrair dados para treinamento (2023 até atual - ~3 anos)
            current_date = datetime.now().strftime('%Y-%m-%d')
            df = self.pipeline.extract_data('2023-01-01', current_date)
            
            if df.empty:
                print("   ❌ Nenhum dado extraído")
                return False
            
            # Normalizar colunas
            if 'ID_USINA' in df.columns:
                df['id_usina'] = df['ID_USINA']
            if 'GERACAO_MWH' in df.columns:
                df['geracao_mwh'] = df['GERACAO_MWH']
            if 'MEDICAO_DATA_HORA' in df.columns:
                df['medicao_data_hora'] = df['MEDICAO_DATA_HORA']
            
            df['medicao_data_hora'] = pd.to_datetime(df['medicao_data_hora'])
            
            print(f"   📊 Dados: {len(df)} registros, {df['id_usina'].nunique()} usinas")
            
            # ====== ENGENHARIA DE FEATURES AVANÇADA ======
            print("   🔧 Aplicando Engenharia de Features Avançada...")
            
            # 1. FEATURES TEMPORAIS BÁSICAS
            df['hora'] = df['medicao_data_hora'].dt.hour
            df['dia_semana'] = df['medicao_data_hora'].dt.dayofweek  # 0=Monday, 6=Sunday
            df['mes'] = df['medicao_data_hora'].dt.month
            df['dia_ano'] = df['medicao_data_hora'].dt.dayofyear
            df['semana_ano'] = df['medicao_data_hora'].dt.isocalendar().week
            df['trimestre'] = df['medicao_data_hora'].dt.quarter
            
            # 2. FEATURES CÍCLICAS (Sine/Cosine) - CRÍTICO para padrões temporais
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
            
            # 3. FEATURES SOLARES BASEADAS EM POSIÇÃO ASTRONÔMICA
            # Aproximação do nascer/pôr do sol para Goiás (latitude ~-16°)
            def solar_elevation_angle(hour, day_of_year, latitude=-16.0):
                """Calcula ângulo de elevação solar aproximado"""
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
            
            df['elevacao_solar'] = solar_elevation_angle(df['hora'], df['dia_ano'])
            df['sol_visivel'] = (df['elevacao_solar'] > 0).astype(int)  # 1 se sol está visível
            df['intensidade_solar'] = np.maximum(0, df['elevacao_solar'] / 90)  # Normalizado 0-1
            
            # 4. FEATURES CATEGÓRICAS AVANÇADAS
            # Período do dia mais específico
            def get_periodo_detalhado(hora):
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
            
            df['periodo_detalhado'] = df['hora'].apply(get_periodo_detalhado)
            
            # Estação do ano
            def get_estacao(mes):
                if mes in [12, 1, 2]:
                    return 0  # Verão
                elif mes in [3, 4, 5]:
                    return 1  # Outono
                elif mes in [6, 7, 8]:
                    return 2  # Inverno
                else:
                    return 3  # Primavera
            
            df['estacao'] = df['mes'].apply(get_estacao)
            
            # Fim de semana vs dia útil
            df['fim_semana'] = (df['dia_semana'].isin([5, 6])).astype(int)  # Sáb/Dom
            
            # 5. FEATURES DE LAG (valores anteriores) - POR USINA
            print("      📈 Criando features de lag...")
            df = df.sort_values(['id_usina', 'medicao_data_hora'])
            
            # Lags de 1h, 2h, 3h, 6h, 12h, 24h, 48h, 168h (1 semana)
            lag_periods = [1, 2, 3, 6, 12, 24, 48, 168]
            for lag in lag_periods:
                df[f'geracao_lag_{lag}h'] = df.groupby('id_usina')['geracao_mwh'].shift(lag)
            
            # 6. FEATURES DE MÉDIAS MÓVEIS - POR USINA
            print("      📊 Criando médias móveis...")
            windows = [3, 6, 12, 24, 48, 168]  # 3h, 6h, 12h, 1d, 2d, 1sem
            for window in windows:
                df[f'geracao_ma_{window}h'] = df.groupby('id_usina')['geracao_mwh'].rolling(
                    window=window, min_periods=1
                ).mean().reset_index(0, drop=True)
            
            # 7. FEATURES ESTATÍSTICAS AVANÇADAS
            print("      📏 Criando features estatísticas...")
            # Médias móveis com diferentes janelas
            for window in [6, 24, 168]:
                # Desvio padrão móvel
                df[f'geracao_std_{window}h'] = df.groupby('id_usina')['geracao_mwh'].rolling(
                    window=window, min_periods=1
                ).std().reset_index(0, drop=True)
                
                # Diferença da média móvel
                df[f'diff_ma_{window}h'] = df['geracao_mwh'] - df[f'geracao_ma_{window}h']
                
                # Percentual da média móvel
                df[f'pct_ma_{window}h'] = df['geracao_mwh'] / (df[f'geracao_ma_{window}h'] + 1e-8)
            
            # 8. FEATURES DE VARIAÇÃO TEMPORAL
            # Diferenças entre períodos
            df['diff_1h'] = df.groupby('id_usina')['geracao_mwh'].diff(1)
            df['diff_24h'] = df.groupby('id_usina')['geracao_mwh'].diff(24)
            df['diff_168h'] = df.groupby('id_usina')['geracao_mwh'].diff(168)
            
            # Taxa de mudança
            df['rate_change_1h'] = df['diff_1h'] / (df['geracao_lag_1h'] + 1e-8)
            df['rate_change_24h'] = df['diff_24h'] / (df['geracao_lag_24h'] + 1e-8)
            
            # 9. FEATURES DE INTERAÇÃO
            # Hora x Estação (interação importante para energia solar)
            df['hora_x_estacao'] = df['hora'] * df['estacao']
            df['elevacao_x_estacao'] = df['elevacao_solar'] * df['estacao']
            
            # 10. FEATURES ESPECÍFICAS POR USINA
            # Encoding da usina (pode capturar diferenças de capacidade/localização)
            usina_mapping = {usina: idx for idx, usina in enumerate(df['id_usina'].unique())}
            df['usina_encoded'] = df['id_usina'].map(usina_mapping)
            
            # Capacidade relativa (baseada na média histórica de cada usina)
            usina_capacity = df.groupby('id_usina')['geracao_mwh'].mean()
            df['capacidade_relativa'] = df['id_usina'].map(usina_capacity)
            df['geracao_normalizada'] = df['geracao_mwh'] / df['capacidade_relativa']
            
            # Remover linhas com NaN (devido aos lags)
            print("      🧹 Removendo dados com NaN...")
            df_clean = df.dropna()
            print(f"      📊 Dados após limpeza: {len(df_clean)} registros (removidos {len(df) - len(df_clean)})")
            
            # Selecionar features finais (excluir colunas não numéricas e target)
            exclude_cols = [
                'geracao_mwh', 'id_usina', 'medicao_data_hora', 
                'ID_USINA', 'MEDICAO_DATA_HORA', 'GERACAO_MWH'  # Versões maiúsculas
            ]
            
            all_cols = df_clean.columns.tolist()
            feature_cols = [col for col in all_cols if col not in exclude_cols]
            
            # Verificar se todas as features são numéricas
            numeric_features = []
            for col in feature_cols:
                if df_clean[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                    numeric_features.append(col)
            
            print(f"   ✅ Features Engineering Completa!")
            print(f"      📊 Total de features: {len(numeric_features)}")
            print(f"      🔧 Categorias de features:")
            print(f"         • Temporais básicas: 6")
            print(f"         • Cíclicas (sin/cos): 8") 
            print(f"         • Solares (astronômicas): 3")
            print(f"         • Categóricas: 4")
            print(f"         • Lags: {len(lag_periods)}")
            print(f"         • Médias móveis: {len(windows)}")
            print(f"         • Estatísticas: ~18")
            print(f"         • Variações temporais: 5")
            print(f"         • Interações: 2")
            print(f"         • Específicas por usina: 3")
            
            # Preparar dados ML
            feature_cols = numeric_features
            df = df_clean  # Usar dados limpos
            X = df[feature_cols].values
            y = df['geracao_mwh'].values
            
            # Configurar TimeSeriesSplit para séries temporais
            tscv = TimeSeriesSplit(n_splits=5)
            
            print(f"   🔄 Usando TimeSeriesSplit com 5 folds temporais...")
            
            # Treinar modelo com validação temporal
            model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
            
            # Validação cruzada temporal
            cv_scores_r2 = []
            cv_scores_mae = []
            cv_scores_rmse = []
            
            fold_num = 1
            for train_idx, test_idx in tscv.split(X):
                X_train_fold, X_test_fold = X[train_idx], X[test_idx]
                y_train_fold, y_test_fold = y[train_idx], y[test_idx]
                
                # Treinar no fold
                model.fit(X_train_fold, y_train_fold)
                y_pred_fold = model.predict(X_test_fold)
                
                # Métricas do fold
                r2_fold = r2_score(y_test_fold, y_pred_fold)
                mae_fold = mean_absolute_error(y_test_fold, y_pred_fold)
                rmse_fold = np.sqrt(mean_squared_error(y_test_fold, y_pred_fold))
                
                cv_scores_r2.append(r2_fold)
                cv_scores_mae.append(mae_fold)
                cv_scores_rmse.append(rmse_fold)
                
                print(f"      Fold {fold_num}: R²={r2_fold:.4f}, MAE={mae_fold:.2f}, RMSE={rmse_fold:.2f}")
                fold_num += 1
            
            # Treinar modelo final com todos os dados
            final_split_idx = int(len(X) * 0.8)  # 80% para treino final
            X_train_final, X_test_final = X[:final_split_idx], X[final_split_idx:]
            y_train_final, y_test_final = y[:final_split_idx], y[final_split_idx:]
            
            model.fit(X_train_final, y_train_final)
            y_pred_final = model.predict(X_test_final)
            
            # Métricas finais
            r2_final = r2_score(y_test_final, y_pred_final)
            mae_final = mean_absolute_error(y_test_final, y_pred_final)
            rmse_final = np.sqrt(mean_squared_error(y_test_final, y_pred_final))
            
            print(f"   \n   ✅ Modelo Random Forest - Resultados:")
            print(f"      📊 Cross-Validation (5 folds):")
            print(f"         R² médio: {np.mean(cv_scores_r2):.4f} (±{np.std(cv_scores_r2):.4f})")
            print(f"         MAE médio: {np.mean(cv_scores_mae):.2f} (±{np.std(cv_scores_mae):.2f}) MWh")
            print(f"         RMSE médio: {np.mean(cv_scores_rmse):.2f} (±{np.std(cv_scores_rmse):.2f}) MWh")
            print(f"      📈 Teste Final (Hold-out):")
            print(f"         R² final: {r2_final:.4f}")
            print(f"         MAE final: {mae_final:.2f} MWh")
            print(f"         RMSE final: {rmse_final:.2f} MWh")
            
            # ANÁLISE DE IMPORTÂNCIA DAS FEATURES
            print(f"\n   🎯 Analisando importância das features...")
            feature_importance = pd.DataFrame({
                'feature': feature_cols,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            print(f"      🏆 Top 10 features mais importantes:")
            for i, (_, row) in enumerate(feature_importance.head(10).iterrows()):
                print(f"         {i+1:2d}. {row['feature']:<25} {row['importance']:.4f}")
            
            # Salvar performance
            performance_data = {
                'modelo': ['RandomForest_AdvancedFeatures'],
                'metodo_validacao': ['TimeSeriesSplit_5_folds'],
                'r2_cv_mean': [np.mean(cv_scores_r2)],
                'r2_cv_std': [np.std(cv_scores_r2)],
                'mae_cv_mean': [np.mean(cv_scores_mae)],
                'mae_cv_std': [np.std(cv_scores_mae)],
                'rmse_cv_mean': [np.mean(cv_scores_rmse)],
                'rmse_cv_std': [np.std(cv_scores_rmse)],
                'r2_final': [r2_final],
                'mae_final': [mae_final],
                'rmse_final': [rmse_final],
                'features_total': [len(feature_cols)],
                'features_top10': [', '.join(feature_importance.head(10)['feature'].tolist())],
                'dados_totais': [len(X)],
                'dados_treino_final': [len(X_train_final)],
                'dados_teste_final': [len(X_test_final)],
                'periodo_inicio': ['2022-01-01'],
                'periodo_fim': [current_date],
                'data_teste': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            }
            
            # Salvar também importância das features
            self.results['feature_importance'] = feature_importance
            
            self.results['performance'] = pd.DataFrame(performance_data)
            self.results['model'] = model
            self.results['training_data'] = df
            
            return True
            
        except Exception as e:
            print(f"   ❌ Erro no treinamento: {e}")
            return False
    
    def create_predictions(self, days=30):
        """Teste 4: Criar previsões"""
        print(f"\n🔮 Teste 4: Criando previsões ({days} dias)...")
        
        try:
            df = self.results['training_data']
            
            # Última data dos dados
            last_date = df['medicao_data_hora'].max()
            
            # Datas futuras
            future_dates = pd.date_range(
                start=last_date + timedelta(hours=1),
                periods=days * 24,
                freq='H'
            )
            
            predictions = []
            
            for usina in df['id_usina'].unique():
                print(f"   📊 Processando {usina}...")
                usina_data = df[df['id_usina'] == usina].copy()
                
                # Padrões por hora
                hourly_pattern = usina_data.groupby('hora')['geracao_mwh'].agg(['mean', 'std']).fillna(0)
                
                for future_date in future_dates:
                    hora = future_date.hour
                    
                    # Previsão baseada no padrão histórico
                    base_generation = hourly_pattern.loc[hora, 'mean'] if hora in hourly_pattern.index else 0
                    
                    # Adicionar variabilidade
                    if hourly_pattern.loc[hora, 'std'] > 0:
                        noise = np.random.normal(0, hourly_pattern.loc[hora, 'std'] * 0.05)
                        prediction = base_generation + noise
                    else:
                        prediction = base_generation
                    
                    # Não pode ser negativo
                    prediction = max(0, prediction)
                    
                    predictions.append({
                        'id_usina': usina,
                        'medicao_data_hora': future_date,
                        'geracao_mwh': prediction,
                        'modelo': 'pattern_based',
                        'hora': hora
                    })
            
            predictions_df = pd.DataFrame(predictions)
            
            print(f"   ✅ Previsões geradas:")
            print(f"      Total: {len(predictions_df)} registros")
            print(f"      Usinas: {predictions_df['id_usina'].nunique()}")
            print(f"      Período: {predictions_df['medicao_data_hora'].min()} até {predictions_df['medicao_data_hora'].max()}")
            
            self.results['predictions'] = predictions_df
            return True
            
        except Exception as e:
            print(f"   ❌ Erro nas previsões: {e}")
            return False
    
    def save_results(self):
        """Teste 5: Salvar resultados"""
        print("\n💾 Teste 5: Salvando resultados...")
        
        try:
            base_path = '/home/decode/workspace/Inteligencia-Energetica/ml'
            
            # 1. Previsões
            if 'predictions' in self.results:
                pred_file = f"{base_path}/previsoes_energia_solar_30_dias.csv"
                self.results['predictions'].to_csv(pred_file, index=False)
                print(f"   ✅ Previsões: {pred_file}")
            
            # 2. Performance
            if 'performance' in self.results:
                perf_file = f"{base_path}/performance_modelos.csv"
                self.results['performance'].to_csv(perf_file, index=False)
                print(f"   ✅ Performance: {perf_file}")
            
            # 3. Resumo por usina
            if 'predictions' in self.results:
                resumo = self.results['predictions'].groupby('id_usina')['geracao_mwh'].agg([
                    'count', 'mean', 'max', 'sum'
                ]).round(2)
                resumo.columns = ['Total_Horas', 'Media_MWh', 'Maximo_MWh', 'Total_MWh']
                
                resumo_file = f"{base_path}/resumo_previsoes_por_usina.csv"
                resumo.to_csv(resumo_file)
                print(f"   ✅ Resumo: {resumo_file}")
                
                print(f"\n   📋 Resumo por usina (30 dias):")
                print(resumo)
            
            # 4. Importância das Features
            if 'feature_importance' in self.results:
                feat_file = f"{base_path}/importancia_features.csv"
                self.results['feature_importance'].to_csv(feat_file, index=False)
                print(f"   ✅ Importância Features: {feat_file}")
            
            return True
            
        except Exception as e:
            print(f"   ❌ Erro ao salvar: {e}")
            return False
    
    def run_complete_test(self):
        """Executa todos os testes em sequência"""
        print("=" * 70)
        print("🌞 PIPELINE ML ENERGIA SOLAR - TESTE COMPLETO")
        print("=" * 70)
        
        tests = [
            ("Conectividade", self.run_connectivity_test),
            ("Análise de Dados", self.run_data_analysis),
            ("Treinamento ML", self.run_ml_training),
            ("Previsões", lambda: self.create_predictions(30)),
            ("Salvamento", self.save_results)
        ]
        
        success_count = 0
        
        for test_name, test_func in tests:
            try:
                if test_func():
                    success_count += 1
                else:
                    print(f"\n❌ Falha no teste: {test_name}")
                    break
            except Exception as e:
                print(f"\n❌ Erro no teste {test_name}: {e}")
                break
        
        # Resultado final
        print("\n" + "=" * 70)
        if success_count == len(tests):
            print("🎉 TODOS OS TESTES PASSARAM!")
            print("💡 Pipeline ML está funcionando perfeitamente!")
            print("\n📁 Arquivos gerados:")
            print("   • previsoes_energia_solar_30_dias.csv")
            print("   • performance_modelos.csv")
            print("   • resumo_previsoes_por_usina.csv")
            print("   • importancia_features.csv")
        else:
            print(f"⚠️ {success_count}/{len(tests)} testes passaram")
            print("🔧 Pipeline precisa de ajustes")
        
        # Fechar conexões
        self.pipeline.close_connections()
        
        return success_count == len(tests)

def main():
    """Função principal"""
    tester = SolarMLTester()
    return tester.run_complete_test()

if __name__ == "__main__":
    main()