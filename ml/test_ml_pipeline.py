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
            # Extrair dados para treinamento (2022 até atual - ~3 anos)
            current_date = datetime.now().strftime('%Y-%m-%d')
            df = self.pipeline.extract_data('2022-01-01', current_date)
            
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
            
            # Features básicas para treinamento
            df['hora'] = df['medicao_data_hora'].dt.hour
            df['dia_semana'] = df['medicao_data_hora'].dt.dayofweek
            df['mes'] = df['medicao_data_hora'].dt.month
            df['dia_ano'] = df['medicao_data_hora'].dt.dayofyear
            
            # Preparar dados ML
            feature_cols = ['hora', 'dia_semana', 'mes', 'dia_ano']
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
            
            # Salvar performance
            performance_data = {
                'modelo': ['RandomForest'],
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
                'features': [len(feature_cols)],
                'dados_totais': [len(X)],
                'dados_treino_final': [len(X_train_final)],
                'dados_teste_final': [len(X_test_final)],
                'periodo_inicio': ['2022-01-01'],
                'periodo_fim': [current_date],
                'data_teste': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            }
            
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