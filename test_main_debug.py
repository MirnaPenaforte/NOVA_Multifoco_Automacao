import pandas as pd
import sys
sys.path.append('.')
from core.read_Csv import ler_csv_sem_header
from core.Col_estoque import processar_estoque_agrupado
from core.Col_data_validade import processar_validade_estoque
from core.Col_Custo import extrair_preco_custo

estoque_path = "imports/ESTOQUE_23-04-2026.csv"
df_estoque_bruto = ler_csv_sem_header(estoque_path)

if df_estoque_bruto is not None:
    estoque_final = processar_estoque_agrupado(df_estoque_bruto.copy())
    custo_final = extrair_preco_custo(df_estoque_bruto.copy())
    
    print("--- Custo Final Head ---")
    print(custo_final.head())
    
    df_final = pd.merge(estoque_final, custo_final, on='EAN', how='outer')
    
    print("--- DataFrame Final Head (Preço Custo) ---")
    print(df_final[['EAN', 'Preço Custo']].head(10))
