import pandas as pd
import sys
sys.path.append('.')
from core.Col_Custo import extrair_preco_custo

# Simulando o dataframe bruto como seria lido pelo read_csv (tudo string)
dados = [
    ['EAN1', '14.253700'],
    ['EAN2', '14,63170000'],
    ['EAN3', '0,00'],
    ['EAN4', '0'],
    ['EAN5', 'NaN']
]

# df_estoque tem colunas 0 a 5, onde 1 é EAN e 5 é o preço
df = pd.DataFrame([[0, row[0], 0, 0, 0, row[1]] for row in dados])

print("--- DataFrame Bruto ---")
print(df)

resultado = extrair_preco_custo(df)

print("\n--- Resultado Processado (core/Col_Custo.py) ---")
print(resultado)
print("\nTipos de dados no resultado:")
print(resultado.dtypes)
