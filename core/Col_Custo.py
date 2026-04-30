import pandas as pd
import os
import glob



def extrair_preco_custo(df_estoque):
    """
    Calcula o preço de custo mínimo para cada EAN, considerando apenas uma linha por lote (deduplicação).

    Regras:
    1. Ignora linhas com estoque zero.
    2. Se houver o mesmo EAN com o mesmo lote, considera apenas uma linha (deduplica).
    3. Para o resultado final, seleciona o menor preço de custo encontrado entre os lotes do EAN.
    
    Colunas do df_estoque (por índice posicional):
        1 → EAN
        2 → Quantidade de estoque do lote
        3 → Lote
        5 → Preço de custo
    """
    INDICE_EAN    = 1
    INDICE_QTD    = 2
    INDICE_LOTE   = 3
    INDICE_PRECO  = 5

    try:
        # 1. Seleção e cópia das colunas necessárias
        df = df_estoque[[INDICE_EAN, INDICE_QTD, INDICE_LOTE, INDICE_PRECO]].copy()
        df.columns = ['EAN', 'Qtd', 'Lote', 'Preço Custo']

        # 2. Limpeza e Padronização
        df['EAN'] = df['EAN'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df['Lote'] = df['Lote'].astype(str).str.strip()
        
        # Converte quantidade para numérico e filtra > 0
        df['Qtd'] = pd.to_numeric(df['Qtd'], errors='coerce').fillna(0.0)
        df = df[df['Qtd'] > 0]

        # Converte preço para numérico (aceita vírgula ou ponto decimal)
        df['Preço Custo'] = (
            df['Preço Custo']
            .astype(str)
            .str.replace(',', '.', regex=False)
        )
        df['Preço Custo'] = pd.to_numeric(df['Preço Custo'], errors='coerce').fillna(0.0)

        # 3. DEDUPLICAÇÃO: Se for o mesmo EAN com o mesmo Lote, considera apenas uma linha
        # Ordenamos pelo preço para garantir que, se houver preços diferentes no mesmo lote, 
        # a lógica de pegar o "menor" comece já na deduplicação.
        df = df.sort_values(by=['EAN', 'Lote', 'Preço Custo'])
        df = df.drop_duplicates(subset=['EAN', 'Lote'], keep='first')

        # 4. Agrega por EAN: pega o menor preço de custo (min)
        df_agrupado = df.groupby('EAN', as_index=False).agg(
            Preco_Min=('Preço Custo', 'min')
        )

        # 5. Ajustes finais
        df_agrupado.columns = ['EAN', 'Preço Custo']
        
        # Arredonda para 2 casas decimais
        df_agrupado['Preço Custo'] = df_agrupado['Preço Custo'].round(2)

        # Se for <= 0, substitui por 0.001 (requisito da API)
        df_agrupado.loc[df_agrupado['Preço Custo'] <= 0, 'Preço Custo'] = 0.001

        return df_agrupado

    except Exception as e:
        print(f"❌ Erro ao capturar preço de custo: {e}")
        return None