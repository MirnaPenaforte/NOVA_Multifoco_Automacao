import pandas as pd

def extrair_preco_custo(df_estoque):
    """
    Extrai o preço de custo (índice 5) e formata com 2 casas decimais (ex: 14,63).
    """
    INDICE_EAN = 1
    INDICE_PRECO = 5 

    try:
        # 1. Seleção e Cópia
        df_custo = df_estoque[[INDICE_EAN, INDICE_PRECO]].copy()
        df_custo.columns = ['EAN', 'Preço Custo']
        
        # 2. Limpeza do EAN (sempre necessária)
        df_custo['EAN'] = df_custo['EAN'].astype(str).str.strip()
        
        # 3. Tratamento do Preço: transformar direto em numérico/float
        # Substitui vírgula por ponto (caso venha 14,63170000) e converte.
        df_custo['Preço Custo'] = df_custo['Preço Custo'].astype(str).str.replace(',', '.', regex=False)
        df_custo['Preço Custo'] = pd.to_numeric(df_custo['Preço Custo'], errors='coerce').fillna(0.0)
        df_custo['Preço Custo'] = df_custo['Preço Custo'].round(2)
        
        # 4. Se for <= 0, substituir por 0.001
        df_custo.loc[df_custo['Preço Custo'] <= 0, 'Preço Custo'] = 0.001

        # 5. Pega a última ocorrência (Preço mais recente)
        df_custo = df_custo.drop_duplicates(subset=['EAN'], keep='last')

        return df_custo

    except Exception as e:
        print(f"❌ Erro ao capturar preço de custo (texto): {e}")
        return None