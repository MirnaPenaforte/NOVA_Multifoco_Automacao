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
        
        # 3. Tratamento do Preço — converter para numérico e formatar com 2 casas
        df_custo['Preço Custo'] = (
            df_custo['Preço Custo']
            .astype(str)
            .str.strip()
            .str.replace('.', '', regex=False)   # Remove separador de milhar (ou ponto decimal errado)
            .str.replace(',', '.', regex=False)  # Converte vírgula decimal para ponto
        )
        df_custo['Preço Custo'] = pd.to_numeric(df_custo['Preço Custo'], errors='coerce').fillna(0.0)
        
        # Formatar com 2 casas decimais e vírgula como separador decimal
        df_custo['Preço Custo'] = df_custo['Preço Custo'].apply(
            lambda x: f"{x:.2f}".replace('.', ',') if x > 0 else '0,00'
        )

        # 4. Pega a última ocorrência (Preço mais recente)
        df_custo = df_custo.drop_duplicates(subset=['EAN'], keep='last')

        return df_custo

    except Exception as e:
        print(f"❌ Erro ao capturar preço de custo (texto): {e}")
        return None