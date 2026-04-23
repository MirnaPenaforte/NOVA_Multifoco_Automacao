import pandas as pd

def processar_estoque_agrupado(df_estoque):
    """
    Agrupa EANs e soma o estoque usando índices configuráveis.
    """
    # --- CONFIGURAÇÃO (Baseado na estrutura da View VW_MULTFOCO_ESTOQUE) ---
    # [0]=Filial_Cnpj, [1]=Codigo_Barras, [2]=Est_Disponivel, [3]=Lote, [4]=Data_Vencimento, [5]=Preco_custo
    INDICE_EAN = 1     
    INDICE_ESTOQUE = 2 

    try:
        #Limpar espaços e garantir que o EAN seja string
        df_estoque[INDICE_EAN] = df_estoque[INDICE_EAN].astype(str).str.strip()

        # 1. Garantir que os dados de estoque sejam numéricos
        df_estoque[INDICE_ESTOQUE] = pd.to_numeric(df_estoque[INDICE_ESTOQUE], errors='coerce').fillna(0)

        # Filtrar para manter apenas as linhas onde o estoque é maior que zero
        df_estoque = df_estoque[df_estoque[INDICE_ESTOQUE] > 0]

        # 2. Agrupamento e Soma
        # Agrupamos pelo índice do EAN e somamos a coluna de estoque
        df_agrupado = df_estoque.groupby(INDICE_EAN)[INDICE_ESTOQUE].sum().reset_index()

        # 3. Renomear para a saída final
        df_agrupado.columns = ['EAN', 'Estoque']

        return df_agrupado

    except Exception as e:
        print(f"❌ Erro na manipulação de estoque: {e}")
        return None