import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

def calcular_faturamento_atual(df_vendas):
    """
    Soma o valor de faturamento (coluna 6, índice 5) por EAN.
    O valor já representa o faturamento da linha (ex: 71,94 = R$ 71,94).
    Gera as colunas 'Faturamento Atual' e 'Faturamento M-1'.
    """
    INDICE_CFOP = 0      # Coluna 1
    INDICE_DATA = 1
    INDICE_VALOR = 5     # Coluna 6 — valor do faturamento (ex: 71,94)
    INDICE_EAN = 6       # Coluna 7

    try:
        # 0. Filtrar CFOPs indesejados
        df_vendas[INDICE_CFOP] = df_vendas[INDICE_CFOP].astype(str).str.strip()
        df_vendas = df_vendas[~df_vendas[INDICE_CFOP].isin(['6202', '5202'])]

        # 1. Limpeza do EAN
        df_vendas[INDICE_EAN] = df_vendas[INDICE_EAN].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        # 2. Converter valor para float (formato BR: "71,94" → 71.94)
        df_vendas[INDICE_VALOR] = (
            df_vendas[INDICE_VALOR]
            .astype(str)
            .str.replace('.', '', regex=False)   # remove separador de milhar
            .str.replace(',', '.', regex=False)  # vírgula decimal → ponto
        )
        df_vendas[INDICE_VALOR] = pd.to_numeric(df_vendas[INDICE_VALOR], errors='coerce').fillna(0.0)

        # 3. Converte Data
        df_vendas['data_venda'] = pd.to_datetime(df_vendas[INDICE_DATA], format='%d/%m/%Y', errors='coerce')

        hoje = datetime.now()
        mes_atual = hoje.month
        ano_atual = hoje.year

        mes_passado_dt = hoje - relativedelta(months=1)
        mes_passado = mes_passado_dt.month
        ano_passado = mes_passado_dt.year

        mask_atual = (df_vendas['data_venda'].dt.month == mes_atual) & (df_vendas['data_venda'].dt.year == ano_atual)
        mask_passado = (df_vendas['data_venda'].dt.month == mes_passado) & (df_vendas['data_venda'].dt.year == ano_passado)

        # 4. Soma simples do valor por EAN (sem multiplicação)
        df_atual = df_vendas[mask_atual].groupby(INDICE_EAN)[INDICE_VALOR].sum().reset_index()
        df_atual.columns = ['EAN', 'Faturamento Atual']

        df_passado = df_vendas[mask_passado].groupby(INDICE_EAN)[INDICE_VALOR].sum().reset_index()
        df_passado.columns = ['EAN', 'Faturamento M-1']

        # 5. Merge final
        df_final = pd.merge(df_atual, df_passado, on='EAN', how='outer')
        df_final['Faturamento Atual'] = df_final['Faturamento Atual'].fillna(0.0)

        return df_final

    except Exception as e:
        print(f"❌ Erro ao calcular faturamento agrupado: {e}")
        return None 