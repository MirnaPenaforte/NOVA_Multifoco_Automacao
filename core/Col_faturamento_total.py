import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

def calcular_faturamento_atual(df_vendas):
    """
    Soma o valor de faturamento (coluna 5, índice 5) por EAN.
    O valor já representa o faturamento da linha (ex: 71,94 = R$ 71,94).
    Gera as colunas 'Faturamento Atual' (mês corrente) e 'Faturamento M-1' (mês anterior).

    REFERÊNCIA DE MÊS: usa datetime.now() — Atual = mês real do calendário,
    M-1 = mês anterior. O arquivo VENDA_ATUAL contém dados do mês passado
    que serão capturados corretamente pelo mask_passado.
    """
    INDICE_CFOP = 0      # Coluna 1 — CFOP
    INDICE_DATA = 1      # Coluna 2 — Data (dd/mm/YYYY)
    INDICE_QTD = 4       # Coluna 5 — Qtd vendida
    INDICE_VALOR = 5     # Coluna 6 — Valor unitário
    INDICE_EAN = 6       # Coluna 7 — EAN do produto

    try:
        # 0. Filtrar apenas CFOPs válidos para faturamento (whitelist)
        CFOPS_FATURAMENTO = {'5403', '5405', '5102', '6102', '6405', '5101', '6101'}
        df_vendas[INDICE_CFOP] = df_vendas[INDICE_CFOP].astype(str).str.strip()
        df_vendas = df_vendas[df_vendas[INDICE_CFOP].isin(CFOPS_FATURAMENTO)]

        print(f"   ↳ Linhas após filtro CFOP: {len(df_vendas)}")

        # 1. Limpeza do EAN 
        df_vendas[INDICE_EAN] = df_vendas[INDICE_EAN].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        # 2. Converter valores para float
        df_vendas[INDICE_QTD] = (
            df_vendas[INDICE_QTD]
            .astype(str)
            .str.replace('.', '', regex=False)
            .str.replace(',', '.', regex=False)
        )
        df_vendas[INDICE_QTD] = pd.to_numeric(df_vendas[INDICE_QTD], errors='coerce').fillna(0.0)

        df_vendas[INDICE_VALOR] = (
            df_vendas[INDICE_VALOR]
            .astype(str)
            .str.replace('.', '', regex=False)   # remove separador de milhar
            .str.replace(',', '.', regex=False)  # vírgula decimal → ponto
        )
        df_vendas[INDICE_VALOR] = pd.to_numeric(df_vendas[INDICE_VALOR], errors='coerce').fillna(0.0)

        # Calcular o faturamento da linha: qtd * valor unitário
        df_vendas['faturamento_calculado'] = df_vendas[INDICE_QTD] * df_vendas[INDICE_VALOR]

        # 3. Converte Data
        df_vendas['data_venda'] = pd.to_datetime(df_vendas[INDICE_DATA], format='%d/%m/%Y', errors='coerce')

        # 4. Referência de mês = calendário real (datetime.now())
        #    Atual = mês corrente | M-1 = mês anterior
        hoje = datetime.now()
        mes_atual  = hoje.month
        ano_atual  = hoje.year

        ref_passado = hoje - relativedelta(months=1)
        mes_passado = ref_passado.month
        ano_passado = ref_passado.year

        print(f"   ↳ Referência: hoje={hoje.strftime('%d/%m/%Y')} → Atual={mes_atual}/{ano_atual} | M-1={mes_passado}/{ano_passado}")

        mask_atual   = (df_vendas['data_venda'].dt.month == mes_atual)   & (df_vendas['data_venda'].dt.year == ano_atual)
        mask_passado = (df_vendas['data_venda'].dt.month == mes_passado) & (df_vendas['data_venda'].dt.year == ano_passado)

        print(f"   ↳ Linhas mês atual: {mask_atual.sum()} | Linhas M-1: {mask_passado.sum()}")

        # 5. Soma do faturamento calculado por EAN
        df_atual = df_vendas[mask_atual].groupby(INDICE_EAN)['faturamento_calculado'].sum().reset_index()
        df_atual.columns = ['EAN', 'Faturamento Atual']

        df_passado = df_vendas[mask_passado].groupby(INDICE_EAN)['faturamento_calculado'].sum().reset_index()
        df_passado.columns = ['EAN', 'Faturamento M-1']

        # 6. Merge final
        df_final = pd.merge(df_atual, df_passado, on='EAN', how='outer')
        df_final['Faturamento Atual'] = df_final['Faturamento Atual'].fillna(0.0)
        df_final['Faturamento M-1']   = df_final['Faturamento M-1'].fillna(0.0)

        print(f"   ↳ EANs com faturamento: {len(df_final)} | Total Atual: R$ {df_final['Faturamento Atual'].sum():,.2f} | Total M-1: R$ {df_final['Faturamento M-1'].sum():,.2f}")

        return df_final

    except Exception as e:
        print(f"❌ Erro ao calcular faturamento agrupado: {e}")
        return None