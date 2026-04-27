import pandas as pd

def extrair_preco_custo(df_estoque):
    """
    Calcula o preço de custo ponderado por lote para cada EAN.

    Fórmula: Σ(preço_custo_lote × qtd_lote) / qtd_total_EAN

    Colunas do df_estoque (por índice posicional):
        1 → EAN
        2 → Quantidade de estoque do lote
        3 → Lote
        5 → Preço de custo
    """
    INDICE_EAN    = 1
    INDICE_QTD    = 2
    INDICE_PRECO  = 5

    try:
        # 1. Seleção e cópia das colunas necessárias
        df = df_estoque[[INDICE_EAN, INDICE_QTD, INDICE_PRECO]].copy()
        df.columns = ['EAN', 'Qtd', 'Preço Custo']

        # 2. Limpeza do EAN
        df['EAN'] = df['EAN'].astype(str).str.strip()

        # 3. Converte quantidade para numérico
        df['Qtd'] = pd.to_numeric(df['Qtd'], errors='coerce').fillna(0.0)

        # 4. Converte preço para numérico (aceita vírgula ou ponto decimal)
        df['Preço Custo'] = (
            df['Preço Custo']
            .astype(str)
            .str.replace(',', '.', regex=False)
        )
        df['Preço Custo'] = pd.to_numeric(df['Preço Custo'], errors='coerce').fillna(0.0)

        # 5. Calcula a contribuição ponderada de cada lote: preço × qtd_lote
        df['Contribuição'] = df['Preço Custo'] * df['Qtd']

        # 6. Agrega por EAN: soma das contribuições e quantidade total
        df_agrupado = df.groupby('EAN', as_index=False).agg(
            Soma_Contribuicao=('Contribuição', 'sum'),
            Qtd_Total=('Qtd', 'sum')
        )

        # 7. Calcula preço médio ponderado; evita divisão por zero
        df_agrupado['Preço Custo'] = df_agrupado.apply(
            lambda row: row['Soma_Contribuicao'] / row['Qtd_Total']
                        if row['Qtd_Total'] > 0 else 0.0,
            axis=1
        )

        # 8. Arredonda para 2 casas decimais
        df_agrupado['Preço Custo'] = df_agrupado['Preço Custo'].round(2)

        # 9. Se for <= 0, substitui por 0.001 (requisito da API)
        df_agrupado.loc[df_agrupado['Preço Custo'] <= 0, 'Preço Custo'] = 0.001

        # 10. Retorna apenas EAN e Preço Custo
        df_custo = df_agrupado[['EAN', 'Preço Custo']].copy()

        return df_custo

    except Exception as e:
        print(f"❌ Erro ao capturar preço de custo (texto): {e}")
        return None