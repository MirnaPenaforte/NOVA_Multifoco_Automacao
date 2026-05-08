import pandas as pd
import os
import glob
import shutil
from datetime import datetime
MESES_PT = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Marco', 4: 'Abril',
    5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
    9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
}

def limpar_backups_antigos(dir_backups, data_atual):
    """
    Remove as pastas de meses de backup que são muito antigas.
    Mantém o mês atual e os dois meses anteriores. Exclui o resto para garantir pelo menos 60 dias de histórico.
    """
    if not os.path.exists(dir_backups):
        return
        
    for pasta_mes in os.listdir(dir_backups):
        caminho_pasta = os.path.join(dir_backups, pasta_mes)
        
        if os.path.isdir(caminho_pasta):
            # Formato esperado: 03_Marco_2026
            partes = pasta_mes.split('_')
            if len(partes) >= 3:
                try:
                    mes_pasta = int(partes[0])
                    ano_pasta = int(partes[-1])
                    
                    # Convertendo Mês + Ano num índice linear de meses para comparar a diferença
                    meses_atuais_total = data_atual.year * 12 + data_atual.month
                    meses_pasta_total = ano_pasta * 12 + mes_pasta
                    
                    # Se o índice da pasta é menor do que (mês atual - 2), excluímos (garante pelo menos 60 dias de histórico)
                    if meses_atuais_total - meses_pasta_total >= 3:
                        shutil.rmtree(caminho_pasta)
                        print(f"🧹 Backup de mês antigo deletado (mantendo últimos 60-90 dias): {pasta_mes}")
                except Exception as e:
                    print(f"⚠️ Erro ao verificar idade do backup na pasta {pasta_mes}: {e}")

def storage_output(diretorio_saida):
    """
    Mantém os arquivos organizados por Mês/Dia na pasta output.
    """
    try:
        hoje = datetime.now()
        mes_str = f"{hoje.month:02d}_{MESES_PT[hoje.month]}_{hoje.year}"
        dia_str = hoje.strftime('%d-%m-%Y')
        
        dir_mes = os.path.join(diretorio_saida, mes_str)
        dir_dia = os.path.join(dir_mes, dia_str)
        os.makedirs(dir_dia, exist_ok=True)
        
        # Mover os .xlsx soltos para a pasta de backup do dia
        padrao = os.path.join(diretorio_saida, "*.xlsx")
        for arquivo in glob.glob(padrao):
            shutil.move(arquivo, os.path.join(dir_dia, os.path.basename(arquivo)))
            print(f"📦 Relatório organizado: {os.path.basename(arquivo)} em {mes_str}/{dia_str}")

        # Aproveitamos a mesma função inteligente do controlador de importações
        limpar_backups_antigos(diretorio_saida, hoje)
                
    except Exception as e:
        print(f"❌ Erro ao organizar armazenamento dos relatórios: {e}")

def gerar_relatorio_vendas(df_consolidado):
    """
    Gera o relatório com a data de hoje no formato dd-mm-aa e organiza/limpa pastas.
    """
    diretorio_saida = 'output'
    
    # Adiciona a data no formato (dd/mm/aa convertido para hífens para nomes de arquivos)
    timestamp = datetime.now().strftime('%d-%m-%y')
    nome_arquivo = f"relatorio_vendas_{timestamp}.xlsx"
    
    caminho_final = os.path.join(diretorio_saida, nome_arquivo)
    
    try:
        if not os.path.exists(diretorio_saida):
            os.makedirs(diretorio_saida)

        colunas_vazias = ['Descrição', 'Mês -3', 'Mês - 2', 'Transito', 'Pendencia']
        for col in colunas_vazias:
            df_consolidado[col] = ""

        ordem_exata = [
            'EAN', 'Descrição', 'Data Entrada', 'Data Validade', 'Mês -3', 
            'Mês - 2', 'Mês -1', 'Mês Atual', 'Estoque', 'Faturamento Atual', 
            'Faturamento M-1', 'Preço Custo', 'Transito', 'Pendencia'
        ]

        df_final = df_consolidado[ordem_exata]
        df_final.to_excel(caminho_final, index=False, engine='openpyxl')
        print(f"✅ Relatório gerado: {nome_arquivo}")

        # --- CHAMADA DA FUNÇÃO DE ARMAZENAMENTO E LIMPEZA ---
        storage_output(diretorio_saida)

        return True

    except Exception as e:
        print(f"❌ Erro ao exportar: {e}")
        return False