import os
import shutil
import glob
from datetime import datetime

# Limite de cópias mantidas por arquivo-base (ex: VENDA, ESTOQUE)
MAX_BACKUPS_POR_BASE = 30


def _timestamp_str() -> str:
    """Retorna timestamp no formato dd-mm-aaaa-HH (ex: 08-05-2026-14h30)."""
    return datetime.now().strftime('%d-%m-%Y-%Hh%M')


def _salvar_backup(caminho_origem: str, dir_destino: str) -> str | None:
    """
    Copia o arquivo de origem para dir_destino com sufixo de timestamp.
    Mantém no máximo MAX_BACKUPS_POR_BASE arquivos por nome-base.
    Retorna o caminho do arquivo salvo, ou None se falhar.
    """
    try:
        os.makedirs(dir_destino, exist_ok=True)

        nome_base, ext = os.path.splitext(os.path.basename(caminho_origem))
        novo_nome = f"{nome_base}_{_timestamp_str()}{ext}"
        caminho_destino = os.path.join(dir_destino, novo_nome)

        shutil.copy2(caminho_origem, caminho_destino)
        print(f"📦 Backup salvo: {caminho_destino}")

        # Limitar número de backups: manter apenas os MAX_BACKUPS_POR_BASE mais recentes
        # por prefixo (ex: "VENDA_", "ESTOQUE_")
        prefixo = nome_base.split('_')[0]  # ex: "VENDA" ou "ESTOQUE"
        arquivos_existentes = sorted(
            glob.glob(os.path.join(dir_destino, f"{prefixo}*{ext}")),
            key=os.path.getmtime,
            reverse=True,
        )
        for arquivo_antigo in arquivos_existentes[MAX_BACKUPS_POR_BASE:]:
            os.remove(arquivo_antigo)
            print(f"🧹 Backup antigo removido: {arquivo_antigo}")

        return caminho_destino

    except Exception as e:
        print(f"❌ Erro ao salvar backup de '{caminho_origem}': {e}")
        return None


def arquivar_arquivo_raw(caminho_bruto: str, diretorio_imports: str = 'imports') -> str | None:
    """
    Salva uma cópia idêntica do arquivo bruto (como baixado) em:
        imports/backups/raw/ARQUIVO_dd-mm-ano-hora.csv

    Args:
        caminho_bruto: Caminho do arquivo original bruto.
        diretorio_imports: Pasta raiz de imports (padrão: 'imports').

    Retorna:
        Caminho do backup salvo, ou None se falhar.
    """
    dir_raw = os.path.join(diretorio_imports, 'backups', 'raw')
    return _salvar_backup(caminho_bruto, dir_raw)


def arquivar_arquivo_filtrado(caminho_filtrado: str, diretorio_imports: str = 'imports') -> str | None:
    """
    Salva uma cópia do arquivo filtrado (_ATUAL_) em:
        imports/backups/filtrado/ARQUIVO_dd-mm-ano-hora.csv

    Args:
        caminho_filtrado: Caminho do arquivo filtrado (_ATUAL_).
        diretorio_imports: Pasta raiz de imports (padrão: 'imports').

    Retorna:
        Caminho do backup salvo, ou None se falhar.
    """
    dir_filtrado = os.path.join(diretorio_imports, 'backups', 'filtrado')
    return _salvar_backup(caminho_filtrado, dir_filtrado)


def arquivar_arquivos_importacao(
    diretorio_imports: str = 'imports',
    venda_bruta_path: str = None,
    estoque_bruto_path: str = None,
    venda_filtrada_path: str = None,
    estoque_filtrado_path: str = None,
) -> dict:
    """
    Rotina principal de backup. Salva:
      - Arquivos brutos (exatamente como baixados) em imports/backups/raw/
      - Arquivos filtrados (_ATUAL_) em imports/backups/filtrado/

    Apenas uma cópia por execução, com sufixo de timestamp dd-mm-ano-hora.

    Args:
        diretorio_imports:    Pasta raiz de imports.
        venda_bruta_path:     Caminho do CSV bruto de vendas.
        estoque_bruto_path:   Caminho do CSV bruto de estoque.
        venda_filtrada_path:  Caminho do CSV filtrado de vendas (_ATUAL_).
        estoque_filtrado_path: Caminho do CSV filtrado de estoque (_ATUAL_).

    Retorna:
        dict com as chaves 'raw' e 'filtrado', cada uma com lista dos backups salvos.
    """
    resultado = {'raw': [], 'filtrado': []}

    print("\n--- Rotina de Backup dos Arquivos de Importação ---")

    # Backup dos arquivos brutos (raw)
    for caminho in filter(None, [venda_bruta_path, estoque_bruto_path]):
        backup = arquivar_arquivo_raw(caminho, diretorio_imports)
        if backup:
            resultado['raw'].append(backup)

    # Backup dos arquivos filtrados
    for caminho in filter(None, [venda_filtrada_path, estoque_filtrado_path]):
        backup = arquivar_arquivo_filtrado(caminho, diretorio_imports)
        if backup:
            resultado['filtrado'].append(backup)

    total = len(resultado['raw']) + len(resultado['filtrado'])
    print(f"✅ Backup concluído: {total} arquivo(s) arquivado(s).")
    return resultado
