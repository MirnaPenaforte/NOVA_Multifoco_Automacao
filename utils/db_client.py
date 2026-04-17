import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

def get_engine():
    """
    Cria uma engine SQLAlchemy para o SQL Server usando as variáveis de ambiente.
    Usa o driver ODBC 17 for SQL Server.
    """
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    # Se o driver estiver apenas como 'mssql', ajustamos para o nome do driver instalado
    if driver == "mssql":
        driver = "ODBC Driver 17 for SQL Server"

    server = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "1433")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")

    # Monta a connection string do SQLAlchemy para SQL Server via pyodbc
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )

    connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"

    try:
        engine = create_engine(connection_url, pool_pre_ping=True)
        # Testa a conexão
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e}")
        raise RuntimeError("Não foi possível conectar ao banco de dados.")


def buscar_dados_views():
    """
    Consulta as views de Vendas e Estoque no banco de dados e salva os
    resultados como arquivos CSV brutos na pasta /imports.
    Retorna uma lista com os caminhos dos arquivos gerados.
    """
    view_vendas = os.getenv("VIEW_VENDAS")
    view_estoque = os.getenv("VIEW_ESTOQUE")

    if not view_vendas and not view_estoque:
        print("⚠️ Nenhuma view configurada nas variáveis de ambiente (VIEW_VENDAS, VIEW_ESTOQUE).")
        return []

    arquivos_gerados = []
    diretorio_imports = 'imports'

    # Garante que o diretório de destino exista
    os.makedirs(diretorio_imports, exist_ok=True)

    try:
        engine = get_engine()

        with engine.connect() as conn:
            # Processamento da View de Vendas
            if view_vendas:
                print(f"📥 Buscando dados da View: {view_vendas}...")
                query_vendas = text(f"SELECT * FROM {view_vendas}")
                df_vendas = pd.read_sql(query_vendas, conn)

                caminho_vendas = os.path.join(diretorio_imports, "VENDA.csv")
                # Salva sem cabeçalho e com separador ';' conforme esperado pelos scripts de importação
                df_vendas.to_csv(caminho_vendas, sep=';', index=False, header=False, encoding='latin-1')
                arquivos_gerados.append(caminho_vendas)
                print(f"✅ Dados de vendas salvos em: {caminho_vendas} ({len(df_vendas)} linhas)")

            # Processamento da View de Estoque
            if view_estoque:
                print(f"📥 Buscando dados da View: {view_estoque}...")
                query_estoque = text(f"SELECT * FROM {view_estoque}")
                df_estoque = pd.read_sql(query_estoque, conn)

                caminho_estoque = os.path.join(diretorio_imports, "ESTOQUE.csv")
                # Salva sem cabeçalho e com separador ';' conforme esperado pelos scripts de importação
                df_estoque.to_csv(caminho_estoque, sep=';', index=False, header=False, encoding='latin-1')
                arquivos_gerados.append(caminho_estoque)
                print(f"✅ Dados de estoque salvos em: {caminho_estoque} ({len(df_estoque)} linhas)")

        engine.dispose()
        return arquivos_gerados

    except Exception as e:
        print(f"❌ Erro durante a extração das Views: {e}")
        raise RuntimeError("Falha na extração de dados do banco.")


if __name__ == "__main__":
    # Carrega as variáveis para saber quais views investigar
    view_vendas = os.getenv("VIEW_VENDAS", "dbo.VW_MULTFOCO_VENDAS")
    view_estoque = os.getenv("VIEW_ESTOQUE", "dbo.VW_MULTFOCO_ESTOQUE")

    print(f"🔍 Diagnóstico: Investigando as views...")
    try:
        engine = get_engine()
        with engine.connect() as conn:
            for view in [view_vendas, view_estoque]:
                if not view: continue
                
                # Extrai apenas o nome da tabela (remove schema se houver)
                table_name = view.split('.')[-1]
                
                print(f"\n📊 Estrutura da View: {view}")
                query_cols = text(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}'
                    ORDER BY ORDINAL_POSITION
                """)
                cols = pd.read_sql(query_cols, conn)
                if cols.empty:
                    print(f"   ⚠️ Nenhuma coluna encontrada para {table_name}. Verifique o nome.")
                else:
                    for i, row in cols.iterrows():
                        print(f"   [{i}] {row['COLUMN_NAME']} ({row['DATA_TYPE']}) - Nullable: {row['IS_NULLABLE']}")

        engine.dispose()
    except Exception as e:
        print(f"❌ Erro no diagnóstico: {e}")