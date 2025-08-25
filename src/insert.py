import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from dotenv import load_dotenv

load_dotenv()

# --- Configurações do Banco de Dados ---
# Substitua as informações de conexão abaixo com as suas credenciais
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_port = os.getenv("DB_PORT", 5432)

# --- Configurações do Processamento ---
pasta_arquivos = os.path.join("..", "cnes/download/extraido")
estado_filtro = 11
tabela_destino = "dados_cnes2"

# ---
print("Iniciando o processo de ETL (Extrair, Transformar, Carregar)...")

# 1. Configura a conexão com o PostgreSQL
try:
    url_object = URL.create(
        "postgresql+psycopg2",
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_name,
    )
    engine = create_engine(url_object)
    print("Conexão com o banco de dados estabelecida com sucesso!")
except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

# 2. Percorre os arquivos na pasta e processa cada um
# Alterado para buscar arquivos .csv
arquivos = [f for f in os.listdir(pasta_arquivos) if f.endswith('.csv')]

if not arquivos:
    print(f"Nenhum arquivo .csv encontrado na pasta '{pasta_arquivos}'.")
else:
    for arquivo in arquivos:
        caminho_completo = os.path.join(pasta_arquivos, arquivo)
        
        try:
            print(f"\nProcessando o arquivo: {arquivo}")
            
            # Lê o arquivo CSV completo para um DataFrame do pandas
            df = pd.read_csv(caminho_completo, sep=';', encoding='latin1', low_memory=False)
            df.columns = df.columns.str.lower()

            coluna_uf = 'co_uf'
            df_ro = df[df[coluna_uf] == estado_filtro].copy()
            
            print(df_ro)
            
            
            if not df.empty:
                print(f"  {len(df)} linhas encontradas para o filtro.")
                
                # 3. Salva os dados no banco de dados
                df_ro.to_sql(
                    name=tabela_destino,
                    con=engine,
                    if_exists='append',
                    index=False
                )
                print(f"  Dados de {arquivo} salvos na tabela '{tabela_destino}' com sucesso.")
            else:
                print(f"  Nenhuma linha encontrada para o filtro no arquivo {arquivo}.")
                
        except Exception as e:
            print(f"Erro ao processar o arquivo {arquivo}: {e}")

print("\nProcessamento finalizado.")