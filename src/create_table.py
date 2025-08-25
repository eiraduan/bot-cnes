import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# --- Carrega as variáveis de ambiente do arquivo .env ---
load_dotenv()

# --- Configurações do Banco de Dados usando variáveis de ambiente ---
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

try:
    print("Tentando estabelecer a conexão com o banco de dados...")
    conexao = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )
    print("Conexão estabelecida com sucesso!")
    
    cursor = conexao.cursor()

    # --- Criação da Tabela (com verificação para evitar erros) ---
    print("Verificando e criando a tabela 'dados_cnes' se ela não existir...")
    
    # Adiciona a verificação "IF NOT EXISTS" para evitar erros se a tabela já existir
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dados_cnes2 (
        ID SERIAL PRIMARY KEY,
        CO_CNES INTEGER,
        CO_UNIDADE TEXT,
        CO_UF INTEGER,
        CO_IBGE INTEGER,
        NU_CNPJ_MANTENEDORA TEXT,
        NO_RAZAO_SOCIAL VARCHAR(255),
        NO_FANTASIA VARCHAR(255),
        CO_NATUREZA_ORGANIZACAO INTEGER,
        DS_NATUREZA_ORGANIZACAO VARCHAR(255),
        TP_GESTAO VARCHAR(255),
        CO_NIVEL_HIERARQUIA INTEGER,
        DS_NIVEL_HIERARQUIA VARCHAR(255),
        CO_ESFERA_ADMINISTRATIVA INTEGER,
        DS_ESFERA_ADMINISTRATIVA VARCHAR(255),
        CO_ATIVIDADE INTEGER,
        TP_UNIDADE INTEGER,
        CO_CEP INTEGER,
        NO_LOGRADOURO VARCHAR(255),
        NU_ENDERECO VARCHAR(255),
        NO_BAIRRO VARCHAR(255),
        NU_TELEFONE VARCHAR(255),
        NU_LATITUDE DOUBLE PRECISION,
        NU_LONGITUDE DOUBLE PRECISION,
        CO_TURNO_ATENDIMENTO INTEGER,
        DS_TURNO_ATENDIMENTO VARCHAR(255),
        NU_CNPJ TEXT,
        NO_EMAIL VARCHAR(255),
        CO_NATUREZA_JUR INTEGER,
        ST_CENTRO_CIRURGICO NUMERIC(2, 1), 
        ST_CENTRO_OBSTETRICO NUMERIC(2, 1),
        ST_CENTRO_NEONATAL NUMERIC(2, 1),
        ST_ATEND_HOSPITALAR NUMERIC(2, 1),
        ST_SERVICO_APOIO NUMERIC(2, 1),
        ST_ATEND_AMBULATORIAL NUMERIC(2, 1),
        CO_MOTIVO_DESAB INTEGER,
        CO_AMBULATORIAL_SUS VARCHAR(255)
    );
    """
    cursor.execute(create_table_query)
    
    conexao.commit()
    print("Tabela 'dados_cnes' verificada/criada com sucesso.")

except Exception as e:
    print(f"Erro: {e}")

finally:
    if 'conexao' in locals() and conexao:
        cursor.close()
        conexao.close()
        print("Conexão com o banco de dados fechada.")