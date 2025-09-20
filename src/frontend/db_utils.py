import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import logging
import toml # Importe a nova biblioteca
import os   # Importe a biblioteca 'os' para manipulação de caminhos

logging.basicConfig(level=logging.INFO)

@st.cache_resource(ttl="10m")
def get_connection():
    """
    Cria e retorna uma conexão com o banco de dados PostgreSQL.
    Lê as credenciais de um arquivo secrets.toml localizado na mesma pasta.
    """
    try:
        logging.info("Criando nova conexão com o banco de dados...")

        # --- LÓGICA MODIFICADA PARA ENCONTRAR O ARQUIVO ---
        # Pega o caminho absoluto do diretório onde este arquivo (db_utils.py) está
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Constrói o caminho para o arquivo secrets.toml
        secrets_path = os.path.join(current_dir, ".streamlit", "secrets.toml")
        
        # Lê e analisa (parse) o arquivo .toml
        secrets = toml.load(secrets_path)
        creds = secrets["connections"]["postgresql"]
        # --- FIM DA LÓGICA MODIFICADA ---
        
        # O resto do código permanece o mesmo
        db_url = (
            f"{creds['dialect']}+{creds['driver']}://"
            f"{creds['username']}:{creds['password']}@"
            f"{creds['host']}:{creds['port']}/{creds['database']}"
        )
        
        engine = create_engine(db_url)
        logging.info("Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except FileNotFoundError:
        error_msg = f"Arquivo de credenciais não encontrado em: {secrets_path}"
        logging.error(error_msg)
        st.error(error_msg)
        return None
    except Exception as e:
        logging.error(f"Não foi possível conectar ao banco de dados: {e}")
        st.error(f"Erro ao conectar ao banco de dados. Verifique o arquivo secrets.toml.")
        return None

def fetch_data(query: str) -> pd.DataFrame:
    """
    Executa uma consulta SQL e retorna um DataFrame.
    """
    engine = get_connection()
    if engine:
        try:
            logging.info(f"Executando a consulta: {query[:100]}...")
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)
            logging.info("Consulta executada com sucesso.")
            return df
        except Exception as e:
            logging.error(f"Erro ao executar a consulta: {e}")
            st.error(f"Erro ao buscar dados: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()