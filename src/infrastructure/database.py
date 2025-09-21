# src/infrastructure/database.py
import logging
import os
import pandas as pd
from sqlalchemy import create_engine, text, ARRAY, TEXT

def get_database_engine():
    DB_USER = os.getenv('DB_USER', 'admin')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
    DB_HOST = os.getenv('DB_HOST', 'db')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'aps_health_data')
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(DATABASE_URL)

def clear_table(engine, table_name: str):
    try:
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
        logging.info(f"Tabela '{table_name}' limpa com sucesso.")
    except Exception as e:
        logging.warning(f"Erro ao limpar tabela '{table_name}': {e}")

def load_dataframe_to_table(engine, df: pd.DataFrame, table_name: str, array_columns: list = None):
    """
    Carrega um DataFrame para uma tabela, com tratamento especial para colunas de array do PostgreSQL.
    """
    if df.empty:
        return

    try:
        dtype_map = {}
        if array_columns:
            for col in array_columns:
                if col in df.columns:
                    dtype_map[col] = ARRAY(TEXT)

        # --- MUDANÇA PRINCIPAL AQUI ---
        # Remova 'method' ou deixe-o como None para usar o método padrão do SQLAlchemy,
        # que é mais lento, mas lida corretamente com tipos de dados complexos como arrays.
        df.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            dtype=dtype_map if dtype_map else None
        )
        
        logging.info(f"Tabela '{table_name}' carregada com {len(df)} registros.")
    except Exception as e:
        logging.error(f"Erro ao carregar a tabela '{table_name}': {e}")
        logging.error(f"Amostra dos dados que falharam:\n{df.head().to_string()}")
        raise