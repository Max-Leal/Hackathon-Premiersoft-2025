# src/pipeline/load.py

import logging
import os
from sqlalchemy import create_engine
import pandas as pd
from typing import Iterator

# --- Configuração do Banco de Dados ---
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
DB_HOST = os.getenv('DB_HOST', 'db') # 'db' é o nome do serviço no docker-compose
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'aps_health_data')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# --- Funções de Carga ---

def load_full_table(df: pd.DataFrame, table_name: str):
    """Carrega um DataFrame completo em uma tabela, substituindo o conteúdo existente."""
    if df.empty:
        logging.warning(f"DataFrame '{table_name}' está vazio. Carga pulada.")
        return
    
    try:
        logging.info(f"Carregando dados na tabela '{table_name}'...")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Tabela '{table_name}' carregada com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao carregar a tabela '{table_name}': {e}")

def load_table_in_chunks(generator: Iterator[pd.DataFrame], table_name: str):
    """Carrega dados de um gerador em uma tabela, usando chunks."""
    try:
        logging.info(f"Iniciando carga em chunks para a tabela '{table_name}'...")
        
        # Limpa a tabela antes de começar a inserir os chunks
        with engine.connect() as connection:
            connection.execute(f'TRUNCATE TABLE {table_name} RESTART IDENTITY;')
            logging.info(f"Tabela '{table_name}' truncada antes da carga.")
        
        for i, chunk in enumerate(generator):
            if chunk.empty:
                continue
            logging.info(f"Carregando chunk {i+1} para a tabela '{table_name}'...")
            chunk.to_sql(table_name, engine, if_exists='append', index=False)
        
        logging.info(f"Carga em chunks para a tabela '{table_name}' concluída.")
    except Exception as e:
        logging.error(f"Erro durante a carga em chunks para '{table_name}': {e}")

# --- Função Principal de Carga ---

def run(transformed_data: dict):
    """
    Executa a etapa de carga para todos os dados transformados.
    """
    logging.info("Iniciando a etapa de carga...")
    
    # Mapeia o nome do dataframe para o nome da tabela no banco
    table_mapping = {
        'estados': 'estados',
        'municipios': 'municipios',
        'hospitais': 'hospitais',
        'medicos': 'medicos',
        'cid10': 'cid10',
        # 'pacientes' é tratado separadamente por ser um gerador
    }
    
    for df_name, table_name in table_mapping.items():
        if df_name in transformed_data:
            load_full_table(transformed_data[df_name], table_name)
            
    if 'pacientes' in transformed_data:
        # Garanta que a tabela 'pacientes' foi criada no seu init.sql!
        load_table_in_chunks(transformed_data['pacientes'], 'pacientes')
        
    logging.info("Etapa de carga concluída.")