# src/pipeline/load.py

import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Iterator, Dict
import os
import json

# --- Configuração do banco ---

def get_database_engine():
    """Cria e retorna a engine de conexão com PostgreSQL."""
    DB_USER = os.getenv('DB_USER', 'admin')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
    DB_HOST = os.getenv('DB_HOST', 'db')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'aps_health_data')
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    logging.info(f"Tentando conectar em: {DB_HOST}:{DB_PORT} como {DB_USER}")
    return create_engine(DATABASE_URL)

# --- Funções de Conversão e Carga ---

def convert_to_postgresql_array(value):
    if value is None or (isinstance(value, list) and not value): return None
    if isinstance(value, list): return value
    return [str(value)]

def convert_array_columns(df: pd.DataFrame, array_columns: list) -> pd.DataFrame:
    df_copy = df.copy()
    for col in array_columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].apply(convert_to_postgresql_array)
    return df_copy

def clear_table(engine, table_name: str) -> None:
    try:
        logging.info(f"Limpando tabela '{table_name}'...")
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
    except Exception as e:
        logging.warning(f"Erro ao limpar tabela '{table_name}': {e}")

def load_dataframe_to_table(engine, df: pd.DataFrame, table_name: str, array_columns: list = None) -> None:
    if df.empty:
        logging.warning(f"DataFrame '{table_name}' está vazio. Carga pulada.")
        return
    try:
        logging.info(f"Iniciando carga da tabela '{table_name}' com {len(df)} registros...")
        if array_columns:
            df = convert_array_columns(df, array_columns)
        
        df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        logging.info(f"Tabela '{table_name}' carregada com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao carregar a tabela '{table_name}': {e}")
        raise # Relança a exceção para parar o pipeline se uma carga crítica falhar

def load_pacientes_with_dynamic_cids(engine, data_generator: Iterator[pd.DataFrame], dataframes: Dict) -> None:
    """
    Função especializada para carregar pacientes, garantindo que os CIDs dinamicamente
    criados sejam inseridos no banco antes dos pacientes que os utilizam.
    """
    chunk_num = 0
    # Mantém um registro dos CIDs que já sabemos que estão no banco
    cids_in_db = set(pd.read_sql("SELECT codigo FROM cid10", engine)['codigo'])
    
    try:
        for chunk in data_generator:
            if chunk.empty: continue
                
            chunk_num += 1
            logging.info(f"Processando chunk {chunk_num} de pacientes para carga ({len(chunk)} registros)...")
            
            # 1. Identificar CIDs únicos no chunk atual
            cids_in_chunk = set(chunk['cid_10'].dropna().unique())
            
            # 2. Determinar quais CIDs são novos (não estão no nosso registro em memória)
            new_cids_to_load = cids_in_chunk - cids_in_db
            
            # 3. Se houver CIDs novos, carregá-los primeiro
            if new_cids_to_load:
                logging.info(f"Novos CIDs detectados no chunk: {new_cids_to_load}. Carregando-os na tabela 'cid10'...")
                
                # Pega os dados completos dos novos CIDs do DataFrame principal
                df_cid10_master = dataframes['cid10']
                new_cids_df = df_cid10_master[df_cid10_master['codigo'].isin(new_cids_to_load)]
                
                # Carrega os novos CIDs no banco
                new_cids_df.to_sql('cid10', engine, if_exists='append', index=False, method='multi')
                
                # Atualiza nosso registro em memória para evitar recargas
                cids_in_db.update(new_cids_to_load)
                logging.info(f"Novos CIDs carregados com sucesso.")
            
            # 4. Agora, carregar o chunk de pacientes
            logging.info(f"Carregando chunk {chunk_num} de pacientes na tabela 'pacientes'...")
            chunk.to_sql('pacientes', engine, if_exists='append', index=False, method='multi')
            
        logging.info(f"Carga em streaming concluída para 'pacientes' ({chunk_num} chunks)")
        
    except Exception as e:
        logging.error(f"Erro durante a carga em chunks para 'pacientes': {e}")
        raise

# --- Função principal ---

def run(dataframes: Dict[str, pd.DataFrame | Iterator]):
    logging.info("Iniciando a etapa de carga...")
    engine = get_database_engine()
    
    # Ordem de carga estática
    load_order = ['estados', 'municipios', 'cid10', 'hospitais', 'medicos']
    
    for table_name in load_order:
        data = dataframes.get(table_name)
        if data is None: continue
        
        clear_table(engine, table_name)
        
        if isinstance(data, pd.DataFrame):
            array_cols = ['especialidades'] if table_name == 'hospitais' else []
            load_dataframe_to_table(engine, data, table_name, array_cols)
    
    # Carga especial para pacientes
    clear_table(engine, 'pacientes')
    pacientes_generator = dataframes.get('pacientes')
    if pacientes_generator:
        load_pacientes_with_dynamic_cids(engine, pacientes_generator)
    
    engine.dispose()
    logging.info("Etapa de carga concluída.")
# Função para obter a especialidade (necessária para autonomia do script de carga)
def get_especialidade_from_cid(codigo: str) -> str:
    if not isinstance(codigo, str) or not codigo: return "Clínica Geral"
    letra = codigo[0]
    CID_CAPITULO_ESPECIALIDADE_MAP = {
        'A': 'Infectologia', 'B': 'Infectologia', 'C': 'Oncologia',
        'D00-D48': 'Oncologia', 'D50-D89': 'Hematologia', 'E': 'Endocrinologia',
        'F': 'Psiquiatria', 'G': 'Neurologia', 'H00-H59': 'Oftalmologia',
        'H60-H95': 'Otorrinolaringologia', 'I': 'Cardiologia', 'J': 'Pneumologia',
        'K': 'Gastroenterologia', 'L': 'Dermatologia', 'M': 'Ortopedia',
        'N': 'Nefrologia', 'O': 'Ginecologia', 'P': 'Pediatria', 'Q': 'Genética Médica'
    }
    if letra == 'D' and len(codigo) > 2:
        try:
            num = int(codigo[1:3])
            return 'Hematologia' if 50 <= num <= 89 else 'Oncologia'
        except ValueError: pass
    if letra == 'H' and len(codigo) > 2:
        try:
            num = int(codigo[1:3])
            return 'Oftalmologia' if 0 <= num <= 59 else 'Otorrinolaringologia'
        except ValueError: pass
    return CID_CAPITULO_ESPECIALIDADE_MAP.get(letra, "Clínica Geral")
# Em: src/pipeline/load.py

def load_pacientes_with_dynamic_cids(engine, data_generator: Iterator[pd.DataFrame]):
    """
    Carrega pacientes em chunks, criando dinamicamente CIDs faltantes no banco
    antes de carregar os pacientes que os utilizam.
    """
    cids_in_db = set(pd.read_sql("SELECT codigo FROM cid10", engine)['codigo'])
    chunk_num = 0
    try:
        for chunk in data_generator:
            if chunk.empty: continue
            chunk_num += 1
            
            # Identifica CIDs no chunk que ainda não estão no banco
            cids_in_chunk = set(chunk['cid_10'].dropna().unique())
            new_cids_to_create = cids_in_chunk - cids_in_db
            
            # Se encontrar CIDs novos, cria e os carrega no banco primeiro
            if new_cids_to_create:
                logging.warning(f"Novos CIDs detectados: {new_cids_to_create}. Criando-os no banco de dados.")
                new_cid_records = [
                    {
                        'codigo': code,
                        'descricao': f'CID (código {code}) - Criado Automaticamente',
                        'especialidade': get_especialidade_from_cid(code)
                    } for code in new_cids_to_create
                ]
                new_cids_df = pd.DataFrame(new_cid_records)
                new_cids_df.to_sql('cid10', engine, if_exists='append', index=False, method='multi')
                cids_in_db.update(new_cids_to_create) # Atualiza o controle
            
            # Agora, carrega o chunk de pacientes com segurança
            logging.info(f"Carregando chunk {chunk_num} de pacientes ({len(chunk)} registros)...")
            chunk.to_sql('pacientes', engine, if_exists='append', index=False, method='multi')
            
        logging.info(f"Carga em streaming para 'pacientes' concluída.")
    except Exception as e:
        logging.error(f"Erro na carga em chunks para 'pacientes': {e}")
        raise