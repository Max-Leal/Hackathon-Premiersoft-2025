# src/pipeline/load.py

import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Iterator, Dict
import os
import math # <-- IMPORTAÇÃO NECESSÁRIA ADICIONADA AQUI

# --- Funções de Configuração e Auxiliares ---

def get_database_engine():
    DB_USER = os.getenv('DB_USER', 'admin'); DB_PASSWORD = os.getenv('DB_PASSWORD', 'password123')
    DB_HOST = os.getenv('DB_HOST', 'db'); DB_PORT = os.getenv('DB_PORT', '5432'); DB_NAME = os.getenv('DB_NAME', 'aps_health_data')
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(DATABASE_URL)

def clear_table(engine, table_name: str):
    try:
        with engine.begin() as conn: conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
        logging.info(f"Tabela '{table_name}' limpa com sucesso.")
    except Exception as e: logging.warning(f"Erro ao limpar tabela '{table_name}': {e}")

def load_dataframe_to_table(engine, df: pd.DataFrame, table_name: str, array_columns: list = None):
    if df.empty: return
    try:
        if array_columns:
            df_copy = df.copy()
            for col in array_columns:
                if col in df_copy.columns: df_copy[col] = df_copy[col].apply(lambda x: x if isinstance(x, list) else [])
            df = df_copy
        df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        logging.info(f"Tabela '{table_name}' carregada com {len(df)} registros.")
    except Exception as e: logging.error(f"Erro ao carregar a tabela '{table_name}': {e}"); raise

def get_especialidade_from_cid(codigo: str) -> str:
    if not isinstance(codigo, str) or not codigo: return "Clínica Geral"
    letra = codigo[0]
    CID_CAPITULO_ESPECIALIDADE_MAP = {
        'A': 'Infectologia', 'B': 'Infectologia', 'C': 'Oncologia', 'D00-D48': 'Oncologia', 
        'D50-D89': 'Hematologia', 'E': 'Endocrinologia', 'F': 'Psiquiatria', 'G': 'Neurologia', 
        'H00-H59': 'Oftalmologia', 'H60-H95': 'Otorrinolaringologia', 'I': 'Cardiologia', 
        'J': 'Pneumologia', 'K': 'Gastroenterologia', 'L': 'Dermatologia', 'M': 'Ortopedia', 
        'N': 'Nefrologia', 'O': 'Ginecologia', 'P': 'Pediatria', 'Q': 'Genética Médica'
    }
    if letra == 'D' and len(codigo) > 2:
        try: num = int(codigo[1:3]); return 'Hematologia' if 50 <= num <= 89 else 'Oncologia'
        except ValueError: pass
    if letra == 'H' and len(codigo) > 2:
        try: num = int(codigo[1:3]); return 'Oftalmologia' if 0 <= num <= 59 else 'Otorrinolaringologia'
        except ValueError: pass
    return CID_CAPITULO_ESPECIALIDADE_MAP.get(letra, "Clínica Geral")

# --- FUNÇÃO HAVERSINE ADICIONADA AQUI ---
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371; dLat = math.radians(lat2 - lat1); dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# --- Funções de Carga Especializadas ---

def load_pacientes_with_dynamic_cids(engine, data_generator: Iterator[pd.DataFrame]):
    cids_in_db = set(pd.read_sql("SELECT codigo FROM cid10", engine)['codigo'])
    chunk_num = 0
    try:
        for chunk in data_generator:
            if chunk.empty: continue
            chunk_num += 1
            cids_in_chunk = set(chunk['cid_10'].dropna().unique())
            new_cids_to_create = cids_in_chunk - cids_in_db
            if new_cids_to_create:
                logging.warning(f"Novos CIDs detectados: {new_cids_to_create}. Criando-os no banco de dados.")
                new_cid_records = [{'codigo': code, 'descricao': f'CID (código {code}) - Criado Automaticamente', 'especialidade': get_especialidade_from_cid(code)} for code in new_cids_to_create]
                new_cids_df = pd.DataFrame(new_cid_records)
                new_cids_df.to_sql('cid10', engine, if_exists='append', index=False, method='multi')
                cids_in_db.update(new_cids_to_create)
            logging.info(f"Carregando chunk {chunk_num} de pacientes ({len(chunk)} registros)...")
            chunk.to_sql('pacientes', engine, if_exists='append', index=False, method='multi')
        logging.info("Carga em streaming para 'pacientes' concluída.")
    except Exception as e: logging.error(f"Erro na carga em chunks para 'pacientes': {e}"); raise

def alocar_e_carregar_medicos(engine):
    logging.info("Iniciando a lógica de alocação de médicos a hospitais...")
    
    query_medicos = """
        SELECT m.codigo, m.especialidade, m.municipio_id, 
               ST_Y(mu.localizacao) as latitude, ST_X(mu.localizacao) as longitude 
        FROM medicos m JOIN municipios mu ON m.municipio_id = mu.codigo_ibge
        WHERE mu.localizacao IS NOT NULL;
    """
    query_hospitais = """
        SELECT h.codigo, h.especialidades, h.municipio_id, 
               ST_Y(mu.localizacao) as latitude, ST_X(mu.localizacao) as longitude 
        FROM hospitais h JOIN municipios mu ON h.municipio_id = mu.codigo_ibge
        WHERE mu.localizacao IS NOT NULL;
    """
    medicos_df = pd.read_sql(query_medicos, engine)
    hospitais_df = pd.read_sql(query_hospitais, engine)

    if medicos_df.empty or hospitais_df.empty:
        logging.warning("Não há médicos ou hospitais suficientes para fazer a alocação. Pulando esta etapa.")
        return

    hospitais_por_municipio = {mid: g.to_dict('records') for mid, g in hospitais_df.groupby('municipio_id')}
    associacoes = []

    for medico in medicos_df.to_dict('records'):
        medico_id, medico_espec, medico_municipio_id = medico['codigo'], medico['especialidade'], medico['municipio_id']
        hospitais_alocados_count = 0
        hospitais_locais = hospitais_por_municipio.get(medico_municipio_id, [])
        if not hospitais_locais: continue

        candidatos = []
        for hospital in hospitais_locais:
            if medico_espec in hospital.get('especialidades', []):
                distancia = haversine_distance(medico['latitude'], medico['longitude'], hospital['latitude'], hospital['longitude'])
                if distancia <= 30:
                    candidatos.append({'hospital_id': hospital['codigo'], 'distancia': distancia})

        if candidatos:
            candidatos.sort(key=lambda x: x['distancia'])
            for candidato in candidatos:
                if hospitais_alocados_count < 3:
                    associacoes.append({'medico_id': medico_id, 'hospital_id': candidato['hospital_id']})
                    hospitais_alocados_count += 1
                else:
                    break

    if not associacoes:
        logging.warning("Nenhuma associação médico-hospital pôde ser criada com base nas regras.")
        return

    associacoes_df = pd.DataFrame(associacoes)
    clear_table(engine, 'medico_hospital_associacao')
    load_dataframe_to_table(engine, associacoes_df, 'medico_hospital_associacao')

# --- Função Principal de Carga ---

def run(dataframes: Dict[str, pd.DataFrame | Iterator]):
    logging.info("Iniciando a etapa de carga...")
    engine = get_database_engine()

    load_order = ['estados', 'municipios', 'cid10', 'hospitais', 'medicos']
    for table_name in load_order:
        data = dataframes.get(table_name)
        if data is None: continue
        clear_table(engine, table_name)
        if isinstance(data, pd.DataFrame):
            array_cols = ['especialidades'] if table_name == 'hospitais' else []
            load_dataframe_to_table(engine, data, table_name, array_cols)
    
    clear_table(engine, 'pacientes')
    pacientes_generator = dataframes.get('pacientes')
    if pacientes_generator:
        load_pacientes_with_dynamic_cids(engine, pacientes_generator)
    
    alocar_e_carregar_medicos(engine)
    
    engine.dispose()
    logging.info("Etapa de carga concluída.")