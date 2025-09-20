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
        
    alocar_e_carregar_medicos(engine, dataframes)
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
# Em: src/pipeline/load.py

def alocar_e_carregar_medicos(engine, dataframes: Dict):
    """
    Executa a lógica de alocação de médicos a hospitais e carrega a tabela de associação.
    Esta função é chamada DEPOIS que as tabelas medicos e hospitais já foram carregadas.
    """
    logging.info("Iniciando a lógica de alocação de médicos a hospitais...")
    
    # 1. Obter os dados necessários do banco (agora que estão limpos e carregados)
    # >>> CORREÇÃO: Usando ST_X e ST_Y para extrair lon/lat da coluna de geometria <<<
    query_medicos = """
        SELECT m.codigo, m.especialidade, m.municipio_id, 
               ST_Y(mu.localizacao) as latitude, 
               ST_X(mu.localizacao) as longitude 
        FROM medicos m 
        JOIN municipios mu ON m.municipio_id = mu.codigo_ibge
        WHERE mu.localizacao IS NOT NULL;
    """
    query_hospitais = """
        SELECT h.codigo, h.especialidades, h.municipio_id, 
               ST_Y(mu.localizacao) as latitude, 
               ST_X(mu.localizacao) as longitude 
        FROM hospitais h 
        JOIN municipios mu ON h.municipio_id = mu.codigo_ibge
        WHERE mu.localizacao IS NOT NULL;
    """
    medicos_df = pd.read_sql(query_medicos, engine)
    hospitais_df = pd.read_sql(query_hospitais, engine)

    if medicos_df.empty or hospitais_df.empty:
        logging.warning("Não há médicos ou hospitais suficientes para fazer a alocação. Pulando esta etapa.")
        return

    # 2. Otimizar a busca por hospitais (pré-filtrar por município)
    hospitais_por_municipio = {mid: g.to_dict('records') for mid, g in hospitais_df.groupby('municipio_id')}

    associacoes = []
    # 3. Iterar sobre cada médico para encontrar hospitais
    for medico in medicos_df.to_dict('records'):
        medico_id = medico['codigo']
        medico_espec = medico['especialidade']
        medico_municipio_id = medico['municipio_id']
        
        hospitais_alocados = 0
        
        # Buscar hospitais apenas no mesmo município do médico
        hospitais_locais = hospitais_por_municipio.get(medico_municipio_id, [])
        if not hospitais_locais:
            continue # Pula para o próximo médico se não há hospitais na sua cidade

        # 4. Filtrar por especialidade e distância
        candidatos = []
        for hospital in hospitais_locais:
            # A especialidade do médico DEVE estar na lista de especialidades do hospital
            if medico_espec in hospital['especialidades']:
                distancia = haversine_distance(medico['latitude'], medico['longitude'], hospital['latitude'], hospital['longitude'])
                if distancia <= 30:
                    candidatos.append({'hospital_id': hospital['codigo'], 'distancia': distancia})

        # 5. Se houver candidatos, ordenar por distância e alocar até 3
        if candidatos:
            candidatos.sort(key=lambda x: x['distancia']) # Ordena do mais próximo para o mais distante
            for candidato in candidatos:
                if hospitais_alocados < 3:
                    associacoes.append({'medico_id': medico_id, 'hospital_id': candidato['hospital_id']})
                    hospitais_alocados += 1
                else:
                    break # Para de alocar para este médico se já atingiu o limite

    if not associacoes:
        logging.warning("Nenhuma associação médico-hospital pôde ser criada com base nas regras.")
        return

    # 6. Carregar os resultados na tabela de associação
    associacoes_df = pd.DataFrame(associacoes)
    clear_table(engine, 'medico_hospital_associacao')
    load_dataframe_to_table(engine, associacoes_df, 'medico_hospital_associacao')