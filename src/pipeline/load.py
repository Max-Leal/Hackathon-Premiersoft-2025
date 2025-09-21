
# src/pipeline/load.py

import logging
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Iterator, Dict
import os
import math # <-- IMPORTAÇÃO NECESSÁRIA ADICIONADA AQUI
from .utils import haversine_distance

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
        WHERE mu.localizacao IS NOT NULL 
        AND m.especialidade IS NOT NULL 
        AND trim(m.especialidade) != '';
    """
    
    query_hospitais = """
        SELECT h.codigo, h.especialidades, h.municipio_id, 
               ST_Y(mu.localizacao) as latitude, ST_X(mu.localizacao) as longitude 
        FROM hospitais h JOIN municipios mu ON h.municipio_id = mu.codigo_ibge
        WHERE mu.localizacao IS NOT NULL 
        AND h.especialidades IS NOT NULL 
        AND array_length(h.especialidades, 1) > 0;
    """
    
    medicos_df = pd.read_sql(query_medicos, engine)
    hospitais_df = pd.read_sql(query_hospitais, engine)

    logging.info(f"Encontrados {len(medicos_df)} médicos com especialidade e localização válidas")
    logging.info(f"Encontrados {len(hospitais_df)} hospitais com especialidades e localização válidas")

    if medicos_df.empty or hospitais_df.empty:
        logging.warning("Não há médicos ou hospitais suficientes para fazer a alocação. Pulando esta etapa.")
        return
    
    # Normalizar especialidades
    def normalizar_especialidade(espec):
        """Normaliza nomes de especialidades para matching mais flexível"""
        if not isinstance(espec, str):
            return ""
        return espec.strip().lower().replace('ã', 'a').replace('í', 'i').replace('ó', 'o')
    
    # Aplicar normalização
    medicos_df['especialidade_norm'] = medicos_df['especialidade'].apply(normalizar_especialidade)
    
    # Pré-processar especialidades dos hospitais
    hospitais_processados = []
    for _, hospital in hospitais_df.iterrows():
        esp_list = hospital['especialidades']
        if isinstance(esp_list, str):
            # Se veio como string, converte para lista
            esp_list = esp_list.strip('{}').replace('"', '').split(',')
        elif not isinstance(esp_list, list):
            esp_list = []
        
        # Normalizar cada especialidade
        esp_norm = [normalizar_especialidade(e) for e in esp_list if e and e.strip()]
        
        hospital_dict = hospital.to_dict()
        hospital_dict['especialidades_norm'] = esp_norm
        hospitais_processados.append(hospital_dict)
    
    logging.info(f"Processadas especialidades para {len(hospitais_processados)} hospitais")
    
    # Criar mapeamento por município para busca eficiente
    hospitais_por_municipio = {}
    for hospital in hospitais_processados:
        municipio_id = hospital['municipio_id']
        if municipio_id not in hospitais_por_municipio:
            hospitais_por_municipio[municipio_id] = []
        hospitais_por_municipio[municipio_id].append(hospital)
    
    associacoes = []
    medicos_sem_alocacao = 0
    
    for _, medico in medicos_df.iterrows():
        medico_id = medico['codigo']
        medico_espec_norm = medico['especialidade_norm']
        medico_municipio_id = medico['municipio_id']
        medico_lat = medico['latitude']
        medico_lon = medico['longitude']
        
        if not medico_espec_norm:
            medicos_sem_alocacao += 1
            continue

        candidatos = []
        
        # ETAPA 1: Busca no mesmo município com especialidade compatível
        hospitais_locais = hospitais_por_municipio.get(medico_municipio_id, [])
        for hospital in hospitais_locais:
            if medico_espec_norm in hospital['especialidades_norm']:
                distancia = haversine_distance(medico_lat, medico_lon, 
                                             hospital['latitude'], hospital['longitude'])
                candidatos.append({
                    'hospital_id': hospital['codigo'], 
                    'distancia': distancia,
                    'prioridade': 1  # Mesma cidade + especialidade = prioridade máxima
                })

        # ETAPA 2: Se não encontrou, busca no mesmo município sem filtro de especialidade
        if len(candidatos) < 3:
            for hospital in hospitais_locais:
                if hospital['codigo'] not in [c['hospital_id'] for c in candidatos]:
                    distancia = haversine_distance(medico_lat, medico_lon, 
                                                 hospital['latitude'], hospital['longitude'])
                    candidatos.append({
                        'hospital_id': hospital['codigo'], 
                        'distancia': distancia,
                        'prioridade': 2  # Mesma cidade = prioridade média
                    })

        # ETAPA 3: Busca em municípios próximos (até 30km) com especialidade compatível
        if len(candidatos) < 3:
            for hospital in hospitais_processados:
                if (hospital['municipio_id'] != medico_municipio_id and 
                    hospital['codigo'] not in [c['hospital_id'] for c in candidatos]):
                    
                    distancia = haversine_distance(medico_lat, medico_lon, 
                                                 hospital['latitude'], hospital['longitude'])
                    
                    if distancia <= 30:  # Apenas hospitais próximos
                        if medico_espec_norm in hospital['especialidades_norm']:
                            candidatos.append({
                                'hospital_id': hospital['codigo'], 
                                'distancia': distancia,
                                'prioridade': 3  # Próximo + especialidade = prioridade baixa
                            })

        # ETAPA 4: Se ainda não tem 3, busca próximos sem filtro de especialidade
        if len(candidatos) < 3:
            for hospital in hospitais_processados:
                if (hospital['municipio_id'] != medico_municipio_id and 
                    hospital['codigo'] not in [c['hospital_id'] for c in candidatos]):
                    
                    distancia = haversine_distance(medico_lat, medico_lon, 
                                                 hospital['latitude'], hospital['longitude'])
                    
                    if distancia <= 30:  # Apenas hospitais próximos
                        candidatos.append({
                            'hospital_id': hospital['codigo'], 
                            'distancia': distancia,
                            'prioridade': 4  # Próximo = prioridade mínima
                        })

        # SELEÇÃO FINAL: Ordena por prioridade e depois por distância
        if candidatos:
            # Ordenar por prioridade (menor = melhor) e depois por distância
            candidatos.sort(key=lambda x: (x['prioridade'], x['distancia']))
            
            # Seleciona até 3 melhores candidatos
            for candidato in candidatos[:3]:
                associacoes.append({
                    'medico_id': medico_id, 
                    'hospital_id': candidato['hospital_id']
                })
        else:
            medicos_sem_alocacao += 1

    logging.info(f"Criadas {len(associacoes)} associações médico-hospital")
    logging.info(f"{medicos_sem_alocacao} médicos não puderam ser alocados")

    if not associacoes:
        logging.warning("Nenhuma associação médico-hospital pôde ser criada.")
        return

    # Salvar associações
    associacoes_df = pd.DataFrame(associacoes)
    clear_table(engine, 'medico_hospital_associacao')
    load_dataframe_to_table(engine, associacoes_df, 'medico_hospital_associacao')
    
    # Log de estatísticas finais
    medicos_com_alocacao = len(set(a['medico_id'] for a in associacoes))
    hospitais_com_medicos = len(set(a['hospital_id'] for a in associacoes))
    
    logging.info(f"Estatísticas finais:")
    logging.info(f"- {medicos_com_alocacao} médicos alocados (de {len(medicos_df)} elegíveis)")
    logging.info(f"- {hospitais_com_medicos} hospitais receberam médicos")
    logging.info(f"- {len(associacoes)} associações totais criadas")

# --- Função Principal de Carga ---

def run(dataframes: Dict[str, pd.DataFrame | Iterator]):
    logging.info("Iniciando a etapa de carga inteligente e segura...")
    engine = get_database_engine()

    # A ordem de carga é crucial e deve ser mantida
    load_order = ['estados', 'municipios', 'cid10', 'hospitais', 'medicos']

    for table_name in load_order:
        # VERIFICAÇÃO PRINCIPAL: só atua na tabela se houver dados novos para ela
        if table_name in dataframes:
            data = dataframes.get(table_name)
            
            # Garante que não é um iterador ou DataFrame vazio
            is_valid_data = False
            if isinstance(data, pd.DataFrame) and not data.empty:
                is_valid_data = True
            # No caso de iteradores, não podemos verificar se está vazio ainda, então consideramos válido
            elif isinstance(data, Iterator):
                is_valid_data = True

            if is_valid_data:
                logging.info(f"Novos dados para '{table_name}' detectados. Iniciando recarga...")
                clear_table(engine, table_name)
                
                # A lógica de carga original é mantida
                if isinstance(data, pd.DataFrame):
                    array_cols = ['especialidades'] if table_name == 'hospitais' else []
                    load_dataframe_to_table(engine, data, table_name, array_cols)
            else:
                logging.warning(f"Dados para '{table_name}' fornecidos, mas estão vazios. Nenhuma ação será tomada.")
        else:
            logging.info(f"Nenhum dado de upload para '{table_name}'. A tabela existente será preservada.")

    # --- Tratamento Especial para Pacientes ---
    # Usa a sua função customizada, mas apenas se dados de pacientes foram enviados
    if 'pacientes' in dataframes:
        logging.info("Novos dados para 'pacientes' detectados. Iniciando recarga com criação dinâmica de CIDs...")
        clear_table(engine, 'pacientes')
        pacientes_generator = dataframes.get('pacientes')
        if pacientes_generator:
            # Sua função original é chamada aqui, preservando a funcionalidade
            load_pacientes_with_dynamic_cids(engine, pacientes_generator)

    # --- Tratamento Inteligente para Alocação de Médicos ---
    # A realocação só é necessária se os dados que a influenciam (médicos, hospitais, municípios) mudaram.
    if 'medicos' in dataframes or 'hospitais' in dataframes or 'municipios' in dataframes:
        logging.info("Alterações em médicos, hospitais ou municípios detectadas. Executando a realocação de médicos...")
        # Sua função original é chamada aqui, preservando a funcionalidade
        alocar_e_carregar_medicos(engine)
    else:
        logging.info("Nenhuma alteração em médicos, hospitais ou municípios. A alocação de médicos existente será preservada.")
    
    engine.dispose()
    logging.info("Etapa de carga concluída.")
