# src/pipeline/transform.py

import logging
import pandas as pd
from typing import Iterator, Dict
import math
import uuid

# --- Funções Auxiliares ---

def clean_name(full_name: str) -> str:
    if not isinstance(full_name, str): return full_name
    words = full_name.split();
    if not words: return ""
    cleaned_words = [words[0]]
    for i in range(1, len(words)):
        if words[i].lower() != words[i-1].lower(): cleaned_words.append(words[i])
    return ' '.join(cleaned_words)

def get_especialidade_from_cid(codigo: str) -> str:
    if not isinstance(codigo, str) or not codigo: return "Clínica Geral"
    letra = codigo[0]
    CID_CAPITULO_ESPECIALIDADE_MAP = {'A': 'Infectologia', 'B': 'Infectologia', 'C': 'Oncologia', 'D00-D48': 'Oncologia', 'D50-D89': 'Hematologia', 'E': 'Endocrinologia', 'F': 'Psiquiatria', 'G': 'Neurologia', 'H00-H59': 'Oftalmologia', 'H60-H95': 'Otorrinolaringologia', 'I': 'Cardiologia', 'J': 'Pneumologia', 'K': 'Gastroenterologia', 'L': 'Dermatologia', 'M': 'Ortopedia', 'N': 'Nefrologia', 'O': 'Ginecologia', 'P': 'Pediatria', 'Q': 'Genética Médica'}
    if letra == 'D' and len(codigo) > 2:
        try: num = int(codigo[1:3]); return 'Hematologia' if 50 <= num <= 89 else 'Oncologia'
        except ValueError: pass
    if letra == 'H' and len(codigo) > 2:
        try: num = int(codigo[1:3]); return 'Oftalmologia' if 0 <= num <= 59 else 'Otorrinolaringologia'
        except ValueError: pass
    return CID_CAPITULO_ESPECIALIDADE_MAP.get(letra, 'Clínica Geral')

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371; dLat = math.radians(lat2 - lat1); dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def create_point_string(row):
    if pd.notna(row['longitude']) and pd.notna(row['latitude']):
        return f"POINT({row['longitude']} {row['latitude']})"
    return None

def ensure_uuid(val):
    """Verifica se um valor é um UUID válido, se não, gera um novo."""
    if pd.isna(val):
        return str(uuid.uuid4())
    try:
        uuid.UUID(str(val))
        return val
    except (ValueError, TypeError):
        return str(uuid.uuid4())

def ensure_columns_exist(df, required_columns, fill_value=None):
    """
    Utilitário para garantir que todas as colunas obrigatórias existam no DataFrame
    """
    for col in required_columns:
        if col not in df.columns:
            logging.warning(f"Adicionando coluna faltante '{col}' com valor padrão: {fill_value}")
            df[col] = fill_value
    return df

def normalize_gender(genero):
    """
    Normaliza valores de gênero para o formato do banco (M/F)
    """
    if pd.isna(genero) or genero is None:
        return None
    
    genero_str = str(genero).lower().strip()
    
    if genero_str in ['m', 'male', 'masculino', 'masc']:
        return 'M'
    elif genero_str in ['f', 'female', 'feminino', 'fem']:
        return 'F'
    else:
        # Se não reconhecer, retorna a primeira letra maiúscula
        return genero_str[0].upper() if genero_str else None

def clean_nome_fhir(nome_value):
    """
    Limpa nomes que podem vir no formato FHIR (como string JSON ou objeto Python).
    """
    if pd.isna(nome_value) or nome_value is None:
        return None

    nome_data = None
    # Tenta carregar se for uma string que parece JSON
    if isinstance(nome_value, str) and nome_value.strip().startswith('['):
        try:
            import json
            nome_data = json.loads(nome_value)
        except json.JSONDecodeError:
            # Se falhar, continua para o tratamento de string normal
            pass
    # Se já for uma lista (Pandas já converteu), usa diretamente
    elif isinstance(nome_value, list):
        nome_data = nome_value

    # Se conseguimos um objeto Python, extrai o nome
    if nome_data and isinstance(nome_data, list) and len(nome_data) > 0:
        try:
            name_obj = nome_data[0]
            parts = []
            if 'given' in name_obj and isinstance(name_obj['given'], list):
                parts.extend(name_obj['given'])
            if 'family' in name_obj:
                parts.append(name_obj['family'])
            return ' '.join(parts)
        except (TypeError, KeyError):
             # Se a estrutura interna for inesperada, trata como string normal
             pass
    
    # Fallback: se tudo falhar, apenas limpa a string como estava antes
    return clean_name(str(nome_value))

# --- Função Principal de Transformação ---

def run(dataframes: Dict[str, pd.DataFrame | Iterator]) -> Dict[str, pd.DataFrame | Iterator]:
    logging.info("Iniciando a etapa de transformação...")
    
    # Limpeza das colunas
    for name, df in dataframes.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]

    # --- LÓGICA DE GARANTIR UUIDs ---
    for entity in ['hospitais', 'medicos']:
        if entity in dataframes and isinstance(dataframes[entity], pd.DataFrame) and not dataframes[entity].empty:
            logging.info(f"Garantindo a existência de UUIDs para a entidade '{entity}'...")
            dataframes[entity]['codigo'] = dataframes[entity]['codigo'].apply(ensure_uuid)
    
    # Preparar dados básicos
    df_municipios = dataframes.get('municipios')
    df_hospitais = dataframes.get('hospitais')
    df_estados = dataframes.get('estados')
    
    valid_municipio_ids = set()
    if df_municipios is not None and not df_municipios.empty:
        df_municipios['codigo_ibge'] = pd.to_numeric(df_municipios['codigo_ibge'], errors='coerce')
        df_municipios['latitude'] = pd.to_numeric(df_municipios['latitude'], errors='coerce')
        df_municipios['longitude'] = pd.to_numeric(df_municipios['longitude'], errors='coerce')
        valid_municipio_ids = set(df_municipios.dropna(subset=['codigo_ibge'])['codigo_ibge'])
        df_municipios['localizacao'] = df_municipios.apply(create_point_string, axis=1)

    # Processar hospitais
    if df_hospitais is not None and not df_hospitais.empty and df_municipios is not None:
        if 'cidade' in df_hospitais.columns: 
            df_hospitais.rename(columns={'cidade': 'municipio_id'}, inplace=True)
        df_hospitais['municipio_id'] = pd.to_numeric(df_hospitais['municipio_id'], errors='coerce')
        df_hospitais = df_hospitais[df_hospitais['municipio_id'].isin(valid_municipio_ids)].copy()
        if 'especialidades' in df_hospitais.columns:
            df_hospitais['especialidades'] = df_hospitais['especialidades'].fillna('').astype(str).str.split(';').apply(lambda s: [spec.strip() for spec in s if spec.strip()])
        df_hospitais = pd.merge(df_hospitais, df_municipios[['codigo_ibge', 'latitude', 'longitude']], left_on='municipio_id', right_on='codigo_ibge', how='left')
        df_hospitais.dropna(subset=['latitude', 'longitude'], inplace=True)
        df_hospitais['localizacao'] = df_hospitais.apply(create_point_string, axis=1)
        hospital_cols_to_load = ['codigo', 'nome', 'municipio_id', 'especialidades', 'leitos_totais', 'localizacao']
        dataframes['hospitais'] = df_hospitais[hospital_cols_to_load]

    # Preparar mapa de hospitais por município
    hospitals_by_municipality = {}
    if df_hospitais is not None and not df_hospitais.empty:
        hospitals_by_municipality = {mid: g.to_dict('records') for mid, g in df_hospitais.groupby('municipio_id')}
        logging.info(f"Pré-processados hospitais para {len(hospitals_by_municipality)} municípios.")

    def allocate_hospital(patient):
        cid_code = patient.get('cid_10')
        municipio_id = patient.get('cod_municipio')
        patient_lat = patient.get('latitude')
        patient_lon = patient.get('longitude')
        
        if not cid_code or pd.isna(municipio_id) or pd.isna(patient_lat): 
            return None
            
        required_specialty = get_especialidade_from_cid(cid_code)
        hospitais_locais = hospitals_by_municipality.get(municipio_id, [])
        if not hospitais_locais: return None
        
        candidate_hospitals = [h for h in hospitais_locais if required_specialty in h.get('especialidades', [])]
        if not candidate_hospitals: candidate_hospitals = hospitais_locais
        if not candidate_hospitals: return None
        
        closest_hospital = min(candidate_hospitals, key=lambda h: haversine_distance(patient_lat, patient_lon, h['latitude'], h['longitude']))
        return closest_hospital['codigo'] if closest_hospital else None

    def process_single_pacientes_chunk(chunk_data):
        """
        Processa um único chunk de pacientes de forma completamente segura
        """
        logging.debug(f"process_single_pacientes_chunk recebeu tipo: {type(chunk_data)}")
        
        # VALIDAÇÃO CRÍTICA: Verificar se é string (problema conhecido)
        if isinstance(chunk_data, str):
            logging.error(f"ERRO CRÍTICO: Chunk é string: {chunk_data[:200]}...")
            return None
        
        # VALIDAÇÃO: Verificar se é DataFrame
        if not isinstance(chunk_data, pd.DataFrame):
            logging.error(f"ERRO: Chunk não é DataFrame. Tipo: {type(chunk_data)}")
            return None
        
        # VALIDAÇÃO: Verificar se não está vazio
        if chunk_data.empty:
            logging.warning("Chunk está vazio")
            return pd.DataFrame(columns=['codigo', 'cpf', 'nome_completo', 'genero', 'cod_municipio', 'bairro', 'convenio', 'cid_10', 'hospital_alocado_id'])
        
        try:
            # AGORA É SEGURO FAZER COPY
            processed_chunk = chunk_data.copy()
            logging.debug(f"Copy realizada com sucesso. Shape: {processed_chunk.shape}")
            
            # Garantir colunas obrigatórias
            required_columns = ['codigo', 'cpf', 'nome_completo', 'genero', 'cod_municipio', 'bairro', 'convenio', 'cid_10']
            processed_chunk = ensure_columns_exist(processed_chunk, required_columns)

            # Renomear coluna se necessário
            if 'cid-10' in processed_chunk.columns: 
                processed_chunk.rename(columns={'cid-10': 'cid_10'}, inplace=True)
            
            # Aplicar transformações
            processed_chunk['codigo'] = processed_chunk['codigo'].apply(ensure_uuid)
            
            # CORREÇÃO CRÍTICA: Limpar nomes FHIR e normalizar gênero
            processed_chunk['nome_completo'] = processed_chunk['nome_completo'].apply(clean_nome_fhir)
            processed_chunk['genero'] = processed_chunk['genero'].apply(normalize_gender)
            
            processed_chunk['convenio'] = processed_chunk['convenio'].apply(lambda x: str(x).upper() == 'SIM')
            processed_chunk['cod_municipio'] = pd.to_numeric(processed_chunk['cod_municipio'], errors='coerce')
            
            # Validar municípios
            invalid_municipio_mask = ~processed_chunk['cod_municipio'].isin(valid_municipio_ids)
            if invalid_municipio_mask.any():
                processed_chunk.loc[invalid_municipio_mask, 'cod_municipio'] = None
            
            # Merge com coordenadas
            chunk_with_coords = pd.merge(
                processed_chunk, 
                df_municipios[['codigo_ibge', 'latitude', 'longitude']], 
                left_on='cod_municipio', 
                right_on='codigo_ibge', 
                how='left'
            )
            
            # Alocar hospitais
            chunk_with_coords['hospital_alocado_id'] = chunk_with_coords.apply(allocate_hospital, axis=1)
            
            # Garantir colunas finais
            final_columns = ['codigo', 'cpf', 'nome_completo', 'genero', 'cod_municipio', 'bairro', 'convenio', 'cid_10', 'hospital_alocado_id']
            for col in final_columns:
                if col not in chunk_with_coords.columns:
                    chunk_with_coords[col] = None
            
            result_chunk = chunk_with_coords[final_columns]
            logging.info(f"Chunk processado com sucesso: {len(result_chunk)} registros")
            return result_chunk
            
        except Exception as e:
            logging.error(f"Erro ao processar chunk de pacientes: {e}")
            logging.error(f"Tipo original do chunk: {type(chunk_data)}")
            return None

    def safe_transform_pacientes(data_input):
        """
        Versão completamente segura de transform_pacientes
        """
        logging.info(f"=== SAFE TRANSFORM PACIENTES ===")
        logging.info(f"Tipo de entrada: {type(data_input)}")
        
        # Caso 1: Entrada é None
        if data_input is None:
            logging.warning("Entrada é None")
            return
        
        # Caso 2: Entrada é string (ERRO CONHECIDO)
        if isinstance(data_input, str):
            logging.error(f"ERRO CRÍTICO: Entrada é string: {data_input[:200]}...")
            return
        
        # Caso 3: Entrada é DataFrame único
        if isinstance(data_input, pd.DataFrame):
            logging.info(f"Processando DataFrame único com {len(data_input)} registros")
            result = process_single_pacientes_chunk(data_input)
            if result is not None:
                yield result
            return
        
        # Caso 4: Entrada é iterador
        if hasattr(data_input, '__iter__'):
            logging.info("Processando como iterador")
            chunk_count = 0
            
            try:
                for chunk in data_input:
                    chunk_count += 1
                    logging.info(f"Processando chunk {chunk_count}, tipo: {type(chunk)}")
                    
                    result = process_single_pacientes_chunk(chunk)
                    if result is not None:
                        yield result
                    else:
                        logging.warning(f"Chunk {chunk_count} retornou None")
                
                logging.info(f"Processamento de iterador concluído: {chunk_count} chunks")
                
            except Exception as e:
                logging.error(f"Erro ao iterar: {e}")
                return
        else:
            logging.error(f"Tipo de entrada não suportado: {type(data_input)}")
            return

    # Processar outras entidades
    df_cid10 = dataframes.get('cid10')
    if df_cid10 is not None: 
        df_cid10['especialidade'] = df_cid10['codigo'].astype(str).apply(get_especialidade_from_cid)
    
    if df_estados is not None: 
        dataframes['estados'] = df_estados[['codigo_uf', 'uf', 'nome']]
    
    df_medicos = dataframes.get('medicos')
    if df_medicos is not None:
        if 'cidade' in df_medicos.columns: 
            df_medicos.rename(columns={'cidade': 'municipio_id'}, inplace=True)
        df_medicos['municipio_id'] = pd.to_numeric(df_medicos['municipio_id'], errors='coerce')
        df_medicos = df_medicos[df_medicos['municipio_id'].isin(valid_municipio_ids)]

        # ----- ADICIONE ESTA LINHA PARA A CORREÇÃO FINAL -----
        if 'especialidade' in df_medicos.columns:
            df_medicos['especialidade'] = df_medicos['especialidade'].str.strip()
        # -----------------------------------------------------------

        df_medicos['nome_completo'] = df_medicos['nome_completo'].apply(clean_name)
        dataframes['medicos'] = df_medicos[['codigo', 'nome_completo', 'especialidade', 'municipio_id']]

    
    if df_municipios is not None:
        dataframes['municipios'] = df_municipios[['codigo_ibge', 'nome', 'codigo_uf', 'localizacao']]

    # PROCESSAMENTO SEGURO DE PACIENTES
    raw_pacientes = dataframes.get('pacientes')
    logging.info(f"=== PROCESSAMENTO DE PACIENTES ===")
    logging.info(f"Tipo dos dados brutos de pacientes: {type(raw_pacientes)}")
    
    if raw_pacientes is None:
        logging.warning("Não há dados de pacientes para processar")
        dataframes['pacientes'] = iter([])
    elif isinstance(raw_pacientes, str):
        logging.error(f"ERRO CRÍTICO: Dados de pacientes são string: {raw_pacientes[:200]}...")
        dataframes['pacientes'] = iter([])
    else:
        try:
            dataframes['pacientes'] = safe_transform_pacientes(raw_pacientes)
            logging.info("safe_transform_pacientes aplicado com sucesso")
        except Exception as e:
            logging.error(f"Erro ao aplicar safe_transform_pacientes: {e}")
            dataframes['pacientes'] = iter([])
    
    logging.info("Etapa de transformação concluída.")
    return dataframes