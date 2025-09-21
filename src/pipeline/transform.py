# src/pipeline/transform.py

import logging
import pandas as pd
from typing import Iterator, Dict
import math
import uuid
import random
from ..common.geography import haversine_distance 
# --- Funções Auxiliares (sem alterações, apenas garantindo que estejam completas) ---

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
    letra = codigo[0].upper()
    CID_CAPITULO_ESPECIALIDADE_MAP = {
        'A': 'Infectologia', 'B': 'Infectologia', 'C': 'Oncologia', 'D': 'Oncologia',
        'E': 'Endocrinologia', 'F': 'Psiquiatria', 'G': 'Neurologia', 'H': 'Oftalmologia',
        'I': 'Cardiologia', 'J': 'Pneumologia', 'K': 'Gastroenterologia', 'L': 'Dermatologia',
        'M': 'Ortopedia', 'N': 'Nefrologia', 'O': 'Ginecologia', 'P': 'Pediatria',
        'Q': 'Genética Médica', 'R': 'Clínica Geral', 'S': 'Traumatologia', 'T': 'Traumatologia',
        'U': 'Infectologia', 'V': 'Medicina de Emergência', 'W': 'Medicina de Emergência',
        'X': 'Medicina de Emergência', 'Y': 'Medicina de Emergência', 'Z': 'Clínica Geral'
    }
    if letra == 'D' and len(codigo) >= 3:
        try: 
            num = int(codigo[1:3])
            if 50 <= num <= 89: return 'Hematologia'
        except ValueError: pass
    if letra == 'H' and len(codigo) >= 3:
        try: 
            num = int(codigo[1:3])
            if 60 <= num <= 95: return 'Otorrinolaringologia'
        except ValueError: pass
    return CID_CAPITULO_ESPECIALIDADE_MAP.get(letra, 'Clínica Geral')

def haversine_distance(lat1, lon1, lat2, lon2):
    if any(v is None or pd.isna(v) for v in [lat1, lon1, lat2, lon2]): return float('inf')
    R = 6371
    dLat, dLon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = (math.sin(dLat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def create_point_string(row):
    if pd.notna(row['longitude']) and pd.notna(row['latitude']): return f"POINT({row['longitude']} {row['latitude']})"
    return None

def ensure_uuid(val):
    if pd.isna(val): return str(uuid.uuid4())
    try:
        uuid.UUID(str(val)); return val
    except (ValueError, TypeError):
        return str(uuid.uuid4())

def ensure_columns_exist(df, required_columns, fill_value=None):
    for col in required_columns:
        if col not in df.columns: df[col] = fill_value
    return df

def normalize_gender(genero):
    if pd.isna(genero) or genero is None: return None
    genero_str = str(genero).lower().strip()
    if genero_str in ['m', 'male', 'masculino', 'masc']: return 'M'
    elif genero_str in ['f', 'female', 'feminino', 'fem']: return 'F'
    else: return genero_str[0].upper() if genero_str else None

def clean_nome_fhir(nome_value):
    if pd.isna(nome_value) or nome_value is None: return None
    nome_data = None
    if isinstance(nome_value, str) and nome_value.strip().startswith('['):
        try:
            import json
            nome_data = json.loads(nome_value)
        except json.JSONDecodeError: pass
    elif isinstance(nome_value, list):
        nome_data = nome_value
    if nome_data and isinstance(nome_data, list) and len(nome_data) > 0:
        try:
            name_obj, parts = nome_data[0], []
            if 'given' in name_obj and isinstance(name_obj['given'], list): parts.extend(name_obj['given'])
            if 'family' in name_obj: parts.append(name_obj['family'])
            return ' '.join(parts)
        except (TypeError, KeyError): pass
    return clean_name(str(nome_value))

def normalizar_especialidade(especialidade: str) -> str:
    if not isinstance(especialidade, str): return ""
    normalized = especialidade.lower().strip()
    replacements = {'ã': 'a', 'á': 'a', 'à': 'a', 'â': 'a', 'é': 'e', 'ê': 'e', 'í': 'i', 'î': 'i', 'ó': 'o', 'ô': 'o', 'õ': 'o', 'ú': 'u', 'û': 'u', 'ç': 'c'}
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    return normalized

def allocate_hospital_intelligent(patient, all_hospitals_with_coords, municipios_df, general_hospitals_ids):
    cid_code, municipio_id = patient.get('cid_10'), patient.get('cod_municipio')

    if pd.notna(cid_code):
        required_specialty_norm = normalizar_especialidade(get_especialidade_from_cid(cid_code))
        ideal_candidates = [h for h in all_hospitals_with_coords if required_specialty_norm in [normalizar_especialidade(s) for s in h.get('especialidades', []) if s and s.strip()]]
        
        if ideal_candidates:
            if pd.notna(municipio_id):
                municipio_data = municipios_df.loc[municipios_df['codigo_ibge'] == int(municipio_id)]
                if not municipio_data.empty and pd.notna(municipio_data.iloc[0].get('latitude')):
                    patient_coords = {'latitude': municipio_data.iloc[0]['latitude'], 'longitude': municipio_data.iloc[0]['longitude']}
                    for cand in ideal_candidates:
                        cand['distance'] = haversine_distance(patient_coords['latitude'], patient_coords['longitude'], cand['latitude'], cand['longitude'])
                    return min(ideal_candidates, key=lambda x: x['distance'])['codigo']
            return ideal_candidates[0]['codigo']

    logging.warning(f"Paciente {patient.get('codigo')} sem CID ou sem hospital especializado. Usando fallback.")
    
    if pd.notna(municipio_id):
        municipio_data = municipios_df.loc[municipios_df['codigo_ibge'] == int(municipio_id)]
        if not municipio_data.empty and pd.notna(municipio_data.iloc[0].get('latitude')):
            patient_coords = {'latitude': municipio_data.iloc[0]['latitude'], 'longitude': municipio_data.iloc[0]['longitude']}
            for hospital in all_hospitals_with_coords:
                hospital['distance'] = haversine_distance(patient_coords['latitude'], patient_coords['longitude'], hospital['latitude'], hospital['longitude'])
            return min(all_hospitals_with_coords, key=lambda x: x.get('distance', float('inf')))['codigo']

    if general_hospitals_ids: return random.choice(general_hospitals_ids)
    if all_hospitals_with_coords: return random.choice(all_hospitals_with_coords)['codigo']
    return None

def run(dataframes: Dict[str, pd.DataFrame | Iterator]) -> Dict[str, pd.DataFrame | Iterator]:
    logging.info("Iniciando a etapa de transformação...")
    for name, df in dataframes.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]

    for entity in ['hospitais', 'medicos']:
        if entity in dataframes and isinstance(dataframes[entity], pd.DataFrame) and not dataframes[entity].empty:
            dataframes[entity]['codigo'] = dataframes[entity]['codigo'].apply(ensure_uuid)
    
    df_municipios, df_hospitais, df_estados = dataframes.get('municipios'), dataframes.get('hospitais'), dataframes.get('estados')
    
    valid_municipio_ids = set()
    if df_municipios is not None and not df_municipios.empty:
        df_municipios['codigo_ibge'] = pd.to_numeric(df_municipios['codigo_ibge'], errors='coerce')
        df_municipios.dropna(subset=['codigo_ibge'], inplace=True)
        df_municipios['codigo_ibge'] = df_municipios['codigo_ibge'].astype(int)
        valid_municipio_ids = set(df_municipios['codigo_ibge'])
        df_municipios['latitude'] = pd.to_numeric(df_municipios['latitude'], errors='coerce')
        df_municipios['longitude'] = pd.to_numeric(df_municipios['longitude'], errors='coerce')
        df_municipios['localizacao'] = df_municipios.apply(create_point_string, axis=1)

    if df_hospitais is not None and not df_hospitais.empty and df_municipios is not None:
        if 'cidade' in df_hospitais.columns: df_hospitais.rename(columns={'cidade': 'municipio_id'}, inplace=True)
        df_hospitais['municipio_id'] = pd.to_numeric(df_hospitais['municipio_id'], errors='coerce')
        df_hospitais.dropna(subset=['municipio_id'], inplace=True)
        df_hospitais['municipio_id'] = df_hospitais['municipio_id'].astype(int)
        df_hospitais = df_hospitais[df_hospitais['municipio_id'].isin(valid_municipio_ids)].copy()
        if 'especialidades' in df_hospitais.columns:
            df_hospitais['especialidades'] = df_hospitais['especialidades'].fillna('').astype(str).str.split(';').apply(lambda s: [spec.strip() for spec in s if spec.strip()])
        df_hospitais = pd.merge(df_hospitais, df_municipios[['codigo_ibge', 'latitude', 'longitude']], left_on='municipio_id', right_on='codigo_ibge', how='left').dropna(subset=['latitude', 'longitude'])
        df_hospitais['localizacao'] = df_hospitais.apply(create_point_string, axis=1)
        final_cols = ['codigo', 'nome', 'municipio_id', 'especialidades', 'leitos_totais', 'localizacao', 'latitude', 'longitude']
        existing_cols = [col for col in final_cols if col in df_hospitais.columns]
        dataframes['hospitais'] = df_hospitais[existing_cols]

    all_hospitals_with_coords, general_hospitals_ids = [], []
    if df_hospitais is not None and not df_hospitais.empty:
        all_hospitals_with_coords = df_hospitais.to_dict('records')
        general_hospitals_ids = [h['codigo'] for h in all_hospitals_with_coords if 'clinica geral' in [normalizar_especialidade(s) for s in h.get('especialidades', [])]]
        logging.info(f"Pré-processados {len(all_hospitals_with_coords)} hospitais ({len(general_hospitals_ids)} gerais) para alocação.")

    def process_single_pacientes_chunk(chunk_data):
        if not isinstance(chunk_data, pd.DataFrame) or chunk_data.empty: return pd.DataFrame()
        try:
            processed_chunk = chunk_data.copy()
            processed_chunk = ensure_columns_exist(processed_chunk, ['codigo', 'cpf', 'nome_completo', 'genero', 'cod_municipio', 'bairro', 'convenio', 'cid_10'])
            if 'cid-10' in processed_chunk.columns: processed_chunk.rename(columns={'cid-10': 'cid_10'}, inplace=True)
            
            # CORREÇÃO PARA CPF NULO
            original_count = len(processed_chunk)
            processed_chunk.dropna(subset=['cpf'], inplace=True)
            if len(processed_chunk) < original_count:
                logging.warning(f"Removidos {original_count - len(processed_chunk)} pacientes por terem CPF nulo.")
            if processed_chunk.empty:
                return pd.DataFrame()

            processed_chunk['codigo'] = processed_chunk['codigo'].apply(ensure_uuid)
            processed_chunk['nome_completo'] = processed_chunk['nome_completo'].apply(clean_nome_fhir)
            processed_chunk['genero'] = processed_chunk['genero'].apply(normalize_gender)
            processed_chunk['convenio'] = processed_chunk['convenio'].apply(lambda x: str(x).upper() == 'SIM')
            processed_chunk['cod_municipio'] = pd.to_numeric(processed_chunk['cod_municipio'], errors='coerce').astype('Int64')
            
            successful_allocations = 0
            for idx, patient_row in processed_chunk.iterrows():
                allocated_hospital = allocate_hospital_intelligent(patient_row.to_dict(), all_hospitals_with_coords, df_municipios, general_hospitals_ids)
                processed_chunk.loc[idx, 'hospital_alocado_id'] = allocated_hospital
                if allocated_hospital: successful_allocations += 1
            
            total_patients = len(processed_chunk)
            allocation_rate = (successful_allocations / total_patients * 100) if total_patients > 0 else 0
            logging.info(f"Chunk processado: {total_patients} pacientes, {successful_allocations} alocados ({allocation_rate:.1f}%)")
            
            return processed_chunk[['codigo', 'cpf', 'nome_completo', 'genero', 'cod_municipio', 'bairro', 'convenio', 'cid_10', 'hospital_alocado_id']]
        except Exception as e:
            logging.error(f"Erro ao processar chunk de pacientes: {e}")
            return None

    def safe_transform_pacientes(data_input):
        if data_input is None or isinstance(data_input, str): return iter([])
        data_iterator = [data_input] if isinstance(data_input, pd.DataFrame) else data_input
        for chunk in data_iterator:
            result = process_single_pacientes_chunk(chunk)
            if result is not None: yield result

    df_medicos = dataframes.get('medicos')
    df_cid10 = dataframes.get('cid10')
    
    # Processa a tabela CID-10
    if df_cid10 is not None: 
        df_cid10['especialidade'] = df_cid10['codigo'].astype(str).apply(get_especialidade_from_cid)
        dataframes['cid10'] = df_cid10 # Garante que a coluna nova seja salva

    # Processa a tabela de estados
    if df_estados is not None: 
        dataframes['estados'] = df_estados[['codigo_uf', 'uf', 'nome']]

    # Processa a tabela de médicos
    if df_medicos is not None:
        # --- Início do bloco corrigido ---
        if 'cidade' in df_medicos.columns: 
            df_medicos.rename(columns={'cidade': 'municipio_id'}, inplace=True)
        
        df_medicos['municipio_id'] = pd.to_numeric(df_medicos['municipio_id'], errors='coerce')
        df_medicos.dropna(subset=['municipio_id'], inplace=True)
        df_medicos['municipio_id'] = df_medicos['municipio_id'].astype(int)
        df_medicos = df_medicos[df_medicos['municipio_id'].isin(valid_municipio_ids)].copy()
        
        # Adiciona as coordenadas de latitude e longitude do município ao médico
        if df_municipios is not None:
            df_medicos = pd.merge(
                df_medicos,
                df_municipios[['codigo_ibge', 'latitude', 'longitude']],
                left_on='municipio_id',
                right_on='codigo_ibge',
                how='left'
            )
            # Remove médicos cujo município não tem coordenadas válidas
            df_medicos.dropna(subset=['latitude', 'longitude'], inplace=True)

        if 'especialidade' in df_medicos.columns:
            df_medicos['especialidade'] = df_medicos['especialidade'].str.strip()
        
        df_medicos['nome_completo'] = df_medicos['nome_completo'].apply(clean_name)
        
        # Garante que as colunas de coordenadas sejam incluídas na saída
        final_cols = ['codigo', 'nome_completo', 'especialidade', 'municipio_id', 'latitude', 'longitude']
        # Filtra para manter apenas as colunas que realmente existem no DataFrame
        existing_cols = [col for col in final_cols if col in df_medicos.columns]
        dataframes['medicos'] = df_medicos[existing_cols]
        # --- Fim do bloco corrigido ---

    # Processa a tabela de municípios
    if df_municipios is not None: 
        dataframes['municipios'] = df_municipios[['codigo_ibge', 'nome', 'codigo_uf', 'localizacao']]
    
    # Inicia o gerador de transformação de pacientes
    dataframes['pacientes'] = safe_transform_pacientes(dataframes.get('pacientes'))
    
    logging.info("Etapa de transformação concluída.")
    return dataframes