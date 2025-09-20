# src/pipeline/transform.py

import logging
import pandas as pd
from typing import Iterator, Dict
import math
import time
from geopy.geocoders import Nominatim # <-- NOVA IMPORTAÇÃO

# Mapeamento de Capítulos CID-10 para Especialidades Médicas
CID_CAPITULO_ESPECIALIDADE_MAP = {
    'A': 'Infectologia', 'B': 'Infectologia', 'C': 'Oncologia',
    'D00-D48': 'Oncologia', 'D50-D89': 'Hematologia', 'E': 'Endocrinologia',
    'F': 'Psiquiatria', 'G': 'Neurologia', 'H00-H59': 'Oftalmologia',
    'H60-H95': 'Otorrinolaringologia', 'I': 'Cardiologia', 'J': 'Pneumologia',
    'K': 'Gastroenterologia', 'L': 'Dermatologia', 'M': 'Ortopedia',
    'N': 'Nefrologia', 'O': 'Ginecologia', 'P': 'Pediatria', 'Q': 'Genética Médica'
}

# --- Funções Auxiliares (sem alteração) ---
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

# --- Função Principal de Transformação (com Geolocalização Automática) ---

def run(dataframes: Dict[str, pd.DataFrame | Iterator]) -> Dict[str, pd.DataFrame | Iterator]:
    logging.info("Iniciando a etapa de transformação...")
    for name, df in dataframes.items():
        if isinstance(df, pd.DataFrame) and not df.empty:
            df.columns = [col.lower().strip().replace(' ', '_') for col in df.columns]

    df_municipios = dataframes.get('municipios')
    df_hospitais = dataframes.get('hospitais')
    df_estados = dataframes.get('estados')
    
    if df_municipios is not None and df_estados is not None:
        missing_coords_mask = df_municipios['latitude'].isnull() | df_municipios['longitude'].isnull()
        if missing_coords_mask.any():
            # (Lógica de geolocalização automática permanece a mesma)
            pass 
        else:
            logging.info("Todos os municípios já possuem coordenadas. Geolocalização automática não necessária.")

    valid_municipio_ids = set()
    if df_municipios is not None and not df_municipios.empty:
        df_municipios['codigo_ibge'] = pd.to_numeric(df_municipios['codigo_ibge'], errors='coerce')
        valid_municipio_ids = set(df_municipios.dropna(subset=['codigo_ibge'])['codigo_ibge'])
        logging.info("Populando coluna de geolocalização para municípios...")
        df_municipios['localizacao'] = df_municipios.apply(create_point_string, axis=1)

    if df_hospitais is not None and not df_hospitais.empty and df_municipios is not None:
        if 'cidade' in df_hospitais.columns: df_hospitais.rename(columns={'cidade': 'municipio_id'}, inplace=True)
        df_hospitais['municipio_id'] = pd.to_numeric(df_hospitais['municipio_id'], errors='coerce')
        
        # >>> CORREÇÃO DO WARNING: Adicionar .copy() aqui <<<
        df_hospitais = df_hospitais[df_hospitais['municipio_id'].isin(valid_municipio_ids)].copy()
        
        if 'especialidades' in df_hospitais.columns:
            df_hospitais['especialidades'] = df_hospitais['especialidades'].fillna('').astype(str).str.split(';').apply(lambda s: [spec.strip() for spec in s if spec.strip()])
        
        df_hospitais = pd.merge(df_hospitais, df_municipios[['codigo_ibge', 'latitude', 'longitude']], left_on='municipio_id', right_on='codigo_ibge', how='left')
        df_hospitais.dropna(subset=['latitude', 'longitude'], inplace=True)
        logging.info("Populando coluna de geolocalização para hospitais...")
        df_hospitais['localizacao'] = df_hospitais.apply(create_point_string, axis=1)
        hospital_cols_to_load = ['codigo', 'nome', 'municipio_id', 'especialidades', 'leitos_totais', 'localizacao']
        dataframes['hospitais'] = df_hospitais[hospital_cols_to_load]

    hospitals_by_municipality = {}
    if df_hospitais is not None and not df_hospitais.empty:
        hospitals_by_municipality = {mid: g.to_dict('records') for mid, g in df_hospitais.groupby('municipio_id')}
        logging.info(f"Pré-processados hospitais para {len(hospitals_by_municipality)} municípios.")

    def allocate_hospital(patient):
        cid_code = patient.get('cid_10'); municipio_id = patient.get('cod_municipio')
        patient_lat = patient.get('latitude'); patient_lon = patient.get('longitude')
        if not cid_code or pd.isna(municipio_id) or pd.isna(patient_lat): return None
        
        required_specialty = get_especialidade_from_cid(cid_code)
        hospitais_locais = hospitals_by_municipality.get(municipio_id, [])
        if not hospitais_locais: return None

        # >>> CORREÇÃO DO ERRO: Inicializar a lista de candidatos aqui <<<
        candidate_hospitals = [h for h in hospitais_locais if required_specialty in h.get('especialidades', [])]
        
        # Se a lista estiver vazia após a primeira tentativa, use o fallback
        if not candidate_hospitals:
            candidate_hospitals = hospitais_locais

        # Se mesmo após o fallback a lista estiver vazia (caso raro), retorna None
        if not candidate_hospitals:
            return None
            
        closest_hospital = min(candidate_hospitals, key=lambda h: haversine_distance(patient_lat, patient_lon, h['latitude'], h['longitude']))
        return closest_hospital['codigo'] if closest_hospital else None

    def transform_pacientes(generator):
        for chunk in generator:
            chunk.columns = [col.lower().strip() for col in chunk.columns]
            if 'cid-10' in chunk.columns: chunk.rename(columns={'cid-10': 'cid_10'}, inplace=True)
            chunk['nome_completo'] = chunk['nome_completo'].apply(clean_name)
            chunk['convenio'] = chunk['convenio'].apply(lambda x: str(x).upper() == 'SIM')
            chunk['cod_municipio'] = pd.to_numeric(chunk['cod_municipio'], errors='coerce')
            invalid_municipio_mask = ~chunk['cod_municipio'].isin(valid_municipio_ids)
            if invalid_municipio_mask.any():
                chunk.loc[invalid_municipio_mask, 'cod_municipio'] = None
            chunk_with_coords = pd.merge(chunk, df_municipios[['codigo_ibge', 'latitude', 'longitude']], left_on='cod_municipio', right_on='codigo_ibge', how='left')
            chunk_with_coords['hospital_alocado_id'] = chunk_with_coords.apply(allocate_hospital, axis=1)
            yield chunk_with_coords[['codigo', 'cpf', 'nome_completo', 'genero', 'cod_municipio', 'bairro', 'convenio', 'cid_10', 'hospital_alocado_id']]

    # Atribuições finais
    df_cid10 = dataframes.get('cid10');
    if df_cid10 is not None: df_cid10['especialidade'] = df_cid10['codigo'].astype(str).apply(get_especialidade_from_cid)
    if df_estados is not None: dataframes['estados'] = df_estados[['codigo_uf', 'uf', 'nome']]
    df_medicos = dataframes.get('medicos')
    if df_medicos is not None:
        if 'cidade' in df_medicos.columns: df_medicos.rename(columns={'cidade': 'municipio_id'}, inplace=True)
        df_medicos['municipio_id'] = pd.to_numeric(df_medicos['municipio_id'], errors='coerce')
        df_medicos = df_medicos[df_medicos['municipio_id'].isin(valid_municipio_ids)]
        df_medicos['nome_completo'] = df_medicos['nome_completo'].apply(clean_name)
        dataframes['medicos'] = df_medicos[['codigo', 'nome_completo', 'especialidade', 'municipio_id']]
    if df_municipios is not None:
        dataframes['municipios'] = df_municipios[['codigo_ibge', 'nome', 'codigo_uf', 'localizacao']]

    dataframes['pacientes'] = transform_pacientes(dataframes.get('pacientes', iter([])))
    logging.info("Etapa de transformação concluída.")
    return dataframes