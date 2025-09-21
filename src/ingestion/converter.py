# src/ingestion/converter.py

import logging
import pandas as pd
from lxml import etree
from typing import Iterator, Dict
import json
import hl7  # Biblioteca para HL7 v2

from fhir.resources.patient import Patient

# --- Definição das Colunas Canônicas ---
CANONICAL_COLUMNS = {
    'hospitais': ['codigo', 'nome', 'municipio_id', 'especialidades', 'leitos_totais'],
    'medicos': ['codigo', 'nome_completo', 'especialidade', 'municipio_id'],
    'pacientes': ['codigo', 'cpf', 'nome_completo', 'genero', 'cod_municipio', 'bairro', 'convenio', 'cid_10'],
    'estados': ['codigo_uf', 'uf', 'nome', 'latitude', 'longitude'],
    'municipios': ['codigo_ibge', 'nome', 'latitude', 'longitude', 'codigo_uf']
}

# --- Mapa de Schemas (O "Dicionário de Tradução" Universal) ---
SCHEMA_MAPS = {
    'hospitais': {
        'csv': {'codigo': 'codigo', 'nome': 'nome', 'cidade': 'municipio_id', 'especialidades': 'especialidades', 'leitos_totais': 'leitos_totais'},
        'excel': {'ID_HOSP': 'codigo', 'Nome Fantasia': 'nome', 'IBGE': 'municipio_id', 'Lista de Especialidades': 'especialidades', 'Total de Leitos': 'leitos_totais'},
        'json': {'hospital_code': 'codigo', 'name': 'nome', 'city_ibge': 'municipio_id', 'specialties': 'especialidades', 'total_beds': 'leitos_totais'},
        'xml': {'codigo': 'codigo', 'nome': 'nome', 'municipio_id': 'municipio_id', 'especialidades': 'especialidades', 'leitos_totais': 'leitos_totais'},
        'fhir': {
            'id': 'codigo',
            'name': 'nome',
            'type': 'especialidades',
            'partOf.identifier.value': 'municipio_id'
        },
        'hl7': {
            'LOC.1': 'codigo',
            'LOC.2': 'nome',
            'LOC.4': 'municipio_id'
        }
    },
    'medicos': {
        'csv': {'codigo': 'codigo', 'nome_completo': 'nome_completo', 'especialidade': 'especialidade', 'cidade': 'municipio_id'},
        'excel': {'ID_MEDICO': 'codigo', 'Nome Completo do Profissional': 'nome_completo', 'Área de Atuação': 'especialidade', 'IBGE da Cidade': 'municipio_id'},
        'json': {'doctor_id': 'codigo', 'full_name': 'nome_completo', 'specialty': 'especialidade', 'city_ibge_code': 'municipio_id'},
        'xml': {'codigo': 'codigo', 'nome_completo': 'nome_completo', 'especialidade': 'especialidade', 'municipio_id': 'municipio_id'},
        'fhir': {
            'id': 'codigo',
            'name': 'nome_completo',
            'qualification': 'especialidade'
        },
        'hl7': {
            'STF.2': 'codigo',
            'STF.3': 'nome_completo',
            'STF.12': 'especialidade'
        }
    },
    'pacientes': {
        'csv': {'ID_PACIENTE': 'codigo', 'CPF_PACIENTE': 'cpf', 'NOME': 'nome_completo', 'SEXO': 'genero', 'COD_CIDADE': 'cod_municipio', 'BAIRRO': 'bairro', 'PLANO_SAUDE': 'convenio', 'DIAGNOSTICO_CID': 'cid_10'},
        'excel': {'ID Paciente': 'codigo', 'CPF': 'cpf', 'Nome Completo': 'nome_completo', 'Gênero': 'genero', 'Município IBGE': 'cod_municipio', 'Bairro': 'bairro', 'Possui Convênio': 'convenio', 'CID-10': 'cid_10'},
        'json': {'patient_id': 'codigo', 'national_id': 'cpf', 'name': 'nome_completo', 'gender': 'genero', 'city_code': 'cod_municipio', 'neighborhood': 'bairro', 'has_insurance': 'convenio', 'diagnosis_code': 'cid_10'},
        'xml': {'codigo': 'codigo', 'cpf': 'cpf', 'nome_completo': 'nome_completo', 'genero': 'genero', 'cod_municipio': 'cod_municipio', 'bairro': 'bairro', 'convenio': 'convenio', 'cid-10': 'cid_10'},
        'fhir': {
            'id': 'codigo',
            'identifier_cpf': 'cpf',
            'name': 'nome_completo',
            'gender': 'genero',
            'managingOrganization': 'cod_municipio'
        },
        'hl7': {
            'pid_3': 'codigo',
            'pid_5': 'nome_completo',
            'pid_8': 'genero'
        }
    },
    'estados': {
        'csv': {'codigo_uf': 'codigo_uf', 'uf': 'uf', 'nome': 'nome', 'latitude': 'latitude', 'longitude': 'longitude'},
        'excel': {'Cód UF': 'codigo_uf', 'UF': 'uf', 'Nome do Estado': 'nome', 'Lat': 'latitude', 'Lon': 'longitude'},
        'json': {'code': 'codigo_uf', 'acronym': 'uf', 'name': 'nome', 'lat': 'latitude', 'lon': 'longitude'},
        'xml': {'codigo_uf': 'codigo_uf', 'uf': 'uf', 'nome': 'nome', 'latitude': 'latitude', 'longitude': 'longitude'}
    },
    'municipios': {
        'csv': {'codigo_ibge': 'codigo_ibge', 'nome': 'nome', 'latitude': 'latitude', 'longitude': 'longitude', 'codigo_uf': 'codigo_uf'},
        'excel': {'IBGE': 'codigo_ibge', 'Nome Município': 'nome', 'Lat': 'latitude', 'Lon': 'longitude', 'UF Cód': 'codigo_uf'},
        'json': {'ibge_code': 'codigo_ibge', 'city_name': 'nome', 'lat': 'latitude', 'lon': 'longitude', 'state_code': 'codigo_uf'},
        'xml': {'codigo_ibge': 'codigo_ibge', 'nome': 'nome', 'latitude': 'latitude', 'longitude': 'longitude', 'codigo_uf': 'codigo_uf'}
    }
}


def _ensure_canonical_schema(df: pd.DataFrame, entity_type: str) -> pd.DataFrame:
    """Garante que o DataFrame tenha todas as colunas canônicas, preenchendo as faltantes com None."""
    if entity_type not in CANONICAL_COLUMNS:
        return df
    
    # Adicione .copy() aqui para evitar o warning
    df_copy = df.copy()

    final_cols = CANONICAL_COLUMNS[entity_type]
    for col in final_cols:
        if col not in df_copy.columns:
            df_copy[col] = None
    return df_copy[final_cols]

# --- Adaptadores (Funções especialistas em ler cada formato) ---

def from_csv(filepath: str, schema_map: dict, entity_type: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, on_bad_lines='warn', dtype=str)
    df.rename(columns=schema_map, inplace=True)
    available_cols = [col for col in schema_map.values() if col in df.columns]
    df_filtered = df[available_cols] if available_cols else df
    return _ensure_canonical_schema(df_filtered, entity_type)

def from_excel(filepath: str, schema_map: dict, entity_type: str) -> pd.DataFrame:
    df = pd.read_excel(filepath, dtype=str, engine='openpyxl')
    df.rename(columns=schema_map, inplace=True)
    available_cols = [col for col in schema_map.values() if col in df.columns]
    df_filtered = df[available_cols] if available_cols else df
    return _ensure_canonical_schema(df_filtered, entity_type)

def from_json(filepath: str, schema_map: dict, entity_type: str) -> pd.DataFrame:
    df = pd.read_json(filepath, lines=True, dtype=str)
    df.rename(columns=schema_map, inplace=True)
    available_cols = [col for col in schema_map.values() if col in df.columns]
    df_filtered = df[available_cols] if available_cols else df
    return _ensure_canonical_schema(df_filtered, entity_type)

def from_xml_stream(filepath: str, schema_map: dict, entity_type: str, tag: str, chunk_size: int = 1000) -> Iterator[pd.DataFrame]:
    records_chunk = []
    try:
        context = etree.iterparse(filepath, events=('end',), tag=tag)
        for _, elem in context:
            record = {}
            for child in elem.iterchildren():
                key = child.tag.lower()
                record[key] = child.text
            records_chunk.append(record)
            
            if len(records_chunk) >= chunk_size:
                if records_chunk:
                    chunk_df = pd.DataFrame(records_chunk)
                    chunk_df.rename(columns=schema_map, inplace=True)
                    available_cols = [col for col in schema_map.values() if col in chunk_df.columns]
                    if available_cols:
                        chunk_df = chunk_df[available_cols]
                    yield _ensure_canonical_schema(chunk_df, entity_type)
                records_chunk = []
            
            elem.clear()
            while elem.getprevious() is not None: 
                del elem.getparent()[0]
    except Exception as e:
        logging.error(f"Erro ao processar XML: {e}")
        
    # Processa os registros restantes
    if records_chunk:
        chunk_df = pd.DataFrame(records_chunk)
        chunk_df.rename(columns=schema_map, inplace=True)
        available_cols = [col for col in schema_map.values() if col in chunk_df.columns]
        if available_cols:
            chunk_df = chunk_df[available_cols]
        yield _ensure_canonical_schema(chunk_df, entity_type)

def from_fhir_json(filepath: str) -> pd.DataFrame:
    records = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                if data.get('resourceType') != 'Patient': continue
                patient = Patient(**data)
                nome = ' '.join(patient.name[0].given) if patient.name and patient.name[0].given else ''
                sobrenome = patient.name[0].family if patient.name and patient.name[0].family else ''
                records.append({
                    'id': patient.id,
                    'name': f"{nome} {sobrenome}".strip(),
                    'gender': str(patient.gender).upper()[0] if patient.gender else None,
                    'managingOrganization': patient.managingOrganization.identifier.value if patient.managingOrganization and patient.managingOrganization.identifier else None
                })
            except Exception as e: logging.warning(f"Falha ao processar linha FHIR: {e}")
    return pd.DataFrame(records)

def from_hl7(filepath: str, entity_type: str) -> pd.DataFrame:
    """Lê mensagens HL7 e extrai os dados brutos que encontrar."""
    records = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            
            # Dividir mensagens por MSH no início da linha
            messages_raw = []
            lines = content.split('\n')
            current_message = []
            
            for line in lines:
                line = line.strip()
                if line.startswith('MSH'):
                    if current_message:
                        messages_raw.append('\n'.join(current_message))
                    current_message = [line]
                else:
                    if current_message:  # Só adiciona se já temos um MSH
                        current_message.append(line)
            
            # Adicionar última mensagem
            if current_message:
                messages_raw.append('\n'.join(current_message))
            
            for msg_raw in messages_raw:
                if not msg_raw.strip():
                    continue
                    
                try:
                    msg = hl7.parse(msg_raw.replace('\n', '\r'))
                    
                    # Procurar pelo segmento PID
                    pid_segment = None
                    for segment in msg:
                        if str(segment[0]) == 'PID':
                            pid_segment = segment
                            break
                    
                    if pid_segment and len(pid_segment) > 3:
                        # Extrair dados do PID de forma mais robusta
                        patient_id = str(pid_segment[3][0][0]) if pid_segment[3] and len(pid_segment[3][0]) > 0 else None
                        
                        # Nome do paciente (PID.5) - formato: SOBRENOME^NOME
                        nome_completo = ""
                        if len(pid_segment) > 5 and pid_segment[5]:
                            nome_components = str(pid_segment[5][0][0]).split('^')
                            if len(nome_components) >= 2:
                                # Formato: SOBRENOME^NOME -> NOME SOBRENOME
                                nome_completo = f"{nome_components[1]} {nome_components[0]}".strip()
                            elif len(nome_components) == 1:
                                nome_completo = nome_components[0].strip()
                        
                        # Gênero (PID.8)
                        genero = str(pid_segment[8][0][0]) if len(pid_segment) > 8 and pid_segment[8] else None
                        
                        records.append({
                            'pid_3': patient_id,
                            'pid_5': nome_completo,
                            'pid_8': genero
                        })
                except Exception as e:
                    logging.warning(f"Falha ao processar mensagem HL7: {e}")
                    logging.debug(f"Conteúdo da mensagem problemática: {msg_raw[:100]}...")
                    
    except Exception as e:
        logging.error(f"Erro ao abrir arquivo HL7: {e}")
    
    return pd.DataFrame(records)

# --- Orquestrador (A Fábrica que decide qual adaptador usar) ---

def get_file_format(filepath: str) -> str:
    """Determina o formato do arquivo com base na sua extensão."""
    filepath_lower = filepath.lower()
    if filepath_lower.endswith('.csv'): return 'csv'
    if filepath_lower.endswith('.xlsx') or filepath_lower.endswith('.xls'): return 'excel'
    if filepath_lower.endswith('.jsonl'): return 'json'
    if filepath_lower.endswith('.json'): return 'fhir'  # Assumimos que .json solto é um recurso FHIR
    if filepath_lower.endswith('.xml'): return 'xml'
    if filepath_lower.endswith('.hl7'): return 'hl7'
    raise ValueError(f"Formato de arquivo não suportado para o caminho: {filepath}")

def run(filepath: str, entity_type: str) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """
    Lê qualquer arquivo de qualquer entidade, traduz para o formato canônico e retorna
    um DataFrame ou um gerador de DataFrames.
    """
    file_format = get_file_format(filepath)
    schema_map = SCHEMA_MAPS.get(entity_type, {}).get(file_format, {})

    logging.info(f"Processando arquivo {filepath} (formato: {file_format}, entidade: {entity_type})")

    if file_format == 'csv':
        return from_csv(filepath, schema_map, entity_type)
        
    elif file_format == 'excel':
        return from_excel(filepath, schema_map, entity_type)
        
    elif file_format == 'json':
        return from_json(filepath, schema_map, entity_type)
        
    elif file_format == 'fhir':
        # O adaptador FHIR não usa o schema_map diretamente, mas a conformidade final sim
        df_bruto = from_fhir_json(filepath, entity_type)
        if not df_bruto.empty and schema_map:
            df_bruto.rename(columns=schema_map, inplace=True)
        return _ensure_canonical_schema(df_bruto, entity_type)
        
    elif file_format == 'hl7':
        # O adaptador HL7 também não usa o schema_map diretamente
        df_bruto = from_hl7(filepath, entity_type)
        if not df_bruto.empty and schema_map:
            df_bruto.rename(columns=schema_map, inplace=True)
        return _ensure_canonical_schema(df_bruto, entity_type)
        
    elif file_format == 'xml':
        # O XML é o único caso que retorna um gerador
        if entity_type == 'pacientes':
            return from_xml_stream(filepath, schema_map, entity_type, tag='Paciente')
        elif entity_type == 'hospitais':
            return from_xml_stream(filepath, schema_map, entity_type, tag='Hospital')
        elif entity_type == 'medicos':
            return from_xml_stream(filepath, schema_map, entity_type, tag='Medico')
        else:
            # Para outras entidades, tenta uma tag genérica
            tag_name = entity_type.rstrip('s').capitalize()  # Remove 's' do plural e capitaliza
            return from_xml_stream(filepath, schema_map, entity_type, tag=tag_name)
    
    # Se nenhum dos 'if' acima corresponder, o formato não é suportado
    raise NotImplementedError(f"Adaptador para o formato '{file_format}' não implementado.")