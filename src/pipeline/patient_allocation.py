# Sistema de Alocação Inteligente de Pacientes
# Aloca pacientes aos hospitais baseado em CID-10, especialidade e proximidade

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import math

def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcula a distância em km entre dois pontos geográficos usando a fórmula de Haversine
    """
    if any(pd.isna([lat1, lon1, lat2, lon2])):
        return float('inf')
    
    R = 6371  # Raio da Terra em km
    
    # Converter graus para radianos
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    # Diferenças
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    # Fórmula de Haversine
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    return R * c

def get_especialidade_from_cid(cid_code: str) -> str:
    """
    Mapeia códigos CID-10 para especialidades médicas
    """
    if not isinstance(cid_code, str) or not cid_code:
        return "Clínica Geral"
    
    # Pega a primeira letra do CID
    letra = cid_code[0].upper()
    
    # Mapeamento CID-10 para especialidades
    cid_especialidades = {
        'A': 'Infectologia',      # A00-B99: Doenças infecciosas
        'B': 'Infectologia',
        'C': 'Oncologia',         # C00-D48: Neoplasias
        'D': 'Oncologia',         # D00-D48: Neoplasias / D50-D89: Sangue
        'E': 'Endocrinologia',    # E00-E90: Endócrinas, nutricionais
        'F': 'Psiquiatria',       # F00-F99: Transtornos mentais
        'G': 'Neurologia',        # G00-G99: Sistema nervoso
        'H': 'Oftalmologia',      # H00-H59: Olho / H60-H95: Ouvido
        'I': 'Cardiologia',       # I00-I99: Sistema circulatório
        'J': 'Pneumologia',       # J00-J99: Sistema respiratório
        'K': 'Gastroenterologia', # K00-K93: Sistema digestivo
        'L': 'Dermatologia',      # L00-L99: Pele e tecido subcutâneo
        'M': 'Ortopedia',         # M00-M99: Sistema osteomuscular
        'N': 'Nefrologia',        # N00-N99: Sistema geniturinário
        'O': 'Ginecologia',       # O00-O99: Gravidez, parto
        'P': 'Pediatria',         # P00-P96: Período perinatal
        'Q': 'Genética Médica',   # Q00-Q99: Malformações congênitas
        'R': 'Clínica Geral',     # R00-R99: Sintomas não classificados
        'S': 'Traumatologia',     # S00-T98: Lesões e traumatismos
        'T': 'Traumatologia',
        'V': 'Medicina de Emergência', # V01-Y98: Causas externas
        'W': 'Medicina de Emergência',
        'X': 'Medicina de Emergência',
        'Y': 'Medicina de Emergência',
        'Z': 'Clínica Geral'      # Z00-Z99: Contatos com serviços
    }
    
    # Casos especiais para subcategorias do D e H
    if letra == 'D' and len(cid_code) >= 3:
        try:
            numero = int(cid_code[1:3])
            if 50 <= numero <= 89:
                return 'Hematologia'  # Doenças do sangue
        except ValueError:
            pass
    
    if letra == 'H' and len(cid_code) >= 3:
        try:
            numero = int(cid_code[1:3])
            if 60 <= numero <= 95:
                return 'Otorrinolaringologia'  # Doenças do ouvido
        except ValueError:
            pass
    
    return cid_especialidades.get(letra, 'Clínica Geral')

def normalizar_especialidade(especialidade: str) -> str:
    """
    Normaliza nomes de especialidades para facilitar comparação
    """
    if not isinstance(especialidade, str):
        return ""
    
    # Converte para minúsculas e remove acentos básicos
    normalized = especialidade.lower().strip()
    replacements = {
        'ã': 'a', 'á': 'a', 'à': 'a', 'â': 'a',
        'é': 'e', 'ê': 'e',
        'í': 'i', 'î': 'i',
        'ó': 'o', 'ô': 'o', 'õ': 'o',
        'ú': 'u', 'û': 'u',
        'ç': 'c'
    }
    
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    
    return normalized

class PatientAllocationSystem:
    """
    Sistema inteligente de alocação de pacientes a hospitais
    """
    
    def __init__(self):
        self.hospitais_df = None
        self.municipios_df = None
        self.pacientes_df = None
        self.hospital_especialidades_map = {}
        self.hospital_coordinates_map = {}
        
    def load_data(self, hospitais_df: pd.DataFrame, municipios_df: pd.DataFrame) -> bool:
        """
        Carrega dados de hospitais e municípios
        """
        try:
            self.hospitais_df = hospitais_df.copy()
            self.municipios_df = municipios_df.copy()
            
            # Preprocessar especialidades dos hospitais
            self._preprocess_hospital_specialties()
            
            # Preprocessar coordenadas
            self._preprocess_coordinates()
            
            logging.info(f"Sistema carregado com {len(self.hospitais_df)} hospitais e {len(self.municipios_df)} municípios")
            return True
            
        except Exception as e:
            logging.error(f"Erro ao carregar dados: {e}")
            return False
    
    def _preprocess_hospital_specialties(self):
        """
        Preprocessa as especialidades dos hospitais para facilitar busca
        """
        for _, hospital in self.hospitais_df.iterrows():
            hospital_id = hospital['codigo']
            especialidades = hospital.get('especialidades', [])
            
            # Converte string para lista se necessário
            if isinstance(especialidades, str):
                if especialidades.startswith('{') and especialidades.endswith('}'):
                    # Remove chaves e aspas, divide por vírgula
                    especialidades = especialidades.strip('{}').replace('"', '').split(',')
                else:
                    especialidades = [especialidades]
            
            if not isinstance(especialidades, list):
                especialidades = []
            
            # Normaliza especialidades
            normalized_specs = [normalizar_especialidade(spec) for spec in especialidades if spec and spec.strip()]
            
            self.hospital_especialidades_map[hospital_id] = {
                'original': especialidades,
                'normalized': normalized_specs,
                'municipio_id': hospital.get('municipio_id')
            }
    
    def _preprocess_coordinates(self):
        """
        Preprocessa coordenadas dos hospitais baseado nos municípios
        """
        # Merge hospitais com coordenadas dos municípios
        hospitais_coords = pd.merge(
            self.hospitais_df,
            self.municipios_df[['codigo_ibge', 'latitude', 'longitude']],
            left_on='municipio_id',
            right_on='codigo_ibge',
            how='left'
        )
        
        for _, row in hospitais_coords.iterrows():
            hospital_id = row['codigo']
            self.hospital_coordinates_map[hospital_id] = {
                'latitude': row.get('latitude'),
                'longitude': row.get('longitude'),
                'municipio_id': row.get('municipio_id')
            }
    
    def find_best_hospitals(self, patient_data: Dict, max_distance_km: float = 50, max_results: int = 3) -> List[Dict]:
        """
        Encontra os melhores hospitais para um paciente baseado em:
        1. Especialidade compatível com CID-10
        2. Proximidade geográfica
        3. Disponibilidade
        """
        
        cid_10 = patient_data.get('cid_10')
        patient_municipio = patient_data.get('cod_municipio')
        
        if not cid_10:
            logging.warning("Paciente sem CID-10 definido")
            return []
        
        # Determina especialidade necessária
        required_specialty = get_especialidade_from_cid(cid_10)
        required_specialty_norm = normalizar_especialidade(required_specialty)
        
        # Busca coordenadas do paciente
        patient_coords = self._get_patient_coordinates(patient_municipio)
        
        if not patient_coords:
            logging.warning(f"Não foi possível determinar localização do paciente (município: {patient_municipio})")
            return []
        
        candidates = []
        
        # Analisa cada hospital
        for hospital_id, hospital_data in self.hospital_especialidades_map.items():
            hospital_coords = self.hospital_coordinates_map.get(hospital_id)
            
            if not hospital_coords or pd.isna(hospital_coords['latitude']):
                continue
            
            # Calcula distância
            distance = haversine_distance(
                patient_coords['latitude'], patient_coords['longitude'],
                hospital_coords['latitude'], hospital_coords['longitude']
            )
            
            if distance > max_distance_km:
                continue
            
            # Verifica especialidade
            has_specialty = required_specialty_norm in hospital_data['normalized']
            
            # Prioridade baseada em especialidade e distância
            if has_specialty:
                priority = 1  # Alta prioridade - tem especialidade
                score = 1000 - distance  # Menor distância = maior score
            else:
                priority = 2  # Baixa prioridade - não tem especialidade específica
                score = 500 - distance   # Penalização por não ter especialidade
            
            # Bonus se for no mesmo município
            if hospital_data['municipio_id'] == patient_municipio:
                score += 100
            
            candidates.append({
                'hospital_id': hospital_id,
                'distance_km': round(distance, 2),
                'has_required_specialty': has_specialty,
                'required_specialty': required_specialty,
                'priority': priority,
                'score': score,
                'municipio_id': hospital_data['municipio_id']
            })
        
        # Ordena por prioridade (especialidade) e depois por score (distância)
        candidates.sort(key=lambda x: (x['priority'], -x['score']))
        
        # Retorna os melhores candidatos
        return candidates[:max_results]
    
    def _get_patient_coordinates(self, municipio_id) -> Optional[Dict]:
        """
        Busca coordenadas do município do paciente
        """
        if pd.isna(municipio_id):
            return None
            
        municipio_data = self.municipios_df[self.municipios_df['codigo_ibge'] == municipio_id]
        
        if municipio_data.empty:
            return None
        
        row = municipio_data.iloc[0]
        lat, lon = row.get('latitude'), row.get('longitude')
        
        if pd.isna(lat) or pd.isna(lon):
            return None
        
        return {'latitude': lat, 'longitude': lon}
    
    def allocate_patients_batch(self, pacientes_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aloca um lote de pacientes aos melhores hospitais disponíveis
        """
        results = []
        
        logging.info(f"Iniciando alocação para {len(pacientes_df)} pacientes")
        
        for idx, patient in pacientes_df.iterrows():
            patient_data = patient.to_dict()
            
            # Busca melhores hospitais
            best_hospitals = self.find_best_hospitals(patient_data)
            
            if best_hospitals:
                # Aloca no melhor hospital disponível
                best_hospital = best_hospitals[0]
                
                result = {
                    'patient_id': patient.get('codigo'),
                    'patient_name': patient.get('nome_completo'),
                    'cid_10': patient.get('cid_10'),
                    'required_specialty': best_hospital['required_specialty'],
                    'allocated_hospital_id': best_hospital['hospital_id'],
                    'distance_km': best_hospital['distance_km'],
                    'has_required_specialty': best_hospital['has_required_specialty'],
                    'allocation_priority': best_hospital['priority'],
                    'patient_municipio': patient.get('cod_municipio'),
                    'hospital_municipio': best_hospital['municipio_id'],
                    'same_municipio': patient.get('cod_municipio') == best_hospital['municipio_id']
                }
            else:
                # Não foi possível alocar
                result = {
                    'patient_id': patient.get('codigo'),
                    'patient_name': patient.get('nome_completo'),
                    'cid_10': patient.get('cid_10'),
                    'required_specialty': get_especialidade_from_cid(patient.get('cid_10')),
                    'allocated_hospital_id': None,
                    'distance_km': None,
                    'has_required_specialty': False,
                    'allocation_priority': None,
                    'patient_municipio': patient.get('cod_municipio'),
                    'hospital_municipio': None,
                    'same_municipio': False
                }
            
            results.append(result)
        
        results_df = pd.DataFrame(results)
        
        # Estatísticas de alocação
        allocated_count = results_df['allocated_hospital_id'].notna().sum()
        with_specialty_count = results_df['has_required_specialty'].sum()
        same_city_count = results_df['same_municipio'].sum()
        
        logging.info(f"Resultados da alocação:")
        logging.info(f"- {allocated_count}/{len(results_df)} pacientes alocados ({allocated_count/len(results_df)*100:.1f}%)")
        logging.info(f"- {with_specialty_count} alocações com especialidade compatível ({with_specialty_count/allocated_count*100:.1f}%)" if allocated_count > 0 else "")
        logging.info(f"- {same_city_count} alocações no mesmo município ({same_city_count/allocated_count*100:.1f}%)" if allocated_count > 0 else "")
        
        return results_df

# Função de uso simplificado
def allocate_patients_to_hospitals(pacientes_df: pd.DataFrame, 
                                 hospitais_df: pd.DataFrame, 
                                 municipios_df: pd.DataFrame) -> pd.DataFrame:
    """
    Função principal para alocar pacientes a hospitais
    
    Parâmetros:
    - pacientes_df: DataFrame com dados dos pacientes (deve ter: codigo, nome_completo, cid_10, cod_municipio)
    - hospitais_df: DataFrame com dados dos hospitais (deve ter: codigo, especialidades, municipio_id)
    - municipios_df: DataFrame com municípios (deve ter: codigo_ibge, latitude, longitude)
    
    Retorna:
    - DataFrame com resultados da alocação
    """
    
    # Inicializa o sistema
    allocation_system = PatientAllocationSystem()
    
    # Carrega dados
    if not allocation_system.load_data(hospitais_df, municipios_df):
        raise Exception("Falha ao carregar dados no sistema de alocação")
    
    # Executa alocação
    results = allocation_system.allocate_patients_batch(pacientes_df)
    
    return results

# Exemplo de uso:
if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Exemplo de dados (substitua pelos seus DataFrames reais)
    sample_patients = pd.DataFrame([
        {'codigo': 'P001', 'nome_completo': 'João Silva', 'cid_10': 'I21.0', 'cod_municipio': 3550308},
        {'codigo': 'P002', 'nome_completo': 'Maria Santos', 'cid_10': 'K29.1', 'cod_municipio': 3550308},
    ])
    
    sample_hospitals = pd.DataFrame([
        {'codigo': 'H001', 'nome': 'Hospital Cardio', 'especialidades': ['Cardiologia', 'Clínica Geral'], 'municipio_id': 3550308},
        {'codigo': 'H002', 'nome': 'Hospital Geral', 'especialidades': ['Gastroenterologia', 'Clínica Geral'], 'municipio_id': 3550308},
    ])
    
    sample_municipios = pd.DataFrame([
        {'codigo_ibge': 3550308, 'nome': 'São Paulo', 'latitude': -23.5505, 'longitude': -46.6333}
    ])
    
    # Execute a alocação
    try:
        allocation_results = allocate_patients_to_hospitals(sample_patients, sample_hospitals, sample_municipios)
        print("\nResultados da Alocação:")
        print(allocation_results.to_string(index=False))
    except Exception as e:
        logging.error(f"Erro na alocação: {e}")