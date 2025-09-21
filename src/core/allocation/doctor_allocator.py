# src/core/allocation/doctor_allocator.py
import logging
import pandas as pd
from ...common.geography import haversine_distance # Import relativo para voltar dois níveis de pasta

def allocate(medicos_df: pd.DataFrame, hospitais_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aloca médicos a hospitais com base em especialidade e proximidade.
    Esta é uma função pura: recebe DataFrames e retorna um DataFrame com as associações.
    """
    logging.info("Iniciando a lógica de alocação de médicos a hospitais...")
    
    # Validação inicial: se não houver dados, retorna um DataFrame vazio.
    if medicos_df.empty or hospitais_df.empty:
        logging.warning("DataFrame de médicos ou hospitais está vazio. Pulando alocação.")
        return pd.DataFrame(columns=['medico_id', 'hospital_id'])

    # FUNÇÃO APRIMORADA DE NORMALIZAÇÃO DE ESPECIALIDADES
    def normalizar_especialidade_completa(especialidade):
        """Normaliza especialidades com mapeamento de sinônimos e variações"""
        if not isinstance(especialidade, str) or not especialidade.strip():
            return ""
        
        # Normalização básica
        norm = especialidade.lower().strip()
        
        # Remover acentos
        replacements = {
            'ã': 'a', 'á': 'a', 'à': 'a', 'â': 'a', 'ä': 'a',
            'é': 'e', 'ê': 'e', 'è': 'e', 'ë': 'e',
            'í': 'i', 'î': 'i', 'ì': 'i', 'ï': 'i',
            'ó': 'o', 'ô': 'o', 'õ': 'o', 'ò': 'o', 'ö': 'o',
            'ú': 'u', 'û': 'u', 'ù': 'u', 'ü': 'u',
            'ç': 'c', 'ñ': 'n'
        }
        
        for old, new in replacements.items():
            norm = norm.replace(old, new)
        
        # Mapeamento de sinônimos e variações comuns
        especialidade_map = {
            'cardiologia': 'cardiologia', 'cardio': 'cardiologia', 'cardiovascular': 'cardiologia',
            'clinica geral': 'clinica geral', 'clinica medica': 'clinica geral', 'medicina interna': 'clinica geral',
            'pediatria': 'pediatria', 'pediatrica': 'pediatria',
            'ginecologia': 'ginecologia', 'gineco': 'ginecologia', 'obstetricia': 'ginecologia',
            'ortopedia': 'ortopedia', 'traumatologia': 'ortopedia',
            'neurologia': 'neurologia', 'neuro': 'neurologia',
            'psiquiatria': 'psiquiatria', 'saude mental': 'psiquiatria',
            'dermatologia': 'dermatologia', 'pele': 'dermatologia',
            'oftalmologia': 'oftalmologia', 'olhos': 'oftalmologia',
            'otorrinolaringologia': 'otorrinolaringologia', 'otorrino': 'otorrinolaringologia',
            'endocrinologia': 'endocrinologia', 'endocrino': 'endocrinologia',
            'gastroenterologia': 'gastroenterologia', 'gastro': 'gastroenterologia',
            'pneumologia': 'pneumologia', 'pulmao': 'pneumologia',
            'nefrologia': 'nefrologia', 'rins': 'nefrologia',
            'oncologia': 'oncologia', 'onco': 'oncologia', 'cancer': 'oncologia',
            'hematologia': 'hematologia', 'hemato': 'hematologia',
            'infectologia': 'infectologia', 'infecto': 'infectologia',
            'emergencia': 'medicina de emergencia', 'pronto socorro': 'medicina de emergencia',
            'anestesiologia': 'anestesiologia', 'anestesista': 'anestesiologia'
        }
        
        for key, value in especialidade_map.items():
            if key in norm or norm in key:
                return value
        
        return norm.replace(' ', ' ').strip()

    # Aplicar normalização
    medicos_df['especialidade_norm'] = medicos_df['especialidade'].apply(normalizar_especialidade_completa)
    
    # Pré-processar especialidades dos hospitais
    hospitais_processados = []
    for _, hospital in hospitais_df.iterrows():
        esp_list = hospital.get('especialidades', [])
        if isinstance(esp_list, str): esp_list = esp_list.strip('{}').replace('"', '').split(',')
        elif not isinstance(esp_list, list): esp_list = []
        
        esp_norm = [normalizar_especialidade_completa(e) for e in esp_list if e and e.strip()]
        
        hospital_dict = hospital.to_dict()
        hospital_dict['especialidades_norm'] = esp_norm
        hospitais_processados.append(hospital_dict)
    
    # Criar mapeamento por município para busca eficiente
    hospitais_por_municipio = {}
    for hospital in hospitais_processados:
        municipio_id = hospital.get('municipio_id')
        if municipio_id:
            if municipio_id not in hospitais_por_municipio:
                hospitais_por_municipio[municipio_id] = []
            hospitais_por_municipio[municipio_id].append(hospital)

    associacoes = []
    
    for _, medico in medicos_df.iterrows():
        medico_id = medico['codigo']
        medico_espec_norm = medico['especialidade_norm']
        medico_municipio_id = medico['municipio_id']
        medico_lat = medico['latitude']
        medico_lon = medico['longitude']
        
        if not medico_espec_norm:
            continue

        candidatos = []
        
        # ETAPA 1: Busca no mesmo município com especialidade compatível
        hospitais_locais = hospitais_por_municipio.get(medico_municipio_id, [])
        for hospital in hospitais_locais:
            if medico_espec_norm in hospital['especialidades_norm']:
                distancia = haversine_distance(medico_lat, medico_lon, hospital['latitude'], hospital['longitude'])
                candidatos.append({'hospital_id': hospital['codigo'], 'distancia': distancia, 'prioridade': 1})

        # ETAPA 2: Se não encontrou, busca no mesmo município sem filtro de especialidade
        if len(candidatos) < 3:
            for hospital in hospitais_locais:
                if hospital['codigo'] not in [c['hospital_id'] for c in candidatos]:
                    distancia = haversine_distance(medico_lat, medico_lon, hospital['latitude'], hospital['longitude'])
                    candidatos.append({'hospital_id': hospital['codigo'], 'distancia': distancia, 'prioridade': 2})

        # ETAPA 3: Busca em municípios próximos (até 30km) com especialidade compatível
        if len(candidatos) < 3:
            for hospital in hospitais_processados:
                if (hospital['municipio_id'] != medico_municipio_id and 
                    hospital['codigo'] not in [c['hospital_id'] for c in candidatos]):
                    distancia = haversine_distance(medico_lat, medico_lon, hospital['latitude'], hospital['longitude'])
                    if distancia <= 30 and medico_espec_norm in hospital['especialidades_norm']:
                        candidatos.append({'hospital_id': hospital['codigo'], 'distancia': distancia, 'prioridade': 3})

        # ETAPA 4: Se ainda não tem 3, busca próximos sem filtro de especialidade
        if len(candidatos) < 3:
            for hospital in hospitais_processados:
                if (hospital['municipio_id'] != medico_municipio_id and 
                    hospital['codigo'] not in [c['hospital_id'] for c in candidatos]):
                    distancia = haversine_distance(medico_lat, medico_lon, hospital['latitude'], hospital['longitude'])
                    if distancia <= 30:
                        candidatos.append({'hospital_id': hospital['codigo'], 'distancia': distancia, 'prioridade': 4})

        # SELEÇÃO FINAL
        if candidatos:
            candidatos.sort(key=lambda x: (x['prioridade'], x['distancia']))
            for candidato in candidatos[:3]:
                associacoes.append({'medico_id': medico_id, 'hospital_id': candidato['hospital_id']})

    if not associacoes:
        logging.warning("Nenhuma associação médico-hospital pôde ser criada.")
        return pd.DataFrame(columns=['medico_id', 'hospital_id'])

    associacoes_df = pd.DataFrame(associacoes).drop_duplicates()
    
    # Logging de estatísticas
    medicos_alocados = associacoes_df['medico_id'].nunique()
    total_medicos = len(medicos_df)
    percentual_alocacao = (medicos_alocados / total_medicos * 100) if total_medicos > 0 else 0
    logging.info(f"Lógica de alocação concluída. {len(associacoes_df)} associações criadas.")
    logging.info(f"{medicos_alocados} de {total_medicos} médicos foram alocados ({percentual_alocacao:.1f}%).")

    return associacoes_df