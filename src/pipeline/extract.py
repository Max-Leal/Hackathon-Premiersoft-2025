# Correção para extract.py - função run()

import logging
import pandas as pd
from typing import Dict, Iterator
from ..ingestion import converter
from .extract_utils import read_excel_cid10

def run() -> Dict:
    files_to_process = [
        ('estados', 'data/raw/estados.csv'),
        ('municipios', 'data/raw/municipios.csv'),
        ('hospitais', 'data/raw/hospitais.csv'),
        #('hospitais', 'data/raw/hospitais_teste.jsonl'),
        ('medicos', 'data/raw/medicos_amostra.csv'),
        ('pacientes', 'data/raw/pacientes_amostra.xml'),
        ('pacientes', 'data/raw/pacientes_fhir.jsonl'),
        ('pacientes', 'data/raw/pacientes_adt_completo.hl7'),
    ]
    
    dataframes = {}
    for entity_type, path in files_to_process:
        logging.info(f"Ingerindo dados para '{entity_type}' do arquivo '{path}'...")
        try:
            df_or_iter = converter.run(path, entity_type)
            
            if entity_type in dataframes:
                current_data = dataframes[entity_type]
                new_data = df_or_iter
                
                if isinstance(current_data, Iterator): current_data = pd.concat(list(current_data), ignore_index=True)
                if isinstance(new_data, Iterator): new_data = pd.concat(list(new_data), ignore_index=True)

                dataframes[entity_type] = pd.concat([current_data, new_data], ignore_index=True)
            else:
                dataframes[entity_type] = df_or_iter
        except Exception as e:
            logging.error(f"Falha ao ingerir o arquivo {path}: {e}")
    
    try:
        dataframes['cid10'] = read_excel_cid10('data/raw/tabela CID-10.xlsx')
    except Exception as e:
        logging.error(f"Falha ao processar o arquivo CID-10: {e}")

    logging.info("Extração e conversão inicial concluídas.")
    return dataframes


# Alternativa: Função para verificar e corrigir tipos de dados
def validate_and_fix_dataframes(dataframes: Dict) -> Dict:
    """
    Valida e corrige tipos de dados nos dataframes
    """
    for entity_type, data in dataframes.items():
        if data is None:
            logging.warning(f"Dados para {entity_type} são None, substituindo por DataFrame vazio")
            dataframes[entity_type] = pd.DataFrame()
            
        elif isinstance(data, str):
            logging.error(f"Dados para {entity_type} são string inválida: {data[:100]}...")
            dataframes[entity_type] = pd.DataFrame()
            
        elif hasattr(data, '__iter__') and not isinstance(data, pd.DataFrame):
            # É um iterador, vamos validar que produz DataFrames
            def validated_iterator():
                for chunk in data:
                    if isinstance(chunk, pd.DataFrame):
                        yield chunk
                    else:
                        logging.warning(f"Chunk inválido em {entity_type}: {type(chunk)}")
            
            dataframes[entity_type] = validated_iterator()
    
    return dataframes