import logging
import pandas as pd
from typing import Dict, Iterator
from ingestion import converter
from .extract_utils import read_excel_cid10
import json
import os

def run() -> Dict:
    """
    Orquestra a extração de dados. Opera em dois modos:
    1. MODO MANIFESTO: Se 'upload_manifest.json' existe, processa os arquivos listados nele.
    2. MODO PADRÃO (FALLBACK): Se o manifesto não existe, processa uma lista fixa de arquivos padrão.
    """
    RAW_DATA_DIR = 'data/raw'
    manifest_path = os.path.join(RAW_DATA_DIR, 'upload_manifest.json')
    files_to_process = []

    # --- Lógica de decisão de modo ---
    if os.path.exists(manifest_path):
        logging.info("MODO MANIFESTO: 'upload_manifest.json' encontrado. Processando arquivos do upload.")
        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest_data = json.load(f)
            
            for entity_type, filenames in manifest_data.items():
                for filename in filenames:
                    full_path = os.path.join(RAW_DATA_DIR, filename)
                    if os.path.exists(full_path):
                        files_to_process.append((entity_type, full_path))
                    else:
                        logging.warning(f"Arquivo '{filename}' do manifesto não encontrado.")
            os.remove(manifest_path)
            logging.info("Manifesto processado e removido.")
        except Exception as e:
            logging.error(f"Falha ao processar o manifesto: {e}. A extração será interrompida.")
            return {}
    else:
        logging.info("MODO PADRÃO: Manifesto não encontrado. Usando a lista de arquivos padrão da pasta 'data/raw'.")
        files_to_process = [
            ('estados', 'data/raw/estados.csv'),
            ('municipios', 'data/raw/municipios.csv'),
            ('hospitais', 'data/raw/hospitais.csv'),
            ('medicos', 'data/raw/medicos_amostra.csv'),
            ('pacientes', 'data/raw/pacientes_amostra.xml'),
            ('cid10', 'data/raw/tabela CID-10.xlsx')
        ]

    # --- Lógica de processamento (comum aos dois modos) ---
    dataframes: Dict[str, pd.DataFrame | Iterator] = {}
    for entity_type, path in files_to_process:
        if not os.path.exists(path):
            logging.warning(f"Arquivo '{path}' não encontrado. Pulando.")
            continue
            
        logging.info(f"Ingerindo dados para '{entity_type}' do arquivo '{path}'...")
        try:
            if entity_type == 'cid10':
                df_or_iter = read_excel_cid10(path)
            else:
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
            logging.error(f"Falha ao ingerir o arquivo '{path}': {e}", exc_info=True)
    
    logging.info("Etapa de extração concluída.")
    return dataframes