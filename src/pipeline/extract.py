# src/pipeline/extract.py

import pandas as pd
import logging
from lxml import etree
from typing import Iterator

# --- Funções de Leitura Específicas ---

def read_csv_resilient(filepath: str) -> pd.DataFrame:
    """Lê um CSV de forma resiliente, ignorando e avisando sobre linhas malformadas."""
    try:
        logging.info(f"Lendo arquivo CSV: {filepath}")
        return pd.read_csv(filepath, on_bad_lines='warn')
    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo {filepath}: {e}")
        return pd.DataFrame()

def read_excel(filepath: str) -> pd.DataFrame:
    """Lê um arquivo Excel."""
    try:
        logging.info(f"Lendo arquivo Excel: {filepath}")
        return pd.read_excel(filepath)
    except FileNotFoundError:
        logging.error(f"Arquivo não encontrado: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Erro ao ler o arquivo {filepath}: {e}")
        return pd.DataFrame()

def stream_xml_to_dataframe_chunks(filepath: str, tag: str, chunk_size: int = 100000) -> Iterator[pd.DataFrame]:
    """
    Lê um arquivo XML grande em streaming e o converte em chunks de DataFrames do Pandas.
    Usa um gerador (yield) para não carregar tudo na memória.
    """
    logging.info(f"Iniciando streaming do arquivo XML: {filepath}")
    records_chunk = []
    
    try:
        context = etree.iterparse(filepath, events=('end',), tag=tag)
        for event, elem in context:
            record = {child.tag.lower(): child.text for child in elem.iterchildren()}
            records_chunk.append(record)
            
            if len(records_chunk) >= chunk_size:
                yield pd.DataFrame(records_chunk)
                records_chunk = []
            
            # Limpeza de memória crucial para arquivos grandes
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        if records_chunk:
            yield pd.DataFrame(records_chunk)
            
    except FileNotFoundError:
        logging.error(f"Arquivo XML não encontrado: {filepath}")
        # Retorna um gerador vazio
        return
        yield

# --- Função Principal de Extração ---

def run() -> dict:
    """
    Executa a extração de todos os arquivos de dados da pasta raw.
    Retorna um dicionário de DataFrames (e um gerador para o arquivo de pacientes).
    """
    data_sources = {
        'estados': ('csv', 'data/raw/estados.csv'),
        'municipios': ('csv', 'data/raw/municipios.csv'),
        'hospitais': ('csv', 'data/raw/hospitais.csv'),
        'medicos': ('csv', 'data/raw/medicos.csv'),
        'cid10': ('excel', 'data/raw/tabela CID-10.xlsx'),
        'pacientes': ('xml_stream', 'data/raw/pacientes.xml')
    }
    
    dataframes = {}
    
    for name, (filetype, path) in data_sources.items():
        if filetype == 'csv':
            dataframes[name] = read_csv_resilient(path)
        elif filetype == 'excel':
            dataframes[name] = read_excel(path)
        elif filetype == 'xml_stream':
            # Para pacientes, armazenamos o gerador, não o DataFrame completo
            dataframes[name] = stream_xml_to_dataframe_chunks(path, tag='Paciente')
            
    logging.info("Extração concluída.")
    return dataframes