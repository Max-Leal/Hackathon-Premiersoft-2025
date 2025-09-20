# src/pipeline/extract.py

import pandas as pd
import logging
from lxml import etree
from typing import Iterator, Dict
import re 

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

def read_excel_cid10(filepath: str) -> pd.DataFrame:
    """
    Lê o arquivo Excel do CID-10, extrai códigos e descrições,
    e alerta sobre linhas que não seguem o padrão esperado.
    """
    try:
        logging.info(f"Lendo arquivo Excel (com estratégia regex final): {filepath}")
        
        # Lê a primeira coluna, tratando tudo como string e preenchendo vazios
        df_raw = pd.read_excel(filepath, header=None, usecols=[0], dtype=str).fillna('')
        
        cid_pattern = re.compile(r'^\s*([A-Z][0-9]{2}(?:\.[0-9A-Z])?)\s*-\s*(.*)')

        records = []
        skipped_lines_count = 0
        for index, item in enumerate(df_raw[0]): # Itera sobre a série
            if not item.strip():  # Pula linhas completamente vazias sem alarde
                continue

            match = cid_pattern.match(item)
            if match:
                codigo = match.group(1).strip()
                descricao = match.group(2).strip()
                records.append({'codigo': codigo, 'descricao': descricao})
            else:
                # >>> MELHORIA PRINCIPAL: Loga as linhas ignoradas <<<
                if skipped_lines_count < 5: # Mostra as 5 primeiras linhas com problema para não poluir o log
                    logging.warning(f"Linha {index + 1} do arquivo CID-10 ignorada por não corresponder ao padrão esperado: '{item}'")
                skipped_lines_count += 1

        if skipped_lines_count > 0:
            logging.warning(f"Total de {skipped_lines_count} linhas ignoradas no arquivo CID-10.")

        if not records:
            raise ValueError("Nenhum registro de CID-10 válido foi extraído.")
            
        return pd.DataFrame(records)

    except Exception as e:
        logging.error(f"Erro ao ler e processar o arquivo CID-10: {e}")
        return pd.DataFrame()

def stream_xml_to_dataframe_chunks(filepath: str, tag: str, chunk_size: int = 100000) -> Iterator[pd.DataFrame]:
    """Lê um arquivo XML grande em streaming e o converte em chunks de DataFrames do Pandas."""
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
            
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        if records_chunk:
            yield pd.DataFrame(records_chunk)
            
    except FileNotFoundError:
        logging.error(f"Arquivo XML não encontrado: {filepath}")
        return; yield
        
# --- Função Principal de Extração ---

def run() -> Dict[str, pd.DataFrame | Iterator]:
    """Executa a extração de todos os arquivos de dados da pasta raw."""
    data_sources = {
        'estados': ('csv', 'data/raw/estados.csv'),
        'municipios': ('csv', 'data/raw/municipios.csv'),
        'hospitais': ('csv', 'data/raw/hospitais.csv'),
        'medicos': ('csv', 'data/raw/medicos.csv'),
        'cid10': ('excel_cid', 'data/raw/tabela CID-10.xlsx'),
        'pacientes': ('xml_stream', 'data/raw/pacientes_amostra.xml')
    }
    
    dataframes = {}
    for name, (filetype, path) in data_sources.items():
        if filetype == 'csv':
            dataframes[name] = read_csv_resilient(path)
        elif filetype == 'excel_cid':
            dataframes[name] = read_excel_cid10(path)
        elif filetype == 'xml_stream':
            dataframes[name] = stream_xml_to_dataframe_chunks(path, tag='Paciente')
            
    logging.info("Extração concluída.")
    return dataframes