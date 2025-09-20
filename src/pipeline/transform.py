# src/pipeline/transform.py

import logging
import pandas as pd
from typing import Iterator

# --- Funções de Limpeza Específicas ---

def clean_name(full_name: str) -> str:
    """Remove sobrenomes duplicados consecutivos de um nome completo."""
    if not isinstance(full_name, str):
        return full_name
        
    words = full_name.split()
    cleaned_words = []
    for i, word in enumerate(words):
        if i == 0 or word.lower() != words[i-1].lower():
            cleaned_words.append(word)
    return ' '.join(cleaned_words)

def transform_hospitais(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza a coluna 'especialidades' do DataFrame de hospitais."""
    if df.empty:
        return df
    
    logging.info("Transformando dados de hospitais: normalizando especialidades.")
    # Garante que a coluna exista e trata valores nulos antes de dividir
    if 'especialidades' in df.columns:
        df['especialidades'] = df['especialidades'].fillna('').astype(str).str.split(';')
    return df

def transform_medicos(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa a coluna de nomes do DataFrame de médicos."""
    if df.empty:
        return df
    
    logging.info("Transformando dados de médicos: limpando nomes.")
    if 'nome_completo' in df.columns:
        df['nome_completo'] = df['nome_completo'].apply(clean_name)
    return df

def transform_pacientes_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Limpa a coluna de nomes em um chunk do DataFrame de pacientes."""
    if chunk.empty:
        return chunk
    
    if 'nome_completo' in chunk.columns:
        chunk['nome_completo'] = chunk['nome_completo'].apply(clean_name)
    return chunk

def generate_cleaned_pacientes(pacientes_generator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """Cria um novo gerador que aplica a transformação a cada chunk de pacientes."""
    logging.info("Iniciando transformação em streaming para dados de pacientes.")
    for chunk in pacientes_generator:
        yield transform_pacientes_chunk(chunk)

# --- Função Principal de Transformação ---

def run(dataframes: dict) -> dict:
    """
    Executa a etapa de transformação em todos os DataFrames.
    """
    logging.info("Iniciando a etapa de transformação...")
    
    transformed_data = {}
    
    if 'hospitais' in dataframes:
        transformed_data['hospitais'] = transform_hospitais(dataframes['hospitais'])
        
    if 'medicos' in dataframes:
        transformed_data['medicos'] = transform_medicos(dataframes['medicos'])
        
    if 'pacientes' in dataframes:
        # Mantemos o padrão de gerador para os pacientes
        transformed_data['pacientes'] = generate_cleaned_pacientes(dataframes['pacientes'])
        
    # Adiciona os dataframes que não precisam de transformação complexa
    for name, df in dataframes.items():
        if name not in transformed_data:
            transformed_data[name] = df
            
    logging.info("Etapa de transformação concluída.")
    return transformed_data