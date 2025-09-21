import logging
import pandas as pd
from typing import Dict, Iterator

from ..infrastructure import database

def run(data_to_load: Dict[str, pd.DataFrame | Iterator]):
    """
    Carrega um dicionário de DataFrames ou iteradores de DataFrames no banco de dados.
    A ordem de carga é fixa para garantir a integridade referencial.
    """
    logging.info("Iniciando a etapa de carga no banco de dados...")
    engine = database.get_database_engine()

    # Ordem de carga para garantir que as tabelas de referência (chaves estrangeiras)
    # existam antes das tabelas que as utilizam.
    load_order = [
        'estados',
        'municipios',
        'cid10',
        'hospitais',
        'medicos',
        'pacientes',
        'medico_hospital_associacao' # Por último, pois depende de medicos e hospitais
    ]

    for table_name in load_order:
        if table_name in data_to_load:
            data = data_to_load[table_name]
            
            # Limpa a tabela antes de carregar novos dados
            database.clear_table(engine, table_name)
            
            logging.info(f"Carregando dados para a tabela '{table_name}'...")
            
            if isinstance(data, pd.DataFrame):
                # Define colunas de array (apenas para hospitais)
                array_cols = ['especialidades'] if table_name == 'hospitais' else None
                database.load_dataframe_to_table(engine, data, table_name, array_cols)
            
            elif hasattr(data, '__iter__'):
                # Caso especial para dados em streaming (pacientes)
                # Não há necessidade de lógica especial de CID aqui, pois
                # a transformação já deve ter garantido que eles existem.
                chunk_num = 0
                for chunk in data:
                    chunk_num += 1
                    logging.info(f"  -> Carregando chunk {chunk_num}...")
                    database.load_dataframe_to_table(engine, chunk, table_name)
            
            else:
                logging.warning(f"Formato de dados não suportado para a tabela '{table_name}'. Pulando.")

    engine.dispose()
    logging.info("Etapa de carga concluída.")