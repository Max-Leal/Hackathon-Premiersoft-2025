# main.py
import logging
import pandas as pd

# Importa os módulos de cada camada
from src.pipeline import extract, transform, load
from src.core.allocation import doctor_allocator

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Orquestra a execução do pipeline de ETL com separação de responsabilidades."""
    logging.info("Iniciando o pipeline de ETL de dados de saúde.")

    # --- ETAPA 1: EXTRAÇÃO ---
    # Responsabilidade: Ler os dados brutos de diversas fontes.
    # (Sem alterações aqui)
    logging.info("--- Estágio 1: Extração ---")
    raw_dataframes = extract.run()

    # --- ETAPA 2: TRANSFORMAÇÃO ---
    # Responsabilidade: Limpar, normalizar, padronizar e enriquecer os dados.
    # Não executa lógica de negócio complexa como alocações.
    logging.info("--- Estágio 2: Transformação ---")
    clean_dataframes = transform.run(raw_dataframes)

    # --- ETAPA 3: LÓGICA DE NEGÓCIO (CORE) ---
    # Responsabilidade: Aplicar as regras de alocação. Esta é a nova etapa explícita.
    logging.info("--- Estágio 3: Lógica de Negócio (Alocações) ---")
    
    # Prepara os dados de entrada para a alocação de médicos
    # O transform.py já enriqueceu os hospitais com coordenadas, que são necessárias.
    medicos_para_alocar_df = clean_dataframes['medicos']
    hospitais_para_alocar_df = clean_dataframes['hospitais']
    
    # Chama a função pura de alocação que criamos em `core`
    logging.info("Executando a alocação de médicos...")
    doctor_associations_df = doctor_allocator.allocate(
        medicos_df=medicos_para_alocar_df,
        hospitais_df=hospitais_para_alocar_df
    )

    # (A lógica de alocação de pacientes ainda está em `transform.py`, vamos movê-la no futuro)
    # Por enquanto, `clean_dataframes` já contém os pacientes alocados.
    
    # --- ETAPA 4: CARGA ---
    # Responsabilidade: Persistir os dados finais no banco de dados.
    logging.info("--- Estágio 4: Carga ---")
    
    # Monta o dicionário final com todos os dados prontos para serem carregados
    final_data_to_load = {
        'estados': clean_dataframes['estados'],
        'municipios': clean_dataframes['municipios'],
        'cid10': clean_dataframes['cid10'],
        'hospitais': clean_dataframes['hospitais'],
        'medicos': clean_dataframes['medicos'],
        'pacientes': clean_dataframes['pacientes'],
        'medico_hospital_associacao': doctor_associations_df # Adiciona o resultado da alocação!
    }
    
    # O `load.run` agora é uma função simples que apenas salva o que recebe
    load.run(final_data_to_load)

    logging.info("Pipeline de ETL concluído com sucesso.")

if __name__ == "__main__":
    main()