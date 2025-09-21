import logging
from pipeline import extract, transform, load

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Orquestra a execução do pipeline de ETL."""
    logging.info("Iniciando o pipeline de ETL de dados de saúde.")

    # Etapa de Extração
    logging.info("--- Estágio 1: Extração ---")
    dataframes = extract.run()

    # Etapa de Transformação
    logging.info("--- Estágio 2: Transformação ---")
    transformed_data = transform.run(dataframes)

    # Etapa de Carga
    logging.info("--- Estágio 3: Carga ---")
    load.run(transformed_data)

    logging.info("Pipeline de ETL concluído com sucesso.")

if __name__ == "__main__":
    main()