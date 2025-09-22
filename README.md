Com certeza! A seguir, apresento uma versão do seu README formatada em Markdown, ideal para o GitHub. Ela corrige a estrutura, melhora a legibilidade e adiciona elementos como uma tabela de conteúdos para facilitar a navegação.

APS Hackathon Premiersoft 2025
Sistema de gestão e análise de dados para saúde pública, com dashboard interativo, ingestão de dados, alocação inteligente de recursos e consulta de entidades.

📜 Tabela de Conteúdos
📂 Estrutura do Projeto

🚀 Como Rodar o Projeto

📊 Funcionalidades

🏛️ Arquitetura da Solução

🛠️ Requisitos

📌 Observações

✍️ Autores

📂 Estrutura do Projeto
A estrutura de pastas foi organizada para separar as responsabilidades e facilitar a manutenção.

├── data/              # Dados brutos e scripts de inicialização
├── scripts/           # Scripts SQL para inicialização do banco
├── src/
│   ├── common/        # Funções de utilidade (ex: cálculos geográficos)
│   ├── core/          # Lógica de negócio pura (ex: algoritmos de alocação)
│   ├── infrastructure/  # Conexão e interação com o banco de dados
│   ├── ingestion/     # Conversores para múltiplos formatos de arquivo (CSV, XML, HL7)
│   ├── pipeline/      # Orquestração do ETL (Extract, Transform, Load)
│   └── frontend/      # Interface do dashboard (Streamlit)
├── .streamlit/        # Configurações do Streamlit
├── docker-compose.yml # Orquestração de containers
└── requirements.txt   # Dependências Python
🚀 Como Rodar o Projeto
Siga os passos abaixo para executar a aplicação em seu ambiente local.

Pré-requisitos
Docker

Docker Compose

Python 3.11+

1. Clone o Repositório
Bash

git clone <URL_DO_SEU_REPOSITORIO>
cd <NOME_DO_SEU_REPOSITORIO>
2. Suba os Serviços com Docker Compose
Este comando irá construir as imagens, iniciar o banco de dados, executar o pipeline de ETL e, em seguida, subir a aplicação web.

Bash

docker-compose up --build
Alternativamente, você pode executar os serviços passo a passo:

Bash

# Constrói as imagens
docker-compose build

# Inicia apenas o banco de dados em background
docker-compose up -d db

# Executa o pipeline de ETL para popular o banco
docker-compose run pipeline

# Inicia todos os serviços (incluindo o frontend)
docker-compose up
3. Instale as Dependências (se for rodar localmente sem Docker)
Caso prefira executar o frontend fora do container, certifique-se de que o banco de dados esteja rodando via Docker e instale as dependências:

Bash

pip install -r requirements.txt
4. Execute o Frontend (se for rodar localmente sem Docker)
Com as dependências instaladas, inicie a interface Streamlit:

Bash

streamlit run src/frontend/app.py
A aplicação estará disponível em http://localhost:8501.

📊 Funcionalidades
Dashboard Interativo: Visualização dos principais indicadores de saúde, como ocupação de leitos, distribuição de profissionais e estatísticas epidemiológicas.

Ingestão de Dados: Sistema de upload robusto que aceita diversos formatos de dados brutos (Excel, CSV, XML, JSON, HL7).

Alocação Inteligente: Algoritmos para otimizar a alocação de médicos e pacientes, sugerindo as melhores combinações com base em localização, especialidade e capacidade.

Consulta de Entidades: Interface para busca, filtro e navegação por entidades cadastradas (hospitais, médicos, pacientes).

🏛️ Arquitetura da Solução
O sistema foi desenhado com uma clara separação de responsabilidades entre as camadas, facilitando a manutenção e a escalabilidade. O fluxo de dados segue as etapas clássicas de um pipeline de ETL.

Fluxo de Dados
Ingestão (/src/ingestion): Arquivos de múltiplos formatos (.csv, .xlsx, .hl7, etc.) são lidos e convertidos para um schema padronizado em memória (DataFrame Pandas). Um SCHEMA_MAP universal é usado para traduzir os nomes das colunas de diferentes fontes.

Transformação (/src/pipeline/transform.py): Os DataFrames padronizados passam por um processo de:

Limpeza de dados (valores nulos, formatos inconsistentes).

Validação de tipos (ex: leitos_totais para inteiro).

Remoção de duplicatas (ex: cpf de pacientes).

Enriquecimento (ex: adição de coordenadas geográficas a médicos e hospitais).

Lógica de Negócio (/src/core): Funções "puras" recebem os DataFrames limpos e aplicam as regras complexas de alocação de médicos e pacientes, retornando os resultados como novos DataFrames. Esta camada não tem conhecimento sobre o banco de dados ou a origem dos dados.

Carga (/src/pipeline/load.py): A camada final recebe todos os DataFrames processados e os persiste no banco de dados PostgreSQL, respeitando a ordem de dependência das tabelas para garantir a integridade referencial.

Visualização (/src/frontend): O dashboard Streamlit lê os dados já consolidados e limpos diretamente do banco de dados para apresentar os KPIs e gráficos interativos ao usuário final.

🛠️ Requisitos
Linguagem: Python 3.11

Processamento de Dados: Pandas

Banco de Dados: PostgreSQL + PostGIS

Dashboard: Streamlit

Containerização: Docker & Docker Compose

Bibliotecas Principais: sqlalchemy, psycopg2-binary, python-hl7, geopandas

📌 Observações
Os dados de exemplo para teste estão localizados na pasta data/raw/.

O banco de dados é inicializado com tabelas e tipos de dados customizados através do script scripts/init.sql.

As configurações do tema e layout do Streamlit estão na pasta .streamlit/.

✍️ Autores
Projeto desenvolvido por Matheus Dias Estacio, Eric Dias, Max Augusto Leal e Leonardo Muller Mandel para o APS Hackathon Premiersoft 2025, com fins educacionais e de inovação em saúde pública.
