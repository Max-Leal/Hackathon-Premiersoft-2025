# APS Hackathon Premiersoft 2025

Sistema de **gestão e análise de dados para saúde pública**, com dashboard interativo, ingestão de dados, alocação inteligente de recursos e consulta de entidades.

---

## 📂 Estrutura do Projeto
```
├── data/ # Dados brutos e scripts de inicialização
├── scripts/ # Scripts SQL para inicialização do banco
├── src/
│ ├── dashboard/ # Módulos de dashboard
│ ├── frontend/ # Interface Streamlit e utilitários de banco
│ ├── pipeline/ # ETL: extract, transform, load
│ └── main.py # Ponto de entrada principal
├── requirements.txt # Dependências Python
├── docker-compose.yml # Orquestração de containers
```

---

## 🚀 Como Rodar o Projeto

### 1. Suba o Banco de Dados com Docker
No terminal, execute:

```bash
docker-compose build
docker-compose up -d db
docker-compose run pipeline
docker-compose up
```
Ou
```bash
docker-compose up --build
````

### 2. Instale as Dependências Python
Após o banco estar rodando:

```bash
pip install -r requirements.txt
```

### 3. Execute o Frontend (Streamlit)

Inicie a interface web:
```bash
cd ./src/frontend
python -m streamlit run app.py
```

## 📊 Funcionalidades

Dashboard: Visualização dos principais indicadores de saúde.
Upload: Ingestão de dados brutos (Excel, CSV, XML, JSON).
Alocação: Algoritmos para alocação inteligente de médicos e pacientes.
Consulta: Busca e navegação por entidades cadastradas.

## 🏛️ Arquitetura da Solução
O sistema foi desenhado com uma clara separação de responsabilidades entre as camadas, facilitando a manutenção e a escalabilidade. O fluxo de dados segue as etapas clássicas de um pipeline de ETL.
Estrutura de Componentes
A organização do código-fonte na pasta src/ reflete essa separação:
code
Code
src/
├── common/             # Funções de utilidade (ex: cálculos geográficos)
├── core/               # Lógica de negócio pura (ex: algoritmos de alocação)
├── infrastructure/     # Conexão e interação com o banco de dados
├── ingestion/          # Conversores para múltiplos formatos de arquivo (CSV, XML, HL7, etc.)
├── pipeline/           # Orquestração do ETL (Extract, Transform, Load)
├── frontend/           # Interface do dashboard (Streamlit)
└── main.py             # Ponto de entrada que orquestra o pipeline
Fluxo de Dados
Ingestão (/src/ingestion): Arquivos de múltiplos formatos (.csv, .xlsx, .hl7, etc.) são lidos e convertidos para um schema padronizado em memória (DataFrame Pandas). Um SCHEMA_MAP universal é usado para traduzir os nomes das colunas de diferentes fontes.
Transformação (/src/pipeline/transform.py): Os DataFrames padronizados passam por um processo de limpeza, validação de tipos (ex: leitos_totais para inteiro), remoção de duplicatas (ex: cpf de pacientes) e enriquecimento (ex: adição de coordenadas geográficas a médicos e hospitais).
Lógica de Negócio (/src/core): Funções "puras" recebem os DataFrames limpos e aplicam as regras complexas de alocação de médicos e pacientes, retornando os resultados como novos DataFrames. Esta camada não tem conhecimento do banco de dados.
Carga (/src/pipeline/load.py): A camada final recebe todos os DataFrames processados e os persiste no banco de dados PostgreSQL, respeitando a ordem de dependência das tabelas para garantir a integridade referencial.
Visualização (/src/frontend): O dashboard Streamlit lê os dados já consolidados e limpos diretamente do banco de dados para apresentar os KPIs e gráficos.

## 🛠️ Requisitos

Linguagem: Python 3.11
Processamento de Dados: Pandas
Banco de Dados: PostgreSQL + PostGIS
Dashboard: Streamlit
Containerização: Docker & Docker Compose
Bibliotecas Principais: sqlalchemy, psycopg2-binary, python-hl7.

## 📌 Observações

Os dados de exemplo estão na pasta data/raw.
O banco de dados é inicializado via script SQL em scripts/init.sql.
As configurações do Streamlit estão na pasta .streamlit/.

## ✍️ APS Hackathon Premiersoft 2025
Projeto desenvolvido para fins educacionais e de inovação em saúde pública.
