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

## 🛠️ Requisitos

Python 3.12+
Docker e Docker Compose
Dependências listadas em requirements.txt

## 📌 Observações

Os dados de exemplo estão na pasta data/raw.
O banco de dados é inicializado via script SQL em scripts/init.sql.
As configurações do Streamlit estão na pasta .streamlit/.

## ✍️ APS Hackathon Premiersoft 2025
Projeto desenvolvido para fins educacionais e de inovação em saúde pública.
