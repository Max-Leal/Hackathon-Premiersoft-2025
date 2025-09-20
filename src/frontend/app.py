import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import numpy as np
import time

# --- CONFIGURAÇÕES DA PÁGINA ---
st.set_page_config(
    page_title="APS",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS DE PRECISÃO PARA UM DESIGN MODERNO ---
st.markdown("""
<style>
    /* Sombra e estilos da Sidebar */
    [data-testid="stSidebar"] {
        box-shadow: 3px 0px 15px rgba(0, 0, 0, 0.05);
    }

    [data-testid="stSidebar"] > div:first-child {
        background-color: #FFFFFF;
        padding: 1.5rem 0.75rem; 
    }

    /* Estilos do option_menu (links) */
    .nav-link {
        border-radius: 0.5rem;
        color: #8A91A8;
        padding: 0.75rem 1rem !important;
        margin: 0.4rem 0 !important;
        font-weight: 500;
        font-size: 1rem;
    }

    .nav-link-icon {
        font-size: 1.25rem;
    }

    .nav-link:hover {
        background-color: #F0F2F6;
        color: #1E202A;
    }

    .nav-link:hover .nav-link-icon {
        color: #1E202A;
    }
    
    /* A cor de fundo roxa do item ativo vem do primaryColor do tema */
    .nav-link.active, .nav-link.active:hover {
        font-weight: 500; 
        color: #FFFFFF !important;
    }
    
    .nav-link.active .nav-link-icon {
        color: #FFFFFF !important;
    }

    /* Seta (>) à direita */
    .nav-link::after {
        content: '>';
        margin-left: auto;
        font-size: 0.9rem;
        font-weight: bold;
        color: #C0C5D8;
    }

    li:first-child .nav-link::after { content: ''; }

    .nav-link.active::after { color: #FFFFFF; }

    /* --- ESTILOS PARA OS COMPONENTES DAS PÁGINAS --- */
    
    /* Títulos principais das páginas */
    h1 {
        font-weight: 700 !important;
        color: #1E2A3B !important;
        letter-spacing: -0.5px !important;
    }

    /* Estilo para os cards de métricas (KPIs) */
    [data-testid="stMetric"] {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.04);
    }

</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES DAS PÁGINAS ---

def page_dashboard():
    st.title("Dashboard de Saúde | APS")
    st.markdown("Visão consolidada dos principais indicadores de saúde.")

    # --- FILTROS ---
    cols = st.columns([1, 1, 2])
    with cols[0]:
        st.selectbox("Filtrar por Estado", ["Todos", "SP", "RJ", "MG"])
    with cols[1]:
        st.selectbox("Filtrar por Cidade", ["Todas", "São Paulo", "Rio de Janeiro"])

    st.write("---")

    # --- KPIs (Métricas) ---
    kpi_cols = st.columns(4)
    with kpi_cols[0]:
        st.metric(label="Total de Pacientes", value="1.2M", delta="+5.2%")
    with kpi_cols[1]:
        st.metric(label="Leitos Ocupados", value="85.2%", delta="-1.5%", delta_color="inverse")
    with kpi_cols[2]:
        st.metric(label="Médicos Ativos", value="2,458", delta="+12%")
    with kpi_cols[3]:
        st.metric(label="Hospitais Monitorados", value="172")
    
    st.write("---")

    # --- Gráficos ---
    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.subheader("Pacientes por Especialidade (CID-10)")
        chart_data = pd.DataFrame({
            "Especialidade": ["Cardiologia", "Oncologia", "Neurologia", "Ortopedia", "Outros"],
            "Pacientes": [120, 85, 60, 95, 150],
        })
        st.bar_chart(chart_data, x="Especialidade", y="Pacientes")

    with chart_cols[1]:
        st.subheader("Distribuição Geográfica dos Hospitais")
        map_data = pd.DataFrame(
            np.random.randn(100, 2) / [50, 50] + [-23.55, -46.63],
            columns=['lat', 'lon']
        )
        st.map(map_data)

def page_upload():
    st.title("Ingestão e Processamento de Dados 📤")
    st.markdown("Importe os arquivos de dados brutos para a plataforma.")

    cols = st.columns(2)
    with cols[0]:
        st.subheader("Dados Cadastrais")
        st.file_uploader("Hospitais (Excel, JSON)", type=['xlsx', 'json'], accept_multiple_files=True)
        st.file_uploader("Médicos (XML, JSON)", type=['xml', 'json'], accept_multiple_files=True)
        st.file_uploader("Pacientes (CSV, JSON)", type=['csv', 'json'], accept_multiple_files=True)

    with cols[1]:
        st.subheader("Dados de Padrões")
        st.file_uploader("CID-10 (CSV)", type=['csv'])
        st.file_uploader("Estados e Municípios (JSON)", type=['json'])

    st.write("---")
    if st.button("Iniciar Processamento", use_container_width=True, type="primary"):
        with st.spinner('Processando arquivos... Isso pode levar alguns minutos.'):
            time.sleep(5)
        st.success("Processamento concluído com sucesso!")
        with st.expander("Ver Relatório de Processamento"):
            st.write("✅ **Hospitais:** 2 arquivos processados, 1500 registros únicos, 25 duplicados removidos.")
            st.warning("⚠️ **Médicos:** 1 arquivo com erro de formatação (enviado para DLQ). 2 arquivos processados.")
            st.info("Arquivos inválidos foram movidos para a 'Dead Letter Queue' (DLQ) para análise manual.")

def page_alocacao():
    st.title("Alocação Inteligente de Recursos 🧠")
    st.markdown("Execute os algoritmos de alocação de médicos e pacientes nos hospitais.")

    tab_medicos, tab_pacientes = st.tabs(["Alocação de Médicos", "Alocação de Pacientes"])

    with tab_medicos:
        st.header("Alocar Médicos em Hospitais")
        st.markdown("Regras: Máx. 3 hospitais/médico, compatibilidade de especialidade, preferência para mesma cidade (raio máx. 30km).")
        if st.button("Executar Alocação de Médicos", key="medicos", use_container_width=True):
            with st.spinner("Analisando especialidades e geolocalização..."):
                time.sleep(4)
            st.success("Alocação de médicos concluída!")
            df_medicos = pd.DataFrame({'Médico': ['Dr. House', 'Dr. Grey'], 'Hospital Alocado': ['Hospital A', 'Hospital B'], 'Distância (km)': [5, 12]})
            st.dataframe(df_medicos, use_container_width=True)

    with tab_pacientes:
        st.header("Alocar Pacientes em Hospitais")
        st.markdown("Regras: Compatibilidade entre sintoma (CID-10) e especialidade do hospital, preferência pelo hospital mais próximo.")
        if st.button("Executar Alocação de Pacientes", key="pacientes", use_container_width=True):
            with st.spinner("Cruzando dados de CID-10 com especialidades hospitalares..."):
                time.sleep(4)
            st.success("Alocação de pacientes concluída!")
            df_pacientes = pd.DataFrame({'Paciente': ['Fulano', 'Ciclano'], 'CID-10': ['I10', 'C50'], 'Hospital Alocado': ['Hospital A (Cardio)', 'Hospital C (Onco)']})
            st.dataframe(df_pacientes, use_container_width=True)

def page_entidades():
    st.title("Consulta de Entidades Cadastradas 📋")
    st.markdown("Navegue e pesquise pelos dados já consolidados na plataforma.")

    df_hosp = pd.DataFrame({'Nome': ['Hospital A', 'Hospital B'], 'CNPJ': ['11.111.111/0001-11', '22.222.222/0001-22'], 'Cidade': ['São Paulo', 'Rio de Janeiro']})
    df_med = pd.DataFrame({'Nome': ['Dr. House', 'Dr. Grey'], 'CRM': ['12345-SP', '67890-RJ'], 'Especialidade': ['Nefrologia', 'Cirurgia Geral']})

    tab_hosp, tab_medicos, tab_pacientes = st.tabs(["Hospitais", "Médicos", "Pacientes"])

    with tab_hosp:
        st.text_input("Buscar Hospital por Nome ou CNPJ", key="search_hosp")
        st.dataframe(df_hosp, use_container_width=True)

    with tab_medicos:
        st.text_input("Buscar Médico por Nome ou CRM", key="search_med")
        st.dataframe(df_med, use_container_width=True)

    with tab_pacientes:
        st.info("A consulta de dados de pacientes está restrita por políticas de privacidade (LGPD).")

# --- SIDEBAR (SEU CÓDIGO ORIGINAL) ---
with st.sidebar:
    st.markdown(
        """
        <div style="display: flex; align-items: center; margin-bottom: 2rem;">
            <svg width="28" height="28" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M12 2L2 7V17L12 22L22 17V7L12 2Z" stroke="#1E202A" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <h1 style="font-weight:bold; font-size: 24px; margin: 0 0 0 10px; color: #1E202A;">APS
                <span style="font-size: 14px; color: #8A91A8; font-weight: 500;">v.01</span>
            </h1>
        </div>
        """,
        unsafe_allow_html=True
    )

    selected = option_menu(
        menu_title=None,
        options=["Dashboard", "Upload", "Alocação", "Entidades"],
        icons=["clipboard-data", "cloud-upload", "shuffle", "card-list"],
        default_index=0,
        orientation="vertical",
    )

# --- ROTEAMENTO DAS PÁGINAS ---
if selected == "Dashboard":
    page_dashboard()
elif selected == "Upload":
    page_upload()
elif selected == "Alocação":
    page_alocacao()
elif selected == "Entidades":
    page_entidades()