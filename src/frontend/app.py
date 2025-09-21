import os
from db_utils import fetch_data, execute_query

import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import numpy as np
import time
import random # Adicione esta linha
import pydeck as pdk # Biblioteca para mapas avançados
import plotly.express as px
from db_utils import fetch_data

import base64

def load_svg(image_file):
    """Lê um arquivo de imagem e o retorna como uma string de imagem em Base64."""
    try:
        with open(image_file, "rb") as f:
            img_bytes = f.read()
        b64_string = base64.b64encode(img_bytes).decode("utf-8")
        # Identifica a extensão para o mime type correto
        extension = image_file.split('.')[-1].lower()
        if extension == 'svg':
            return f"data:image/svg+xml;base64,{b64_string}"
        else:
            return f"data:image/{extension};base64,{b64_string}"
    except FileNotFoundError:
        st.error(f"Arquivo do logo não encontrado em: {image_file}")
        return ""

# --- ATIVAÇÃO DA CHAVE DE API DO MAPBOX ---
if "MAPBOX_API_KEY" in st.secrets:
    os.environ["MAPBOX_API_KEY"] = st.secrets["MAPBOX_API_KEY"]
else:
    st.warning("Chave da API do Mapbox não encontrada. O mapa pode não ser exibido corretamente.")

# --- CONFIGURAções DA PÁGINA ---
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
    [data-testid="stSidebar"] {
    box-shadow: 5px 0px 20px -5px rgba(0, 0, 0, 0.2);
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
    .nav-link-icon { font-size: 1.25rem; }
    .nav-link:hover {
        background-color: #F0F2F6;
        color: #1E202A;
    }
    .nav-link:hover .nav-link-icon { color: #1E202A; }
    .nav-link.active, .nav-link.active:hover {
        font-weight: 500; 
        color: #FFFFFF !important;
    }
    .nav-link.active .nav-link-icon { color: #FFFFFF !important; }
    .nav-link::after {
        content: '>';
        margin-left: auto;
        font-size: 0.9rem;
        font-weight: bold;
        color: #C0C5D8;
    }
    li:first-child .nav-link::after { content: ''; }
    .nav-link.active::after { color: #FFFFFF; }
    h1 {
        font-weight: 700 !important;
        color: #1E2A3B !important;
        letter-spacing: -0.5px !important;
    }
    [data-testid="stMetric"] {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.04);
    }
            [data-testid="stMetric"]:hover {
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        transform: translateY(-2px);
        transition: all 0.2s ease-in-out;
        border-color: #76BFAC;
    }
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES PARA BUSCAR DADOS DO DASHBOARD (COM CACHE E TRATAMENTO DE ERROS) ---

@st.cache_data(ttl=600)
def get_kpi_data():
    """Busca os dados agregados para os KPIs de forma segura."""
    total_pacientes_df = fetch_data("SELECT COUNT(codigo) FROM pacientes;")
    medicos_ativos_df = fetch_data("SELECT COUNT(codigo) FROM medicos;")
    hospitais_monitorados_df = fetch_data("SELECT COUNT(codigo) FROM hospitais;")

    total_pacientes = total_pacientes_df.iloc[0, 0] if not total_pacientes_df.empty else 0
    medicos_ativos = medicos_ativos_df.iloc[0, 0] if not medicos_ativos_df.empty else 0
    hospitais_monitorados = hospitais_monitorados_df.iloc[0, 0] if not hospitais_monitorados_df.empty else 0
    
    return {
        "total_pacientes": total_pacientes,
        "medicos_ativos": medicos_ativos,
        "hospitais_monitorados": hospitais_monitorados
    }

@st.cache_data(ttl=600)
def get_top_cid_data():
    """Busca os 8 principais diagnósticos (CID-10)."""
    query = """
    SELECT c.codigo || ' - ' || c.descricao AS cid_descricao, COUNT(p.codigo) AS total_pacientes
    FROM pacientes AS p
    INNER JOIN cid10 AS c ON p.cid_10 = c.codigo
    GROUP BY c.codigo, c.descricao ORDER BY total_pacientes DESC LIMIT 8;
    """
    return fetch_data(query)

@st.cache_data(ttl=600)
def get_hospital_data():
    """Busca dados detalhados dos hospitais de forma segura."""
    query = """
    WITH pacientes_por_hospital AS (
        SELECT hospital_alocado_id, COUNT(codigo) as leitos_ocupados
        FROM pacientes WHERE hospital_alocado_id IS NOT NULL GROUP BY hospital_alocado_id
    )
    SELECT h.nome, ST_Y(h.localizacao) AS lat, ST_X(h.localizacao) AS lon, h.leitos_totais,
           COALESCE(p.leitos_ocupados, 0)::int AS leitos_ocupados
    FROM hospitais h
    LEFT JOIN pacientes_por_hospital p ON h.codigo = p.hospital_alocado_id;
    """
    df = fetch_data(query)
    
    if not df.empty and 'leitos_totais' in df.columns and 'leitos_ocupados' in df.columns:
        df['leitos_totais'] = pd.to_numeric(df['leitos_totais'], errors='coerce').fillna(0)
        df['leitos_ocupados'] = pd.to_numeric(df['leitos_ocupados'], errors='coerce').fillna(0)
        df['taxa_ocupacao'] = np.where(df['leitos_totais'] > 0, df['leitos_ocupados'] / df['leitos_totais'], 0)
    else:
        df = pd.DataFrame(columns=['nome', 'lat', 'lon', 'leitos_totais', 'leitos_ocupados', 'taxa_ocupacao'])
    return df

@st.cache_data(ttl=600)
def get_medico_alocacao_data():
    """Busca dados sobre a alocação de médicos."""
    query = """
    WITH contagem_por_medico AS (
        SELECT medico_id, COUNT(hospital_id) AS num_hospitais
        FROM medico_hospital_associacao GROUP BY medico_id
    )
    SELECT CASE WHEN num_hospitais = 1 THEN '1 Hospital' WHEN num_hospitais = 2 THEN '2 Hospitais' ELSE '3+ Hospitais' END AS num_hospitais,
           COUNT(medico_id) AS total_medicos
    FROM contagem_por_medico GROUP BY num_hospitais ORDER BY num_hospitais;
    """
    return fetch_data(query)

# --- FUNÇÕES DE BUSCA DE DADOS (COM CORREÇÃO NOS KPIs) ---
@st.cache_data(ttl=10) 
def get_dashboard_data():
    """Busca todos os dados agregados necessários para o dashboard de forma segura."""
    def get_count(table_name):
        df = fetch_data(f"SELECT COUNT(codigo) FROM {table_name};")
        if df is not None and not df.empty:
            return df.iloc[0, 0]
        return 0

    total_pacientes = get_count("pacientes")
    total_medicos = get_count("medicos")
    total_hospitais = get_count("hospitais")
    
    genero_df = fetch_data("SELECT genero, COUNT(codigo) AS total FROM pacientes GROUP BY genero;")
    convenio_df = fetch_data("SELECT convenio, COUNT(codigo) AS total FROM pacientes GROUP BY convenio;")
    top_cid_df = fetch_data("""
        SELECT
            c.codigo || ' - ' || c.descricao AS cid_completo,
            COUNT(p.codigo) AS total_pacientes
        FROM pacientes AS p
        JOIN cid10 AS c ON p.cid_10 = c.codigo
        GROUP BY c.codigo, c.descricao
        ORDER BY total_pacientes DESC
        LIMIT 10;
    """)
    
    return {
        "total_pacientes": total_pacientes, "total_medicos": total_medicos,
        "total_hospitais": total_hospitais, "genero_df": genero_df,
        "convenio_df": convenio_df, "top_cid_df": top_cid_df
    }

@st.cache_data(ttl=600)
def get_hospital_geo_data():
    """Busca dados geográficos e de capacidade dos hospitais."""
    query = "SELECT nome, ST_Y(localizacao) AS lat, ST_X(localizacao) AS lon, leitos_totais FROM hospitais WHERE localizacao IS NOT NULL;"
    return fetch_data(query)

# --- FUNÇÕES DAS PÁGINAS ---

def page_dashboard():
    """
    Dashboard que chama as funções de busca de dados GLOBAIS.
    """
    st.title("Painel de Saúde Estratégico | APS")
    st.markdown("Análise de indicadores operacionais e de capacidade da rede de saúde.")

    # Chama as funções de busca que estão FORA desta função
    dashboard_data = get_dashboard_data()
    df_hospitais = get_hospital_geo_data()

    tab_geral, tab_geo, tab_recursos = st.tabs([" Visão Geral ", " Análise Geográfica ", " Capacidade da Rede "])

    # O resto da função de renderização continua igual...
    with tab_geral:
        st.header("Indicadores Chave de Performance (KPIs)")
        kpi_cols = st.columns(4)
        
        kpi_cols[0].metric("Total de Pacientes", f"{dashboard_data.get('total_pacientes', 0):,}".replace(",", "."))
        kpi_cols[1].metric("Médicos Ativos", f"{dashboard_data.get('total_medicos', 0):,}".replace(",", "."))
        kpi_cols[2].metric("Hospitais Monitorados", dashboard_data.get('total_hospitais', 0))
        
        convenio_data = dashboard_data.get('convenio_df', pd.DataFrame())
        total_pacientes_kpi = dashboard_data.get('total_pacientes', 0)
        if not convenio_data.empty and total_pacientes_kpi > 0:
            total_com_convenio = convenio_data.loc[convenio_data['convenio'] == True, 'total'].sum()
            percent_convenio = total_com_convenio / total_pacientes_kpi
            kpi_cols[3].metric("Pacientes com Convênio", f"{percent_convenio:.1%}")
        else:
            kpi_cols[3].metric("Pacientes com Convênio", "N/A")
            
        st.divider()
        chart_cols = st.columns(2)

        with chart_cols[0]:
            st.subheader("Top 10 Diagnósticos (CID-10)")
            df_cid = dashboard_data.get('top_cid_df', pd.DataFrame())
            if not df_cid.empty:
                fig = px.bar(df_cid, y='cid_completo', x='total_pacientes', orientation='h', 
                             labels={'cid_completo': 'Diagnóstico (CID-10)', 'total_pacientes': 'Nº de Pacientes'}, text_auto=True, title="Diagnósticos Mais Frequentes")
                fig.update_layout(yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Não há dados de diagnósticos para exibir.")
            
        with chart_cols[1]:
            st.subheader("Perfil dos Pacientes")
            genero_df = dashboard_data.get('genero_df', pd.DataFrame())
            if not genero_df.empty:
                genero_df['genero'] = genero_df['genero'].map({'M': 'Masculino', 'F': 'Feminino'}).fillna('Não especificado')
                fig_genero = px.pie(genero_df, names='genero', values='total', title='Distribuição por Gênero', hole=0.4)
                st.plotly_chart(fig_genero, use_container_width=True)
            else:
                st.info("Não há dados de gênero para exibir.")

    with tab_geo:
        st.header("Distribuição Geográfica de Hospitais")
        if not df_hospitais.empty:
            view_state = pdk.ViewState(latitude=df_hospitais['lat'].mean(), longitude=df_hospitais['lon'].mean(), zoom=9, pitch=50)
            layer = pdk.Layer("ScatterplotLayer", data=df_hospitais, get_position='[lon, lat]', get_color='[200, 30, 0, 160]', get_radius='leitos_totais * 2', pickable=True, auto_highlight=True)
            tooltip = {"html": "<b>{nome}</b><br/>Leitos Totais: {leitos_totais}", "style": {"backgroundColor": "#333", "color": "white"}}
            try:
                r = pdk.Deck(layers=[layer], initial_view_state=view_state, map_style=pdk.map_styles.MAPBOX_LIGHT, tooltip=tooltip)
                st.pydeck_chart(r)
                st.info("Passe o mouse sobre os pontos para ver detalhes. O tamanho do círculo representa a capacidade total de leitos.")
            except Exception as e:
                st.error(f"Erro ao renderizar o mapa: {e}. Verifique a chave da API do Mapbox.")
        else:
            st.warning("Não há hospitais com dados de geolocalização para exibir no mapa.")

    with tab_recursos:
        st.header("Capacidade da Rede Hospitalar")
        if not df_hospitais.empty:
            st.dataframe(df_hospitais, column_config={"nome": "Hospital", "leitos_totais": st.column_config.NumberColumn("Leitos Totais", format="%d"), "lat": None, "lon": None}, use_container_width=True, hide_index=True)
        else:
            st.info("Não há dados de hospitais para exibir.")

# --- FINAL ERIC ---

def page_upload():
    """
    Página funcional para upload de arquivos, salvamento no diretório raw
    e execução do pipeline de ETL. Todos os uploaders aceitam os 4 tipos de arquivo.
    """
    st.title("Ingestão e Processamento de Dados")
    st.markdown("Importe os arquivos de dados brutos para a plataforma. Os arquivos serão salvos em `data/raw/`.")

    # Define a lista de tipos de arquivo permitidos para reutilização
    ALLOWED_FILE_TYPES = ['xlsx', 'xml', 'json', 'hl7']

    # --- Lógica de Caminhos ---
    try:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
        RAW_DATA_PATH = os.path.join(project_root, "data", "raw")
        PIPELINE_SCRIPT_PATH = os.path.join(project_root, "src", "pipeline", "main.py")
        os.makedirs(RAW_DATA_PATH, exist_ok=True)
    except Exception as e:
        st.error(f"Erro ao configurar os caminhos do projeto: {e}")
        st.info("Certifique-se de que a estrutura de pastas 'data/raw' e 'src/pipeline' existe.")
        return

    # --- Interface de Upload ---
    cols = st.columns(2)
    with cols[0]:
        st.subheader("Dados Cadastrais")
        hospitais_files = st.file_uploader("Hospitais", type=ALLOWED_FILE_TYPES, accept_multiple_files=True)
        medicos_files = st.file_uploader("Médicos", type=ALLOWED_FILE_TYPES, accept_multiple_files=True)
        pacientes_files = st.file_uploader("Pacientes", type=ALLOWED_FILE_TYPES, accept_multiple_files=True)

    with cols[1]:
        st.subheader("Dados de Padrões")
        cid_files = st.file_uploader("CID-10", type=ALLOWED_FILE_TYPES, accept_multiple_files=True)
        estados_files = st.file_uploader("Estados", type=ALLOWED_FILE_TYPES, accept_multiple_files=True)
        municipios_files = st.file_uploader("Municípios", type=ALLOWED_FILE_TYPES, accept_multiple_files=True)

    st.write("---")

    # --- Lógica de Execução ---
    if st.button("Iniciar Processamento", use_container_width=True, type="primary"):
        # Agrupa todos os arquivos em uma única lista
        all_files = (hospitais_files + medicos_files + pacientes_files + 
                     cid_files + estados_files + municipios_files)

        if not all_files:
            st.warning("Nenhum arquivo foi selecionado para upload.")
            return

        # 1. Salvar os arquivos
        progress_bar = st.progress(0, text="Salvando arquivos...")
        for i, uploaded_file in enumerate(all_files):
            dest_path = os.path.join(RAW_DATA_PATH, uploaded_file.name)
            try:
                with open(dest_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
            except Exception as e:
                st.error(f"Erro ao salvar o arquivo {uploaded_file.name}: {e}")
            progress_bar.progress((i + 1) / len(all_files), text=f"Salvando {uploaded_file.name}...")
        
        progress_bar.empty()
        st.success(f"**{len(all_files)} arquivo(s)** salvo(s) com sucesso na pasta `data/raw/`.")

        # 2. Executar o pipeline de ETL
        with st.spinner("Executando o pipeline de ETL... Isso pode levar alguns minutos."):
            try:
                command = ["python", PIPELINE_SCRIPT_PATH]
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    cwd=project_root,
                    check=True
                )

                st.success("**Pipeline de ETL concluído com sucesso!**")
                with st.expander("Ver Relatório de Processamento (Logs do Pipeline)"):
                    st.code(result.stdout, language='log')

            except subprocess.CalledProcessError as e:
                st.error("**Ocorreu um erro durante a execução do pipeline.**")
                with st.expander("Ver Detalhes do Erro"):
                    st.code(e.stderr, language='log')
            except FileNotFoundError:
                st.error(f"Erro: O script do pipeline não foi encontrado em '{PIPELINE_SCRIPT_PATH}'.")
            except Exception as e:
                st.error(f"Um erro inesperado ocorreu: {e}")

def page_alocacao():
    """
    Esta página exibe os relatórios do processo de alocação automática,
    separando os resultados de médicos e pacientes em abas distintas.
    """
    st.title("Relatórios de Alocação Automática")
    st.markdown(
        "Navegue pelas abas para visualizar os resultados do processo de alocação "
        "gerado pelo sistema."
    )

    # Cria as abas para separar os relatórios
    tab_medicos, tab_pacientes = st.tabs(["Relatório de Médicos", "Relatório de Pacientes"])

    # --- ABA: RELATÓRIO DE ALOCAÇÕES DE MÉDICOS (ATUALIZADA) ---
    with tab_medicos:
        st.header("Associações Atuais: Médicos e Hospitais")
        st.markdown("A tabela abaixo mostra todos os médicos alocados, com detalhes sobre onde moram e onde trabalham.")

        with st.spinner("Carregando relatório de alocação de médicos..."):
            # --- CONSULTA ATUALIZADA ---
            query_medicos = """
                SELECT
                    m.nome_completo AS medico,
                    m.especialidade AS especialidade_medico,
                    mun_medico.nome AS municipio_medico,
                    h.nome AS hospital,
                    h.especialidades AS especialidades_hospital,
                    mun_hospital.nome AS municipio_hospital
                FROM
                    medico_hospital_associacao AS mha
                JOIN
                    medicos AS m ON mha.medico_id = m.codigo
                JOIN
                    hospitais AS h ON mha.hospital_id = h.codigo
                JOIN
                    municipios AS mun_hospital ON h.municipio_id = mun_hospital.codigo_ibge
                JOIN
                    municipios AS mun_medico ON m.municipio_id = mun_medico.codigo_ibge
                ORDER BY
                    m.nome_completo, h.nome;
            """
            
            medicos_alloc_df = fetch_data(query_medicos)

            if not medicos_alloc_df.empty:
                st.dataframe(medicos_alloc_df, use_container_width=True, hide_index=True)
            else:
                st.info("O processo de alocação automática ainda não associou médicos a hospitais.")

    # --- ABA: RELATÓRIO DE ALOCAÇÕES DE PACIENTES (SEM ALTERAÇÃO) ---
    with tab_pacientes:
        st.header("Alocações Atuais: Pacientes e Hospitais")
        st.markdown("A tabela abaixo mostra todos os pacientes que foram alocados a um hospital pelo sistema.")

        with st.spinner("Carregando relatório de alocação de pacientes..."):
            query_pacientes = """
                SELECT
                    p.nome_completo AS paciente,
                    p.cpf,
                    p.cid_10,
                    h.nome AS hospital_alocado,
                    mun.nome AS municipio_hospital
                FROM
                    pacientes AS p
                JOIN
                    hospitais AS h ON p.hospital_alocado_id = h.codigo
                JOIN
                    municipios AS mun ON h.municipio_id = mun.codigo_ibge
                WHERE
                    p.hospital_alocado_id IS NOT NULL
                ORDER BY
                    p.nome_completo;
            """
            
            pacientes_alloc_df = fetch_data(query_pacientes)

            if not pacientes_alloc_df.empty:
                st.dataframe(pacientes_alloc_df, use_container_width=True, hide_index=True)
            else:
                st.info("O processo de alocação automática ainda não associou pacientes a hospitais.")

def page_entidades():
    """
    Página principal para exibir todas as entidades do banco de dados.
    Utiliza uma função auxiliar para renderizar cada tabela, evitando repetição de código.
    """

    # --- Função Auxiliar Genérica ---
    def display_table_data(title, table_name, select_clause, search_columns, search_label, order_by_column, key):
        """
        Renderiza uma subseção completa para exibir dados de uma tabela.
        
        Args:
            title (str): O título da subseção (ex: "Hospitais Cadastrados").
            table_name (str): O nome da tabela no banco de dados.
            select_clause (str): A parte "SELECT ..." da consulta SQL.
            search_columns (list): Lista de colunas para usar no filtro de busca (ex: ["nome", "cpf"]).
            search_label (str): O rótulo para a caixa de busca.
            order_by_column (str): A coluna para ordenar os resultados.
            key (str): Uma chave única para o widget st.text_input.
        """
        st.subheader(title)
        search_term = st.text_input(search_label, key=key)

        with st.spinner(f"Carregando dados de {table_name}..."):
            # Constrói a consulta base
            query = f"SELECT {select_clause} FROM {table_name}"

            # Adiciona o filtro de busca se um termo for inserido
            if search_term:
                # Cria uma condição ILIKE para cada coluna de busca
                conditions = [f"{col} ILIKE '%{search_term.replace('%', '%%')}%'" for col in search_columns]
                # Junta as condições com OR
                query += f" WHERE {' OR '.join(conditions)}"
            
            # Adiciona a ordenação
            query += f" ORDER BY {order_by_column};"

            # Busca os dados
            df = fetch_data(query)

            # Exibe o resultado
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info(f"Nenhum registro encontrado em '{table_name}' com o filtro atual.")

    # --- Layout Principal da Página ---
    st.title("Consulta de Entidades Cadastradas")
    st.markdown("Navegue e pesquise pelos dados já consolidados na plataforma.")

    # Define as abas
    tabs = st.tabs(["Hospitais", "Médicos", "Pacientes", "Estados", "Municípios"])

    # --- Aba de Hospitais ---
    with tabs[0]:
        display_table_data(
            title="Hospitais Cadastrados",
            table_name="hospitais",
            select_clause="codigo, nome, municipio_id, especialidades, leitos_totais, ST_AsText(localizacao) AS localizacao",
            search_columns=["nome"],
            search_label="Buscar Hospital por Nome",
            order_by_column="nome",
            key="search_hosp"
        )

    # --- Aba de Médicos ---
    with tabs[1]:
        display_table_data(
            title="Médicos Cadastrados",
            table_name="medicos",
            select_clause="codigo, nome_completo, especialidade, municipio_id",
            search_columns=["nome_completo", "especialidade"],
            search_label="Buscar Médico por Nome ou Especialidade",
            order_by_column="nome_completo",
            key="search_med"
        )

    # --- Aba de Pacientes ---
    with tabs[2]:
        st.warning("A visualização de dados de pacientes deve seguir as políticas de privacidade (LGPD).")
        display_table_data(
            title="Pacientes Cadastrados",
            table_name="pacientes",
            select_clause="codigo, cpf, nome_completo, genero, cod_municipio, bairro, convenio, cid_10, hospital_alocado_id",
            search_columns=["nome_completo", "cpf"],
            search_label="Buscar Paciente por Nome ou CPF",
            order_by_column="nome_completo",
            key="search_pac"
        )

    # --- Aba de Estados ---
    with tabs[3]:
        display_table_data(
            title="Estados (UF)",
            table_name="estados",
            select_clause="codigo_uf, uf, nome",
            search_columns=["nome", "uf"],
            search_label="Buscar Estado por Nome ou Sigla",
            order_by_column="nome",
            key="search_est"
        )

    # --- Aba de Municípios ---
    with tabs[4]:
        display_table_data(
            title="Municípios",
            table_name="municipios",
            select_clause="codigo_ibge, nome, codigo_uf, ST_AsText(localizacao) AS localizacao",
            search_columns=["nome"],
            search_label="Buscar Município por Nome",
            order_by_column="nome",
            key="search_mun"
        )


# --- SIDEBAR (SEU CÓDIO ORIGINAL) ---
with st.sidebar:
    # Define o caminho para o seu arquivo de logo
    LOGO_FILE = "assets/premiersoft_icon.svg"
    
    # Chama a função para carregar e codificar o SVG
    logo_b64_string = load_svg(LOGO_FILE)

    # Usa o resultado em uma tag <img>
    st.markdown(
        f"""
        <div style="display: flex; align-items: center; margin-bottom: 2rem;">
            <img src="{logo_b64_string}" width="64" height="64" />
            <h1 style="font-weight:bold; font-size: 24px; margin: 0 0 0 10px; color: #1E2A3B;">APS
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