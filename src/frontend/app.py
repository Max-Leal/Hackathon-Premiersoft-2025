import os
from db_utils import fetch_data, execute_query

import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import numpy as np
import time
import random # Adicione esta linha
import pydeck as pdk # Biblioteca para mapas avançados
from db_utils import fetch_data

# --- ATIVAÇÃO DA CHAVE DE API DO MAPBOX ---
# O Streamlit lê o arquivo secrets.toml e disponibiliza as chaves em st.secrets
# Nós pegamos a chave e a definimos como uma variável de ambiente que o Pydeck entende.
if "MAPBOX_API_KEY" in st.secrets:
    os.environ["MAPBOX_API_KEY"] = st.secrets["MAPBOX_API_KEY"]
else:
    st.warning("Chave da API do Mapbox não encontrada. O mapa pode não ser exibido corretamente.")

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
    st.title("Painel de Saúde Estratégico | APS")
    st.markdown("Análise de indicadores operacionais e de capacidade da rede de saúde.")

    # --- SIMULAÇÃO DE DADOS DINÂMICICOS ---
    total_pacientes = 1254320
    medicos_ativos = 2458
    hospitais_monitorados = 172
    cid_data = {
        'cid_descricao': ['I10 - Hipertensão essencial', 'E11 - Diabetes mellitus', 'J44 - DPOC', 'I25 - Doença isquêmica do coração', 'N18 - Doença renal crônica', 'G47 - Distúrbios do sono', 'M54 - Dorsalgia', 'J06 - Infecções agudas'],
        'total_pacientes': np.random.randint(5000, 20000, size=8)
    }
    df_cid = pd.DataFrame(cid_data).sort_values('total_pacientes', ascending=False)
    hospital_data = {
        'nome': [f'Hospital {chr(65+i)}' for i in range(10)],
        'lat': np.random.uniform(-23.50, -23.60, size=10),
        'lon': np.random.uniform(-46.60, -46.70, size=10),
        'leitos_totais': np.random.randint(100, 500, size=10),
        'leitos_ocupados': np.random.randint(50, 480, size=10)
    }
    hospital_data['leitos_ocupados'] = np.minimum(hospital_data['leitos_ocupados'], hospital_data['leitos_totais'] - 10)
    df_hospitais = pd.DataFrame(hospital_data)
    df_hospitais['taxa_ocupacao'] = (df_hospitais['leitos_ocupados'] / df_hospitais['leitos_totais'])
    alocacao_data = {
        'num_hospitais': ['1 Hospital', '2 Hospitais', '3 Hospitais'],
        'total_medicos': [1200, 858, 400]
    }
    df_alocacao_medicos = pd.DataFrame(alocacao_data)

    # --- LAYOUT COM ABAS PARA MELHOR ORGANIZAÇÃO ---
    tab_geral, tab_geo, tab_recursos = st.tabs([
        " Visão Geral ", 
        " Análise Geográfica ", 
        " Recursos e Capacidade "
    ])

    # --- ABA 1: VISÃO GERAL ---
    with tab_geral:
        st.header("Indicadores Chave de Performance (KPIs)")
    
        # --- CORREÇÃO DE LAYOUT: KPIs movidos para dentro da aba ---
        kpi_cols = st.columns(4)
        taxa_ocupacao_geral = df_hospitais['leitos_ocupados'].sum() / df_hospitais['leitos_totais'].sum()
        
        with kpi_cols[0]:
            st.metric(label="Total de Pacientes", value=f"{total_pacientes:,}".replace(",", "."))
        with kpi_cols[1]:
            st.metric(label="Taxa de Ocupação Geral", value=f"{taxa_ocupacao_geral:.1%}", delta="-1.5%", delta_color="inverse")
        with kpi_cols[2]:
            st.metric(label="Médicos Ativos", value=f"{medicos_ativos:,}".replace(",", "."))
        with kpi_cols[3]:
            st.metric(label="Hospitais Monitorados", value=hospitais_monitorados)
        
        st.divider()

        # --- CORREÇÃO DE LAYOUT: Gráficos movidos para dentro da aba ---
        chart_cols = st.columns([2, 2])
        with chart_cols[0]:
            st.subheader("Top 8 Diagnósticos (CID-10)")
            st.markdown("Principais condições que levam os pacientes à rede.")
            st.bar_chart(df_cid, x='cid_descricao', y='total_pacientes', color="#7d53de")
            
        with chart_cols[1]:
            st.subheader("Alocação de Médicos na Rede")
            st.markdown("Distribuição de médicos pelo número de hospitais em que atuam.")
            st.data_editor(
                df_alocacao_medicos,
                column_config={
                    "total_medicos": st.column_config.ProgressColumn(
                        "Total de Médicos", format="%f", min_value=0,
                        max_value=int(df_alocacao_medicos['total_medicos'].max()),
                    ),
                },
                hide_index=True, use_container_width=True
            )
            with st.expander("Ver dados da alocação"):
                st.dataframe(df_alocacao_medicos, use_container_width=True)

    # --- ABA 2: ANÁLISE GEOGRÁFICA ---
    with tab_geo:
        st.header("Distribuição e Ocupação de Hospitais")
        
        min_ocupacao = st.slider(
            "Filtrar por taxa de ocupação mínima (%)", 
            min_value=0, max_value=100, value=20, format="%d%%"
        )
        
        df_filtrado = df_hospitais[df_hospitais['taxa_ocupacao'] >= (min_ocupacao / 100.0)]

        if not df_filtrado.empty:
            view_state = pdk.ViewState(
                latitude=df_filtrado['lat'].mean(),
                longitude=df_filtrado['lon'].mean(),
                zoom=10, pitch=50,
            )
            
            layer = pdk.Layer(
                "ScatterplotLayer", data=df_filtrado, get_position='[lon, lat]',
                get_color='[200, 30, 0, 160]', get_radius='leitos_totais',
                pickable=True, auto_highlight=True
            )
            
            df_filtrado['taxa_ocupacao_formatado'] = df_filtrado['taxa_ocupacao'].apply(lambda x: f"{x:.1%}")
            tooltip = {
                "html": "<b>{nome}</b><br/>Ocupação: {taxa_ocupacao_formatado}<br/>Leitos: {leitos_ocupados}/{leitos_totais}",
                "style": {"backgroundColor": "#7d53de", "color": "white"}
            }

            r = pdk.Deck(
                layers=[layer], initial_view_state=view_state,
                map_style=pdk.map_styles.MAPBOX_LIGHT, # Usa um estilo padrão do Mapbox
                tooltip=tooltip
            )
            st.pydeck_chart(r)
            st.info("Passe o mouse sobre os pontos para ver detalhes. O tamanho do círculo representa a capacidade total de leitos.")
        else:
            st.warning("Nenhum hospital encontrado para o filtro de ocupação selecionado.")

    # --- ABA 3: RECURSOS E CAPACIDADE ---
    with tab_recursos:
        st.header("Análise Detalhada da Capacidade Hospitalar")
        st.markdown("Monitore a ocupação de leitos em cada unidade para otimizar a alocação de pacientes.")
        
        st.dataframe(
            df_hospitais,
            column_config={
                "nome": "Hospital",
                "taxa_ocupacao": st.column_config.ProgressColumn(
                    "Taxa de Ocupação", format="%.1f%%", min_value=0, max_value=1,
                ),
                "lat": None, "lon": None,
            },
            use_container_width=True, hide_index=True
        )

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
    """
    Esta página exibe os relatórios do processo de alocação automática,
    separando os resultados de médicos e pacientes em abas distintas.
    """
    st.title("Relatórios de Alocação Automática 🧠")
    st.markdown(
        "Navegue pelas abas para visualizar os resultados do processo de alocação "
        "gerado pelo sistema."
    )

    # Cria as abas para separar os relatórios
    tab_medicos, tab_pacientes = st.tabs(["Relatório de Médicos", "Relatório de Pacientes"])

    # --- ABA: RELATÓRIO DE ALOCAÇÕES DE MÉDICOS ---
    with tab_medicos:
        st.header("Associações Atuais: Médicos e Hospitais")
        st.markdown("A tabela abaixo mostra todos os médicos alocados, com detalhes sobre seus hospitais e municípios correspondentes.")

        with st.spinner("Carregando relatório de alocação de médicos..."):
            # Consulta SQL para buscar os dados detalhados dos médicos alocados
            query_medicos = """
                SELECT
                    m.nome_completo AS medico,
                    m.especialidade AS especialidade_medico,
                    h.nome AS hospital,
                    h.especialidades AS especialidades_hospital,
                    mun.nome AS municipio_hospital
                FROM
                    medico_hospital_associacao AS mha
                JOIN
                    medicos AS m ON mha.medico_id = m.codigo
                JOIN
                    hospitais AS h ON mha.hospital_id = h.codigo
                JOIN
                    municipios AS mun ON h.municipio_id = mun.codigo_ibge
                ORDER BY
                    m.nome_completo, h.nome;
            """
            
            medicos_alloc_df = fetch_data(query_medicos)

            if not medicos_alloc_df.empty:
                st.dataframe(medicos_alloc_df, use_container_width=True, hide_index=True)
            else:
                st.info("O processo de alocação automática ainda não associou médicos a hospitais.")

    # --- ABA: RELATÓRIO DE ALOCAÇÕES DE PACIENTES ---
    with tab_pacientes:
        st.header("Alocações Atuais: Pacientes e Hospitais")
        st.markdown("A tabela abaixo mostra todos os pacientes que foram alocados a um hospital pelo sistema.")

        with st.spinner("Carregando relatório de alocação de pacientes..."):
            # Consulta SQL para buscar os dados detalhados dos pacientes alocados
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
    st.title("Consulta de Entidades Cadastradas 📋")
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
        st.warning("⚠️ A visualização de dados de pacientes deve seguir as políticas de privacidade (LGPD).")
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
    st.markdown(
        """
        <div style="display: flex; align-items-center; margin-bottom: 2rem;">
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