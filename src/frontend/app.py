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

# --- ERIC ----

@st.cache_data(ttl=600)
def get_medico_alocacao_data():
    """Busca dados sobre a alocação de médicos, contando em quantos hospitais cada um atua."""
    query = """
    WITH contagem_por_medico AS (
        -- Primeiro, conta em quantos hospitais cada médico está associado
        SELECT
            medico_id,
            COUNT(hospital_id) AS num_hospitais
        FROM
            medico_hospital_associacao
        GROUP BY
            medico_id
    )
    -- Agora, agrupa os médicos pela quantidade de hospitais em que atuam
    SELECT
        CASE
            WHEN num_hospitais = 1 THEN '1 Hospital'
            WHEN num_hospitais = 2 THEN '2 Hospitais'
            ELSE '3+ Hospitais' -- Agrupa todos os que atuam em 3 ou mais
        END AS num_hospitais, -- Renomeia a coluna para corresponder ao df original
        COUNT(medico_id) AS total_medicos
    FROM
        contagem_por_medico
    GROUP BY
        num_hospitais
    ORDER BY
        num_hospitais;
    """
    return fetch_data(query)

@st.cache_data(ttl=600) # Cache de 10 minutos para não sobrecarregar o banco
def get_kpi_data():
    """Busca os dados agregados para os Indicadores Chave de Performance (KPIs)."""
    total_pacientes_df = fetch_data("SELECT COUNT(codigo) FROM pacientes;")
    medicos_ativos_df = fetch_data("SELECT COUNT(codigo) FROM medicos;")
    hospitais_monitorados_df = fetch_data("SELECT COUNT(codigo) FROM hospitais;")

    # Extrai o valor numérico de cada DataFrame de uma única célula
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
    """Busca os 8 principais diagnósticos (CID-10) com base no número de pacientes."""
    query = """
    SELECT
        c.codigo || ' - ' || c.descricao AS cid_descricao,
        COUNT(p.codigo) AS total_pacientes
    FROM
        pacientes AS p
    INNER JOIN
        cid10 AS c ON p.cid_10 = c.codigo
    GROUP BY
        c.codigo, c.descricao
    ORDER BY
        total_pacientes DESC
    LIMIT 8;
    """
    return fetch_data(query)

@st.cache_data(ttl=600)
def get_hospital_data():
    """Busca dados detalhados dos hospitais, incluindo geolocalização e ocupação."""
    # Esta query assume que você está usando PostGIS para a coluna 'localizacao'.
    # As funções ST_Y e ST_X extraem latitude e longitude, respectivamente.
    query = """
    WITH pacientes_por_hospital AS (
        SELECT
            hospital_alocado_id,
            COUNT(codigo) as leitos_ocupados
        FROM
            pacientes
        WHERE
            hospital_alocado_id IS NOT NULL
        GROUP BY
            hospital_alocado_id
    )
    SELECT
        h.nome,
        ST_Y(h.localizacao) AS lat, -- Extrai a Latitude
        ST_X(h.localizacao) AS lon, -- Extrai a Longitude
        h.leitos_totais,
        COALESCE(p.leitos_ocupados, 0)::int AS leitos_ocupados -- COALESCE para incluir hospitais sem pacientes
    FROM
        hospitais h
    LEFT JOIN
        pacientes_por_hospital p ON h.codigo = p.hospital_alocado_id;
    """
    df = fetch_data(query)
    
    if not df.empty:
        # Calcula a taxa de ocupação, evitando divisão por zero
        df['taxa_ocupacao'] = np.where(
            df['leitos_totais'] > 0,
            df['leitos_ocupados'] / df['leitos_totais'],
            0
        )
    else:
        # Garante que as colunas existam mesmo se a query não retornar nada
        df['taxa_ocupacao'] = pd.Series(dtype='float64')

    return df

def page_dashboard():
    st.title("Painel de Saúde Estratégico | APS")
    st.markdown("Análise de indicadores operacionais e de capacidade da rede de saúde.")

    # --- CARREGAMENTO DOS DADOS DO BANCO DE DADOS ---
    # Todas as fontes de dados agora vêm do banco de dados.
    kpi_data = get_kpi_data()
    df_cid = get_top_cid_data()
    df_hospitais = get_hospital_data()
    df_alocacao_medicos = get_medico_alocacao_data() # <- MUDANÇA AQUI

    # --- LAYOUT COM ABAS PARA MELHOR ORGANIZAÇÃO ---
    tab_geral, tab_geo, tab_recursos = st.tabs([
        " Visão Geral ", 
        " Análise Geográfica ", 
        " Recursos e Capacidade "
    ])

    # --- ABA 1: VISÃO GERAL ---
    with tab_geral:
        st.header("Indicadores Chave de Performance (KPIs)")
    
        kpi_cols = st.columns(4)
        
        if not df_hospitais.empty and df_hospitais['leitos_totais'].sum() > 0:
            taxa_ocupacao_geral = df_hospitais['leitos_ocupados'].sum() / df_hospitais['leitos_totais'].sum()
        else:
            taxa_ocupacao_geral = 0
        
        with kpi_cols[0]:
            st.metric(label="Total de Pacientes", value=f"{kpi_data.get('total_pacientes', 0):,}".replace(",", "."))
        with kpi_cols[1]:
            st.metric(label="Taxa de Ocupação Geral", value=f"{taxa_ocupacao_geral:.1%}")
        with kpi_cols[2]:
            st.metric(label="Médicos Ativos", value=f"{kpi_data.get('medicos_ativos', 0):,}".replace(",", "."))
        with kpi_cols[3]:
            st.metric(label="Hospitais Monitorados", value=kpi_data.get('hospitais_monitorados', 0))
        
        st.divider()

        chart_cols = st.columns([2, 2])
        with chart_cols[0]:
            st.subheader("Top 8 Diagnósticos (CID-10)")
            st.markdown("Principais condições que levam os pacientes à rede.")
            if not df_cid.empty:
                st.bar_chart(df_cid.set_index('cid_descricao'), y='total_pacientes', color="#7d53de")
            else:
                st.info("Não há dados de diagnósticos para exibir.")
            
        with chart_cols[1]:
            st.subheader("Alocação de Médicos na Rede")
            st.markdown("Distribuição de médicos pelo número de hospitais em que atuam.")
            
            if not df_alocacao_medicos.empty:
                st.data_editor(
                    df_alocacao_medicos,
                    column_config={
                        "num_hospitais": "Atuação",
                        "total_medicos": st.column_config.ProgressColumn(
                            "Total de Médicos", format="%d", min_value=0,
                            max_value=int(df_alocacao_medicos['total_medicos'].max()),
                        ),
                    },
                    hide_index=True, use_container_width=True
                )
                with st.expander("Ver dados da alocação"):
                    st.dataframe(df_alocacao_medicos, use_container_width=True)
            else:
                st.info("Não há dados de alocação de médicos para exibir.")

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
                latitude=df_filtrado['lat'].mean(), longitude=df_filtrado['lon'].mean(),
                zoom=10, pitch=50,
            )
            layer = pdk.Layer(
                "ScatterplotLayer", data=df_filtrado, get_position='[lon, lat]',
                get_color='[200, 30, 0, 160]', get_radius='leitos_totais',
                pickable=True, auto_highlight=True
            )
            df_filtrado = df_filtrado.copy()
            df_filtrado['taxa_ocupacao_formatado'] = df_filtrado['taxa_ocupacao'].apply(lambda x: f"{x:.1%}")
            tooltip = {
                "html": "<b>{nome}</b><br/>Ocupação: {taxa_ocupacao_formatado}<br/>Leitos: {leitos_ocupados}/{leitos_totais}",
                "style": {"backgroundColor": "#7d53de", "color": "white"}
            }
            r = pdk.Deck(
                layers=[layer], initial_view_state=view_state,
                map_style=pdk.map_styles.MAPBOX_LIGHT, tooltip=tooltip
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
                "leitos_totais": "Leitos Totais",
                "leitos_ocupados": "Leitos Ocupados",
                "taxa_ocupacao": st.column_config.ProgressColumn(
                    "Taxa de Ocupação", format="%.1f%%", min_value=0, max_value=1,
                ),
                "lat": None, "lon": None,
            },
            use_container_width=True, hide_index=True
        )

# --- FINAL ERIC ---

def page_upload():
    st.title("Ingestão e Processamento de Dados")
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
    st.title("Alocação Inteligente de Recursos")
    st.markdown("Execute os algoritmos de alocação de médicos e pacientes nos hospitais.")

    tab_medicos, tab_pacientes = st.tabs(["Alocação de Médicos", "Alocação de Pacientes"])

    # --- ABA DE ALOCAÇÃO DE MÉDICOS ---
    with tab_medicos:
        st.header("Gerenciar Alocação de Médicos")
        
        # --- Carregar dados para os selects ---
        medicos_df = fetch_data("SELECT codigo, nome_completo FROM medicos ORDER BY nome_completo")
        hospitais_df = fetch_data("SELECT codigo, nome FROM hospitais ORDER BY nome")
        
        # Carrega as alocações existentes para a desalocação
        alocacoes_query = """
            SELECT mha.medico_id, mha.hospital_id, m.nome_completo, h.nome AS nome_hospital
            FROM medico_hospital_associacao mha
            JOIN medicos m ON m.codigo = mha.medico_id
            JOIN hospitais h ON h.codigo = mha.hospital_id
            ORDER BY m.nome_completo;
        """
        alocacoes_df = fetch_data(alocacoes_query)

        col1, col2 = st.columns(2, gap="large")

        # --- COLUNA PARA ALOCAR UM NOVO MÉDICO ---
        with col1:
            st.subheader("Alocar Médico")
            
            # Usar .to_dict('records') para que o selectbox retorne o dicionário da linha inteira
            medico_selecionado = st.selectbox(
                "Selecione o Médico", 
                options=medicos_df.to_dict('records'), 
                format_func=lambda row: row['nome_completo'],
                key="sel_medico"
            )
            hospital_selecionado = st.selectbox(
                "Selecione o Hospital para alocar",
                options=hospitais_df.to_dict('records'),
                format_func=lambda row: row['nome'],
                key="sel_hosp_med"
            )

            if st.button("Alocar Médico", use_container_width=True, type="primary"):
                if medico_selecionado and hospital_selecionado:
                    medico_id = medico_selecionado['codigo']
                    hospital_id = hospital_selecionado['codigo']

                    # Regra 1: Verificar se o médico já está em 3 hospitais
                    count_query = f"SELECT COUNT(*) FROM medico_hospital_associacao WHERE medico_id = '{medico_id}';"
                    count_df = fetch_data(count_query)
                    
                    if count_df.iloc[0,0] >= 3:
                        st.error(f"O(a) médico(a) {medico_selecionado['nome_completo']} já está alocado(a) em 3 hospitais.")
                    else:
                        # Regra 2: Simular verificação de raio de 30km
                        distancia_simulada = random.randint(1, 50) # Simula uma distância
                        if distancia_simulada > 30:
                            st.warning(f"Simulação: Hospital a {distancia_simulada}km, fora do raio de 30km. Alocação não permitida.")
                        else:
                            st.info(f"Simulação: Hospital a {distancia_simulada}km (dentro do raio).")
                            # Executar a alocação
                            insert_query = f"INSERT INTO medico_hospital_associacao (medico_id, hospital_id) VALUES ('{medico_id}', '{hospital_id}') ON CONFLICT DO NOTHING;"
                            if execute_query(insert_query):
                                st.success(f"Médico(a) {medico_selecionado['nome_completo']} alocado(a) com sucesso ao {hospital_selecionado['nome']}!")
                                st.rerun() # Recarrega a página para atualizar as listas

        # --- COLUNA PARA DESALOCAR UM MÉDICO ---
        with col2:
            st.subheader("Desalocar Médico")

            if not alocacoes_df.empty:
                alocacao_para_remover = st.selectbox(
                    "Selecione a alocação para remover",
                    options=alocacoes_df.to_dict('records'),
                    format_func=lambda rec: f"{rec['nome_completo']} @ {rec['nome_hospital']}",
                    key="sel_desalocar"
                )

                if st.button("Desalocar Médico", use_container_width=True):
                    medico_id = alocacao_para_remover['medico_id']
                    hospital_id = alocacao_para_remover['hospital_id']
                    
                    delete_query = f"DELETE FROM medico_hospital_associacao WHERE medico_id = '{medico_id}' AND hospital_id = '{hospital_id}';"
                    if execute_query(delete_query):
                        st.success("Alocação removida com sucesso!")
                        st.rerun()
            else:
                st.info("Nenhuma alocação de médico para remover.")

    # --- ABA DE ALOCAÇÃO DE PACIENTES ---
    with tab_pacientes:
        st.header("Gerenciar Alocação de Pacientes")
        
        # Carregar pacientes (apenas os não alocados para alocação, e os alocados para desalocação)
        pacientes_nao_alocados_df = fetch_data("SELECT codigo, nome_completo FROM pacientes WHERE hospital_alocado_id IS NULL ORDER BY nome_completo")
        pacientes_alocados_df = fetch_data("""
            SELECT p.codigo, p.nome_completo, h.nome AS nome_hospital
            FROM pacientes p JOIN hospitais h ON p.hospital_alocado_id = h.codigo
            ORDER BY p.nome_completo
        """)

        col3, col4 = st.columns(2, gap="large")

        # --- COLUNA PARA ALOCAR PACIENTE ---
        with col3:
            st.subheader("Alocar Paciente")
            
            if not pacientes_nao_alocados_df.empty:
                paciente_selecionado = st.selectbox(
                    "Selecione o Paciente",
                    options=pacientes_nao_alocados_df.to_dict('records'),
                    format_func=lambda row: row['nome_completo'],
                    key="sel_paciente"
                )
                hospital_para_paciente = st.selectbox(
                    "Selecione o Hospital",
                    options=hospitais_df.to_dict('records'),
                    format_func=lambda row: row['nome'],
                    key="sel_hosp_pac"
                )

                # Selectbox dinâmico para médicos do hospital selecionado
                if hospital_para_paciente:
                    hosp_id = hospital_para_paciente['codigo']
                    medicos_do_hospital_df = fetch_data(f"""
                        SELECT m.codigo, m.nome_completo
                        FROM medicos m
                        JOIN medico_hospital_associacao mha ON m.codigo = mha.medico_id
                        WHERE mha.hospital_id = '{hosp_id}' ORDER BY m.nome_completo;
                    """)
                    
                    medico_para_paciente = st.selectbox(
                        "Selecione o Médico para o paciente",
                        options=medicos_do_hospital_df.to_dict('records'),
                        format_func=lambda row: row['nome_completo'],
                        key="sel_med_pac"
                    )

                if st.button("Alocar Paciente", use_container_width=True, type="primary"):
                    if paciente_selecionado and hospital_para_paciente:
                        pac_id = paciente_selecionado['codigo']
                        hosp_id = hospital_para_paciente['codigo']
                        
                        # Aloca apenas ao hospital (schema atual)
                        update_query = f"UPDATE pacientes SET hospital_alocado_id = '{hosp_id}' WHERE codigo = '{pac_id}';"
                        if execute_query(update_query):
                            st.success(f"Paciente {paciente_selecionado['nome_completo']} alocado(a) ao {hospital_para_paciente['nome']} com sucesso!")
                            # Futuramente, você poderia salvar o médico alocado em outra tabela ou coluna
                            st.rerun()
            else:
                st.info("Todos os pacientes já estão alocados.")

        # --- COLUNA PARA DESALOCAR PACIENTE ---
        with col4:
            st.subheader("Desalocar Paciente")
            
            if not pacientes_alocados_df.empty:
                paciente_para_remover = st.selectbox(
                    "Selecione o paciente alocado para remover",
                    options=pacientes_alocados_df.to_dict('records'),
                    format_func=lambda rec: f"{rec['nome_completo']} @ {rec['nome_hospital']}",
                    key="sel_desalocar_pac"
                )
                if st.button("Desalocar Paciente", use_container_width=True):
                    pac_id = paciente_para_remover['codigo']
                    # A desalocação é setar o campo para NULL
                    update_query = f"UPDATE pacientes SET hospital_alocado_id = NULL WHERE codigo = '{pac_id}';"
                    if execute_query(update_query):
                        st.success(f"Paciente {paciente_para_remover['nome_completo']} desalocado com sucesso.")
                        st.rerun()
            else:
                st.info("Nenhum paciente alocado para remover.")

def page_entidades():
    st.title("Consulta de Entidades Cadastradas")
    st.markdown("Navegue e pesquise pelos dados já consolidados na plataforma.")

    # Cria as abas de navegação
    tab_hosp, tab_medicos, tab_pacientes = st.tabs(["Hospitais", "Médicos", "Pacientes"])

    # --- ABA DE HOSPITAIS (CONECTADA AO BANCO) ---
    with tab_hosp:
        st.subheader("Hospitais Cadastrados")
        
        # Filtro de busca
        search_term = st.text_input("Buscar Hospital por Nome", key="search_hosp")

        # Mostra um spinner enquanto os dados são carregados
        with st.spinner("Carregando dados dos hospitais..."):
            
            # --- CONSULTA SQL ATUALIZADA COM TODAS AS COLUNAS CORRETAS ---
            query = "SELECT codigo, nome, cidade, bairro, especialidades, leitos_totais FROM hospitais"
            
            if search_term:
                # Adiciona um filtro WHERE se algo for digitado na busca
                query += f" WHERE nome ILIKE '%{search_term.replace('%', '%%')}%'"
            
            query += " ORDER BY nome;"

            # Usa a função para buscar os dados do banco
            df_hosp = fetch_data(query)
            
            # Exibe o DataFrame se não estiver vazio
            if not df_hosp.empty:
                st.dataframe(df_hosp, use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum hospital encontrado no banco de dados com o filtro atual.")

    # --- ABA DE MÉDICOS (DADOS ESTÁTICOS, COMO ANTES) ---
    with tab_medicos:
        st.subheader("Médicos Cadastrados")
        st.text_input("Buscar Médico por Nome ou CRM", key="search_med")
        
        df_med = pd.DataFrame({
            'Nome': ['Dr. House', 'Dr. Grey'], 
            'CRM': ['12345-SP', '67890-RJ'], 
            'Especialidade': ['Nefrologia', 'Cirurgia Geral']
        })
        st.dataframe(df_med, use_container_width=True)

    # --- ABA DE PACIENTES (MENSAGEM DE PRIVACIDADE, COMO ANTES) ---
    with tab_pacientes:
        st.info("A consulta de dados de pacientes está restrita por políticas de privacidade (LGPD).")


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