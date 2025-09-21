import psycopg2
import math

# Sua função de conexão
# ...

def get_data():
    conn = None
    try:
        # CONEXÃO com o banco de dados (use as suas credenciais)
        conn = psycopg2.connect(
            host='localhost', database='aps_health_data',
            user='admin', password='password123', port='5432'
        )
        cur = conn.cursor()

        # Query para pegar todos os dados de médicos
        cur.execute("SELECT id, estado, municipio, especialidade, latitude, longitude FROM medicos;")
        medicos_data = cur.fetchall()

        # Query para pegar todos os dados de hospitais
        cur.execute("SELECT id, estado, municipio, especialidade, latitude, longitude FROM hospitais;")
        hospitais_data = cur.fetchall()

        # Retorna os dados como listas de tuplas
        return medicos_data, hospitais_data

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro: {error}")
        return [], []
    finally:
        if conn is not None:
            conn.close()

# Exemplo de como os dados serão retornados:
# medicos_data = [(medico_id, 'Estado', 'Municipio', 'Especialidade', lat, lon), ...]
# hospitais_data = [(hospital_id, 'Estado', 'Municipio', 'Especialidade', lat, lon), ...]

def haversine(lat1, lon1, lat2, lon2):
    """Calcula a distância em km entre dois pontos geográficos."""
    R = 6371  # Raio da Terra em km

    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c







#
#
#
#
#
##

def aloca_medicos_hospitais(medicos, hospitais):
    matches = {} # Dicionário para armazenar as alocações: {medico_id: [hospital_id, ...]}
    hospitais_com_especialidade = {} # Cache para hospitais por especialidade

    # Otimização: pré-processar hospitais por especialidade para buscas rápidas
    for h_id, _, _, h_especialidade, _, _ in hospitais:
        if h_especialidade not in hospitais_com_especialidade:
            hospitais_com_especialidade[h_especialidade] = []
        hospitais_com_especialidade[h_especialidade].append(h_id)

    # Convertendo a lista de tuplas para uma lista de dicionários para facilitar o acesso
    hospitais_dict = {h[0]: h for h in hospitais}

    # Loop principal: itera sobre cada médico
    for m_id, m_estado, m_municipio, m_especialidade, m_lat, m_lon in medicos:
        matches[m_id] = [] # Inicia a lista de alocações para o médico atual
        
        if len(matches[m_id]) >= 3:
            continue # Pula para o próximo médico se já tiver 3 alocações

        # PASSO 1: Encontrar hospitais com a mesma especialidade, estado, município e dentro do raio
        opcoes_validas = []
        
        # Filtra a lista de hospitais para hospitais com a especialidade do médico
        hospitais_especialidade = hospitais_com_especialidade.get(m_especialidade, [])
        
        # Itera apenas sobre hospitais da especialidade para reduzir a busca
        for h_id in hospitais_especialidade:
            h_estado, h_municipio, h_especialidade, h_lat, h_lon = hospitais_dict[h_id][1:]
            
            # Verifica as condições
            if h_estado == m_estado and h_municipio == m_municipio:
                distancia = haversine(m_lat, m_lon, h_lat, h_lon)
                if distancia <= 30:
                    opcoes_validas.append((h_id, distancia))

        # PASSO 2: Lógica de alocação (se houver opções)
        if opcoes_validas:
            opcoes_validas.sort(key=lambda x: x[1]) # Ordena por distância (do mais perto ao mais longe)
            
            for h_id, _ in opcoes_validas:
                if h_id not in matches[m_id]: # Verifica se o médico já não trabalha lá
                    matches[m_id].append(h_id)
                    if len(matches[m_id]) >= 3:
                        break # Sai do loop se o médico já tiver 3 alocações
        
        # PASSO 3: Lógica de contingência (se não encontrou hospitais da especialidade)
        if not matches[m_id]:
            opcoes_proximas = []
            
            for h_id, h_estado, h_municipio, _, h_lat, h_lon in hospitais:
                if h_estado == m_estado and h_municipio == m_municipio:
                    distancia = haversine(m_lat, m_lon, h_lat, h_lon)
                    if distancia <= 30:
                        opcoes_proximas.append((h_id, distancia))
            
            if opcoes_proximas:
                opcoes_proximas.sort(key=lambda x: x[1])

                for h_id, _ in opcoes_proximas:
                    if h_id not in matches[m_id]:
                        matches[m_id].append(h_id)
                        if len(matches[m_id]) >= 3:
                            break
    
    return matches

# --- Execução Principal ---
if __name__ == '__main__':
    print("Obtendo dados...")
    medicos, hospitais = get_data()

    if medicos and hospitais:
        print("Dados obtidos. Iniciando a alocação...")
        alocacoes = aloca_medicos_hospitais(medicos, hospitais)
        
        print("\n--- Resultados da Alocação ---")
        for medico_id, hospitais_alocados in list(alocacoes.items())[:5]:
            print(f"Médico ID {medico_id}: Alocado em {hospitais_alocados}")
        print(f"\nTotal de médicos alocados: {len(alocacoes)}")
    else:
        print("Não foi possível obter os dados para alocação.") 