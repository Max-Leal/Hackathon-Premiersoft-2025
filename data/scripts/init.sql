-- Habilita a extensão PostGIS para funcionalidades geoespaciais
CREATE EXTENSION IF NOT EXISTS postgis;

-- Criação das tabelas (exemplo simplificado, adicione todas as colunas)
CREATE TABLE IF NOT EXISTS estados (
    id SERIAL PRIMARY KEY,
    codigo_uf INT UNIQUE NOT NULL,
    uf VARCHAR(2) NOT NULL,
    nome VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS municipios (
    id SERIAL PRIMARY KEY,
    codigo_ibge BIGINT UNIQUE NOT NULL,
    nome VARCHAR(255) NOT NULL,
    codigo_uf INT REFERENCES estados(codigo_uf),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(10, 8)
);

-- Adicione as outras tabelas: hospitais, medicos, pacientes, cid10, etc.
-- Exemplo para hospitais com JSONB e PostGIS
CREATE TABLE IF NOT EXISTS hospitais (
    id SERIAL PRIMARY KEY,
    codigo UUID UNIQUE NOT NULL,
    nome VARCHAR(255),
    municipio_id BIGINT REFERENCES municipios(codigo_ibge),
    especialidades JSONB, -- Coluna para a lista de especialidades
    localizacao GEOMETRY(Point, 4326) -- Coluna para coordenadas geográficas
);

-- Adicione um índice GIN para buscas eficientes nas especialidades
CREATE INDEX idx_hospitais_especialidades ON hospitais USING GIN (especialidades);