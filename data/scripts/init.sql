-- Habilita as extensões necessárias
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- Tabela de Estados (simplificada, lat/lon não são necessárias aqui)
CREATE TABLE IF NOT EXISTS estados (
    codigo_uf INT PRIMARY KEY,
    uf CHAR(2) NOT NULL,
    nome VARCHAR(100) NOT NULL
);

-- Tabela de Municípios
CREATE TABLE IF NOT EXISTS municipios (
    codigo_ibge BIGINT PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    codigo_uf INT NOT NULL REFERENCES estados(codigo_uf),
    localizacao GEOMETRY(Point, 4326) -- Apenas a coluna geométrica é necessária para cálculos
);
CREATE INDEX idx_municipios_localizacao ON municipios USING GIST (localizacao);

-- Tabela CID-10 (Enriquecida na transformação)
CREATE TABLE IF NOT EXISTS cid10 (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT,
    especialidade VARCHAR(100) -- Coluna que será preenchida pelo pipeline
);

-- Tabela de Hospitais
CREATE TABLE IF NOT EXISTS hospitais (
    codigo UUID PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    municipio_id BIGINT REFERENCES municipios(codigo_ibge),
    especialidades TEXT[], -- Usar array de texto é mais simples que JSONB para este caso
    leitos_totais INT CHECK (leitos_totais >= 0),
    localizacao GEOMETRY(Point, 4326)
);
CREATE INDEX idx_hospitais_especialidades ON hospitais USING GIN (especialidades);
CREATE INDEX idx_hospitais_localizacao ON hospitais USING GIST (localizacao);

-- Tabela de Médicos
CREATE TABLE IF NOT EXISTS medicos (
    codigo UUID PRIMARY KEY,
    nome_completo VARCHAR(255) NOT NULL,
    especialidade VARCHAR(100) NOT NULL,
    municipio_id BIGINT REFERENCES municipios(codigo_ibge)
);

-- Tabela de Pacientes (CORRIGIDA)
CREATE TABLE IF NOT EXISTS pacientes (
    codigo UUID PRIMARY KEY,
    cpf VARCHAR(11) UNIQUE NOT NULL,
    nome_completo VARCHAR(255) NOT NULL,
    genero CHAR(1) CHECK (genero IN ('M', 'F')),
    cod_municipio BIGINT REFERENCES municipios(codigo_ibge),
    bairro VARCHAR(150),
    convenio BOOLEAN,
    cid_10 VARCHAR(10) REFERENCES cid10(codigo),
    -- RESULTADO DA ALOCAÇÃO (Simplificado para a Hackathon)
    hospital_alocado_id UUID REFERENCES hospitais(codigo)
);
CREATE INDEX idx_pacientes_cpf ON pacientes(cpf);
CREATE INDEX idx_pacientes_cod_municipio ON pacientes(cod_municipio);

-- Tabela de Associação Médico-Hospital (N:N)
CREATE TABLE IF NOT EXISTS medico_hospital_associacao (
    medico_id UUID NOT NULL REFERENCES medicos(codigo),
    hospital_id UUID NOT NULL REFERENCES hospitais(codigo),
    PRIMARY KEY (medico_id, hospital_id)
);