-- Habilita as extensões necessárias (executado apenas uma vez)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";

-- Tabela 1: estados (não depende de ninguém)
CREATE TABLE IF NOT EXISTS estados (
    codigo_uf INT PRIMARY KEY,
    uf CHAR(2) NOT NULL,
    nome VARCHAR(100) NOT NULL
);

-- Tabela 2: municipios (depende de estados)
CREATE TABLE IF NOT EXISTS municipios (
    codigo_ibge BIGINT PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    codigo_uf INT NOT NULL REFERENCES estados(codigo_uf),
    localizacao GEOMETRY(Point, 4326)
);
CREATE INDEX idx_municipios_localizacao ON municipios USING GIST (localizacao);

-- Tabela 3: cid10 (não depende de ninguém)
CREATE TABLE IF NOT EXISTS cid10 (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT,
    especialidade VARCHAR(100)
);

-- Tabela 4: hospitais (depende de municipios)
CREATE TABLE IF NOT EXISTS hospitais (
    codigo UUID PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    municipio_id BIGINT REFERENCES municipios(codigo_ibge),
    especialidades TEXT[],
    leitos_totais INT CHECK (leitos_totais >= 0),
    localizacao GEOMETRY(Point, 4326)
);
CREATE INDEX idx_hospitais_especialidades ON hospitais USING GIN (especialidades);
CREATE INDEX idx_hospitais_localizacao ON hospitais USING GIST (localizacao);

-- Tabela 5: medicos (depende de municipios)
CREATE TABLE IF NOT EXISTS medicos (
    codigo UUID PRIMARY KEY,
    nome_completo VARCHAR(255) NOT NULL,
    especialidade VARCHAR(100) NOT NULL,
    municipio_id BIGINT REFERENCES municipios(codigo_ibge)
);

-- Tabela 6: pacientes (depende de municipios, cid10, hospitais)
CREATE TABLE IF NOT EXISTS pacientes (
    codigo UUID PRIMARY KEY,
    cpf VARCHAR(11) UNIQUE ,
    nome_completo VARCHAR(255) NOT NULL,
    genero CHAR(1) CHECK (genero IN ('M', 'F')),
    cod_municipio BIGINT REFERENCES municipios(codigo_ibge),
    bairro VARCHAR(150),
    convenio BOOLEAN,
    cid_10 VARCHAR(10) REFERENCES cid10(codigo),
    hospital_alocado_id UUID REFERENCES hospitais(codigo)
);
CREATE INDEX idx_pacientes_cpf ON pacientes(cpf);
CREATE INDEX idx_pacientes_cod_municipio ON pacientes(cod_municipio);

-- Tabela 7: medico_hospital_associacao (depende de medicos e hospitais - DEVE SER UMA DAS ÚLTIMAS)
CREATE TABLE IF NOT EXISTS medico_hospital_associacao (
    medico_id UUID NOT NULL REFERENCES medicos(codigo),
    hospital_id UUID NOT NULL REFERENCES hospitais(codigo),
    PRIMARY KEY (medico_id, hospital_id)
);  