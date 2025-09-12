CREATE SCHEMA IF NOT EXISTS documentation;

DROP TABLE IF EXISTS documentation.ref_source_format;
CREATE TABLE IF NOT EXISTS documentation.ref_source_format (
    id SERIAL PRIMARY KEY,
    source_format TEXT
);

DROP TABLE IF EXISTS documentation.ref_frequence;
CREATE TABLE IF NOT EXISTS documentation.ref_frequence (
    id SERIAL PRIMARY KEY,
    frequence_fr TEXT,
    frequence_en TEXT
);

DROP TABLE IF EXISTS documentation.ref_couverture_geographique;
CREATE TABLE IF NOT EXISTS documentation.ref_couverture_geographique (
    id SERIAL PRIMARY KEY,
    couverture_geo_fr TEXT,
    couverture_geo_en TEXT
);

DROP TABLE IF EXISTS documentation.ref_systeme_info;
CREATE TABLE IF NOT EXISTS documentation.ref_systeme_info (
    id SERIAL PRIMARY KEY,
    nom_si TEXT,
    commentaire TEXT
);

DROP TABLE IF EXISTS documentation.ref_licence;
CREATE TABLE IF NOT EXISTS documentation.ref_licence (
    id SERIAL PRIMARY KEY,
    licence_nom_en TEXT,
    licence_sigle TEXT
);

DROP TABLE IF EXISTS documentation.ref_structures;
CREATE TABLE IF NOT EXISTS documentation.ref_structures (
    id SERIAL PRIMARY KEY,
    nom TEXT,
    nom_long TEXT,
    commentaire TEXT,
    siret INT
);

DROP TABLE IF EXISTS documentation.ref_service;
CREATE TABLE IF NOT EXISTS documentation.ref_service (
    id SERIAL PRIMARY KEY,
    service TEXT,
    sigle TEXT,
    id_structure INT REFERENCES documentation.ref_structures(id)
);

DROP TABLE IF EXISTS documentation.ref_contact;
CREATE TABLE IF NOT EXISTS documentation.ref_contact (
    id SERIAL PRIMARY KEY,
    nom_bureau TEXT,
    mail TEXT,
    commentaire TEXT,
    service INT REFERENCES documentation.ref_service(id)
);

DROP TABLE IF EXISTS documentation.ref_theme;
CREATE TABLE IF NOT EXISTS documentation.ref_theme (
    id SERIAL PRIMARY KEY,
    theme TEXT
);

DROP TABLE IF EXISTS documentation.ref_type_donnees;
CREATE TABLE IF NOT EXISTS documentation.ref_type_donnees (
    id SERIAL PRIMARY KEY,
    type_donnees TEXT
);
