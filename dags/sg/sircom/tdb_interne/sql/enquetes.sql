CREATE TABLE IF NOT EXISTS sircom.engagement_agents_mef (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    indicateurs TEXT,
    taux_engagement DOUBLE PRECISION,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.qualite_de_vie_au_travail (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    indicateurs TEXT,
    taux_satisfaction INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.collab_inter_structures (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    structure TEXT,
    indicateurs TEXT,
    taux INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.observatoire_interne (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    indicateurs TEXT,
    valeur DOUBLE PRECISION,
    unite TEXT,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.enquete_360 (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    indicateurs TEXT,
    valeur DOUBLE PRECISION,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.participation_observatoire_interne (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    taux_participation FLOAT,
    is_last_value BOOLEAN
);


CREATE TABLE IF NOT EXISTS sircom.engagement_environnement (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    niveau TEXT,
    indicateurs_regroupement TEXT,
    indicateurs TEXT,
    nombre_votants DOUBLE PRECISION,
    is_last_value BOOLEAN
);
