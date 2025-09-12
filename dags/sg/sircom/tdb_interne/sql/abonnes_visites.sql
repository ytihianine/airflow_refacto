CREATE TABLE IF NOT EXISTS sircom.abonnes_reseaux_sociaux (
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    reseaux_sociaux TEXT,
    abonnes INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.abonnes_aux_lettres (
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    nouveaux_abonnes_bip INTEGER,
    desabonnes_bip INTEGER,
    cumul_total_abonnes_bip INTEGER,
    nouveaux_abonnes_bie INTEGER,
    desabonnes_bie INTEGER,
    cumul_total_abonnes_bie INTEGER,
    nouveaux_abonnes_totaux INTEGER,
    desabonnes_totaux INTEGER,
    cumul_total_abonnes_totaux INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.visites_portail (
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    visites INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.visites_bercyinfo (
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    visites INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.performances_lettres (
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    indicateurs TEXT,
    taux DOUBLE PRECISION,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.visites_alize (
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    visites INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.visites_intranet_sg (
    id BIGSERIAL PRIMARY KEY,
    date DATE,
    visites INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.ouverture_lettres_alize (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    taux_ouverture FLOAT,
    is_last_value BOOLEAN
);
