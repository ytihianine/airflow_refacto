CREATE TABLE IF NOT EXISTS sircom.abonnes_reseaux_sociaux (
    id INT PRIMARY KEY,
    date DATE,
    reseaux_sociaux TEXT,
    abonnes INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.abonnes_aux_lettres (
    id INT PRIMARY KEY,
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
    id INT PRIMARY KEY,
    date DATE,
    visites INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.visites_bercyinfo (
    id INT PRIMARY KEY,
    date DATE,
    visites INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.performances_lettres (
    id INT PRIMARY KEY,
    date DATE,
    indicateurs TEXT,
    taux DOUBLE PRECISION,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.visites_alize (
    id INT PRIMARY KEY,
    date DATE,
    visites INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.visites_intranet_sg (
    id INT PRIMARY KEY,
    date DATE,
    visites INTEGER,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.ouverture_lettres_alize (
    id INT PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    nombre_agent INTEGER,
    taux_ouverture FLOAT,
    is_last_value BOOLEAN
);
CREATE TABLE IF NOT EXISTS sircom.impact_actions_com(
    id INT PRIMARY KEY,
    nombre_article INTEGER,
    nombre_vue INTEGER,
    date DATE,
    is_last_value BOOLEAN
);
CREATE TABLE IF NOT EXISTS sircom.impressions_reseaux_sociaux (
    id INT PRIMARY KEY,
    date DATE,
    impressions INTEGER,
    is_last_value BOOLEAN
);
