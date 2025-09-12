CREATE TABLE IF NOT EXISTS sircom.rh_formation (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    indicateurs TEXT,
    valeur DOUBLE PRECISION,
    unite TEXT,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.rh_turnover (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    valeur DOUBLE PRECISION,
    unite TEXT,
    is_last_value BOOLEAN
);

CREATE TABLE IF NOT EXISTS sircom.rh_contractuel (
    id BIGSERIAL PRIMARY KEY,
    annee INTEGER,
    semestre TEXT,
    date DATE,
    taux_agents_contractuels INTEGER,
    unite TEXT,
    is_last_value BOOLEAN
);
