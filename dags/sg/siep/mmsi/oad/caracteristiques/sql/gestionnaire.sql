DROP TABLE IF EXISTS siep.gestionnaire;
CREATE TABLE IF NOT EXISTS siep.gestionnaire (
    id BIGSERIAL PRIMARY KEY,
    code_gestionnaire BIGINT UNIQUE NOT NULL,
    libelle_gestionnaire TEXT,
    libelle_simplifie TEXT,
    libelle_abrege TEXT,
    lien_mef_gestionnaire TEXT,
    personnalite_juridique TEXT,
    personnalite_juridique_simplifiee TEXT,
    personnalite_juridique_precision TEXT,
    ministere TEXT
);
