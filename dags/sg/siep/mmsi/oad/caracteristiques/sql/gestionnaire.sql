DROP TABLE IF EXISTS siep.gestionnaire;
CREATE TABLE IF NOT EXISTS siep.gestionnaire (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_gestionnaire BIGINT NOT NULL,
    libelle_gestionnaire TEXT,
    libelle_simplifie TEXT,
    libelle_abrege TEXT,
    lien_mef_gestionnaire TEXT,
    personnalite_juridique TEXT,
    personnalite_juridique_simplifiee TEXT,
    personnalite_juridique_precision TEXT,
    ministere TEXT,
    import_timestamp TIMESTAMP,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_gestionnaire)
) PARTITION BY RANGE (import_timestamp);
