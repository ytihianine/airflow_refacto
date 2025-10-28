DROP TABLE IF EXISTS siep.gestionnaire;
CREATE TABLE IF NOT EXISTS siep.gestionnaire (
    id BIGSERIAL,
    code_gestionnaire BIGINT NOT NULL,
    libelle_gestionnaire TEXT,
    libelle_simplifie TEXT,
    libelle_abrege TEXT,
    lien_mef_gestionnaire TEXT,
    personnalite_juridique TEXT,
    personnalite_juridique_simplifiee TEXT,
    personnalite_juridique_precision TEXT,
    ministere TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_gestionnaire, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
