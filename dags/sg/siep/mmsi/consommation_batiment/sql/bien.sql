DROP TABLE IF EXISTS siep.bien_information_complementaire;
CREATE TABLE IF NOT EXISTS siep.bien_information_complementaire (
    id BIGSERIAL PRIMARY KEY,
    code_bat_gestionnaire TEXT,
    etat_bat TEXT,
    date_sortie_bat DATE,
    date_sortie_site DATE,
    date_derniere_renovation TEXT,
    annee_reference INT,
    efa TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
    PRIMARY KEY (code_bat_gestionnaire, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
