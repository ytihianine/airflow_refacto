DROP TABLE IF EXISTS temporaire.tmp_bien_etat_de_sante;
DROP TABLE IF EXISTS siep.bien_etat_de_sante;
CREATE TABLE IF NOT EXISTS siep.bien_etat_de_sante (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    composant_bien TEXT NOT NULL,
    eds_theorique TEXT,
    eds_constate TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp, composant_bien),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
