DROP TABLE IF EXISTS temporaire.tmp_bien_bacs;
DROP TABLE IF EXISTS siep.bien_bacs;
CREATE TABLE IF NOT EXISTS siep.bien_bacs (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    classe_gtb TEXT,
    commentaire_soumission TEXT,
    commentaire_general TEXT,
    date_installation_gtb DATE,
    date_derniere_inspection DATE,
    presence_gtb BOOLEAN,
    soumis_decret_bacs BOOLEAN,
    raison_soumis_decret_bacs TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
