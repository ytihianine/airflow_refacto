DROP TABLE IF EXISTS temporaire.tmp_bien_valeur;
DROP TABLE IF EXISTS siep.bien_valeur;
CREATE TABLE IF NOT EXISTS siep.bien_valeur (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
	valorisation_chorus FLOAT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
