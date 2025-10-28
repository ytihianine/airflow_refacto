DROP TABLE IF EXISTS temporaire.tmp_bien_effectif;
DROP TABLE IF EXISTS siep.bien_effectif;
CREATE TABLE IF NOT EXISTS siep.bien_effectif (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    effectif_administratif INTEGER,
    effectif_physique INTEGER,
    nb_positions_de_travail INTEGER,
    nb_postes INTEGER,
    nb_residents FLOAT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
