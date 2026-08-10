DROP TABLE IF EXISTS temporaire.tmp_bien_proprietaire;
DROP TABLE IF EXISTS siep.bien_proprietaire;
CREATE TABLE IF NOT EXISTS siep.bien_proprietaire (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
	numero_proprietaire TEXT,
	type_proprietaire_detail TEXT,
	statut_occupation TEXT NOT NULL,
	type_proprietaire TEXT NOT NULL,
	locatif_domanial TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id TEXT,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
