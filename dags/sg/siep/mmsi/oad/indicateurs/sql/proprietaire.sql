DROP TABLE IF EXISTS temporaire.tmp_bien_proprietaire;
DROP TABLE IF EXISTS siep.bien_proprietaire;
CREATE TABLE IF NOT EXISTS siep.bien_proprietaire (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
	numero_proprietaire TEXT,
	type_proprietaire_detail TEXT,
	statut_occupation TEXT NOT NULL,
	type_proprietaire TEXT NOT NULL,
	locatif_domanial TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
