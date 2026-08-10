DROP TABLE IF EXISTS temporaire.tmp_bien_localisation;
DROP TABLE IF EXISTS siep.bien_localisation;
CREATE TABLE IF NOT EXISTS siep.bien_localisation (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
	france_etranger TEXT,
	metropole_outremer TEXT,
	code_insee_normalise TEXT,
	adresse_source TEXT,
	adresse_normalisee TEXT,
	-- complement_adresse TEXT,
	commune_source TEXT,
	commune_normalisee TEXT,
	commune_mef_hmef TEXT,
	-- code_postal TEXT,
	num_departement_source TEXT,
	num_departement_normalisee TEXT,
	code_iso_departement_normalise TEXT,
	code_iso_region_normalise TEXT,
	latitude DOUBLE PRECISION,
	longitude DOUBLE PRECISION,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id TEXT,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
