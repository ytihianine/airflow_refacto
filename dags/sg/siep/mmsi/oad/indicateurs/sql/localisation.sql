DROP TABLE IF EXISTS temporaire.tmp_bien_localisation;
DROP TABLE IF EXISTS siep.bien_localisation;
CREATE TABLE IF NOT EXISTS siep.bien_localisation (
	id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
	france_etranger TEXT,
	code_insee_normalise TEXT,
	adresse_source TEXT,
	adresse_normalisee TEXT,
	-- complement_adresse TEXT,
	commune_source TEXT,
	commune_normalisee TEXT,
	-- code_postal TEXT,
	num_departement_source TEXT,
	num_departement_normalisee TEXT,
	latitude DOUBLE PRECISION,
	longitude DOUBLE PRECISION
);
