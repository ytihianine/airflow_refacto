DROP TABLE IF EXISTS siep.site;
CREATE TABLE IF NOT EXISTS siep.site (
	id BIGSERIAL PRIMARY KEY,
	code_site BIGINT UNIQUE NOT NULL,
	libelle_site TEXT,
	site_mef_hmef TEXT
);
