DROP TABLE IF EXISTS siep.site;
CREATE TABLE IF NOT EXISTS siep.site (
	id BIGSERIAL,
	code_site BIGINT NOT NULL,
	libelle_site TEXT,
	site_mef_hmef TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_site, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
