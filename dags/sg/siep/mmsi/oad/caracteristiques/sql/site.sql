DROP TABLE IF EXISTS siep.site;
CREATE TABLE IF NOT EXISTS siep.site (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
	code_site BIGINT NOT NULL,
	libelle_site TEXT,
	site_mef_hmef TEXT,
    import_timestamp TIMESTAMP,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_site)
) PARTITION BY RANGE (import_timestamp);
