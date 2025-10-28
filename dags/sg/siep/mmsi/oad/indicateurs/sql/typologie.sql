DROP TABLE IF EXISTS temporaire.tmp_bien_typologie;
DROP TABLE IF EXISTS siep.bien_typologie;
CREATE TABLE IF NOT EXISTS siep.bien_typologie (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
	usage_detaille_du_bien TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp),
    FOREIGN KEY(usage_detaille_du_bien) REFERENCES siep.ref_typologie(usage_detaille_du_bien)
) PARTITION BY RANGE (import_timestamp);
