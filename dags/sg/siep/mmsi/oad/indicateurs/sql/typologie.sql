DROP TABLE IF EXISTS siep.bien_typologie CASCADE;
CREATE TABLE IF NOT EXISTS siep.bien_typologie (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
	usage_detaille_du_bien TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp),
    FOREIGN KEY(usage_detaille_du_bien) REFERENCES siep.ref_typologie(usage_detaille_du_bien)
) PARTITION BY RANGE (import_timestamp);
