DROP TABLE IF EXISTS temporaire.tmp_bien_bail;
DROP TABLE IF EXISTS siep.bien_bail;
CREATE TABLE IF NOT EXISTS siep.bien_bail (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
    date_debut_bail DATE,
    date_fin_bail DATE,
    duree_bail INTEGER,
    type_contrat TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
