DROP TABLE IF EXISTS temporaire.tmp_bien_effectif;
DROP TABLE IF EXISTS siep.bien_effectif;
CREATE TABLE IF NOT EXISTS siep.bien_effectif (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
    effectif_administratif INTEGER,
    effectif_physique INTEGER,
    nb_positions_de_travail INTEGER,
    nb_postes FLOAT,
    nb_residents FLOAT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
