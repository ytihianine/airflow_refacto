DROP TABLE IF EXISTS siep.bien_etat_de_sante CASCADE;
CREATE TABLE IF NOT EXISTS siep.bien_etat_de_sante (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
    composant_bien TEXT NOT NULL,
    eds_theorique TEXT,
    eds_constate TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter, composant_bien),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
