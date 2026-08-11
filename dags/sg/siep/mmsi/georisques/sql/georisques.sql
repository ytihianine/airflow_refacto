DROP TABLE IF EXISTS siep.bien_georisque;
CREATE TABLE IF NOT EXISTS siep.bien_georisque (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
    risque_categorie TEXT,
    risque_libelle TEXT,
    risque_present BOOLEAN,
    statut TEXT,
    statut_code INT,
    raison TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp)
) PARTITION BY RANGE (import_timestamp) ;
