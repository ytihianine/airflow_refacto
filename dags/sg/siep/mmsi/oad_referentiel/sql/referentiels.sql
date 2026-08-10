DROP TABLE IF EXISTS siep.ref_typologie CASCADE;
CREATE TABLE IF NOT EXISTS siep.ref_typologie (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    bati_non_bati TEXT,
    famille_de_bien_simplifiee TEXT,
    famille_de_bien TEXT,
    type_de_bien TEXT,
    usage_detaille_du_bien TEXT UNIQUE NOT NULL,
    import_timestamp TIMESTAMP,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter, usage_detaille_du_bien)
);
