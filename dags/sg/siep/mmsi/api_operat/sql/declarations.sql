-- Liste des déclarations
DROP TABLE IF EXISTS siep.ademe_declaration CASCADE;
CREATE TABLE IF NOT EXISTS siep.ademe_declaration (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    id_consommation INTEGER,
    annee_declaree TEXT,
    ref_operat_efa TEXT,
    denomination_occupant_efa TEXT,
    complement_nom_efa TEXT,
    type_occupant_efa TEXT,
    id_occupant_efa TEXT,
    numero_nom_voie TEXT,
    code_postal TEXT,
    commune TEXT,
    id_import_consommations TEXT,
    statut TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (id_consommation, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
