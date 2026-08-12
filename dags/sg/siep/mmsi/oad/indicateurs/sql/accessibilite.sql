DROP TABLE IF EXISTS siep.bien_accessibilite CASCADE;
CREATE TABLE IF NOT EXISTS siep.bien_accessibilite (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
    attestation_accessibilite BOOLEAN,
    beneficie_derogation BOOLEAN,
    fait_objet_adap BOOLEAN,
    date_mise_en_accessibilite DATE,
    motif_derogation TEXT,
    numero_adap TEXT,
    presence_registre_accessibilite BOOLEAN,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS siep.bien_accessibilite_detail CASCADE;
CREATE TABLE IF NOT EXISTS siep.bien_accessibilite_detail (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
    composant_bien TEXT NOT NULL,
    niveau TEXT NOT NULL,
    niveau_fonctionnel TEXT,
    niveau_reglementaire TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter, composant_bien, niveau),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
