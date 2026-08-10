DROP SCHEMA certificat_igc CASCADE;
CREATE SCHEMA certificat_igc;

CREATE TABLE certificat_igc.certificat (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    id_certificat BIGINT,
    subjectid TEXT,
    contact TEXT,
    email  TEXT,
    date_debut_validite DATE,
    date_fin_validite DATE,
    profile TEXT,
    ac TEXT,
    type_offre TEXT,
    supports TEXT,
    version TEXT,
    version_serveur TEXT,
    import_timestamp TIMESTAMP,
    PRIMARY KEY(id_row, import_timestamp)
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE certificat_igc.mandataire (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    libelle TEXT,
    sigle TEXT,
    mail TEXT,
    structure TEXT,
    date DATE,
    import_timestamp TIMESTAMP,
    PRIMARY KEY(id_row, import_timestamp)
) PARTITION BY RANGE (import_timestamp);


CREATE TABLE certificat_igc.agent (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    nom_prenom TEXT,
    agent_direction TEXT,
    agent_mail TEXT,
    agent_groupe_gestionnaire TEXT,
    import_timestamp TIMESTAMP,
    PRIMARY KEY(id_row, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
