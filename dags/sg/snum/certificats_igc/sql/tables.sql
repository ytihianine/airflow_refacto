DROP SCHEMA certificat_igc CASCADE;
CREATE SCHEMA certificat_igc;

CREATE TABLE certificat_igc.aip (
    id SERIAL,
    aip_mail TEXT,
    aip_balf_mail TEXT,
    aip_direction TEXT,
    import_date DATE NOT NULL
) PARTITION BY RANGE (import_date);


CREATE TABLE certificat_igc.certificat (
    id SERIAL,
    dn TEXT,
    subjectid TEXT,
    contact TEXT,
    email  TEXT,
    date_debut_validite DATE,
    date_fin_validite DATE,
    profile TEXT,
    status  TEXT,
    date_revocation DATE,
    certificat_direction TEXT,
    ac TEXT,
    type_offre TEXT,
    supports TEXT,
    etat TEXT,
    version TEXT,
    version_serveur TEXT,
    import_date DATE NOT NULL
) PARTITION BY RANGE (import_date);
