-- Liste des déclarations
DROP TABLE IF EXISTS siep.declaration_ademe;
CREATE TABLE IF NOT EXISTS siep.declaration_ademe (
    id SERIAL PRIMARY KEY,
    id_consommation INTEGER UNIQUE,
    annee_declaree TEXT,
    ref_operat_efa TEXT,
    denomination_occupant_efa TEXT,
    complement_nom_efa TEXT,
    type_occupant_efa TEXT,
    id_occupant_efa TEXT UNIQUE,
    id_import_consommations TEXT,
    statut TEXT
);

DROP TABLE IF EXISTS siep.declaration_ademe_adresse_efa;
CREATE TABLE IF NOT EXISTS siep.declaration_ademe_adresse_efa (
    id SERIAL PRIMARY KEY,
    id_occupant_efa TEXT REFERENCES siep.declaration_ademe(id_occupant_efa),
    numero_nom_voie TEXT,
    code_postal TEXT,
    commune TEXT
);
