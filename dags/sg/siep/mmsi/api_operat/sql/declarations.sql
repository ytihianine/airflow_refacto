-- Liste des déclarations
DROP TABLE IF EXISTS siep.ademe_declaration;
CREATE TABLE IF NOT EXISTS siep.ademe_declaration (
    id SERIAL PRIMARY KEY,
    id_consommation INTEGER UNIQUE,
    annee_declaree TEXT,
    ref_operat_efa TEXT,
    denomination_occupant_efa TEXT,
    complement_nom_efa TEXT,
    type_occupant_efa TEXT,
    id_occupant_efa TEXT UNIQUE,
    numero_nom_voie TEXT,
    code_postal TEXT,
    commune TEXT,
    id_import_consommations TEXT,
    statut TEXT
);
