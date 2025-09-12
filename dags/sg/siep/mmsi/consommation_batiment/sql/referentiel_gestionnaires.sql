DROP TABLE IF EXISTS siep.referentiel_gestionnaires
CREATE TABLE IF NOT EXISTS siep.referentiel_gestionnaires (
    id BIGSERIAL PRIMARY KEY,
    code_gestionnaire BIGINT UNIQUE,
    nom_gestionnaire TEXT,
    type_entite TEXT,
    perimetre TEXT
);
