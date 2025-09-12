DROP TABLE IF EXISTS siep.referentiel_perimetre_par_batiment
CREATE TABLE IF NOT EXISTS siep.referentiel_perimetre_par_batiment (
    id BIGSERIAL PRIMARY KEY
    code_batiment TEXT UNIQUE,
    perimetre TEXT
);
