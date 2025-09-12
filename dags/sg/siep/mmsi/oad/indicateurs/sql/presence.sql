DROP TABLE IF EXISTS temporaire.tmp_bien_effectif;
DROP TABLE IF EXISTS siep.bien_effectif;
CREATE TABLE IF NOT EXISTS siep.bien_effectif (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    effectif_administratif INTEGER,
    effectif_physique INTEGER,
    nb_positions_de_travail INTEGER,
    nb_postes INTEGER,
    nb_residents FLOAT
);
