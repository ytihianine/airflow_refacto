DROP TABLE IF EXISTS temporaire.tmp_bien_valeur;
DROP TABLE IF EXISTS siep.bien_valeur;
CREATE TABLE IF NOT EXISTS siep.bien_valeur (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
	valorisation_chorus FLOAT
);
