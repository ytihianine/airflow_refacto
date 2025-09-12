DROP TABLE IF EXISTS temporaire.tmp_bien_etat_de_sante;
DROP TABLE IF EXISTS siep.bien_etat_de_sante;
CREATE TABLE IF NOT EXISTS siep.bien_etat_de_sante (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    composant_bien TEXT NOT NULL,
    eds_theorique TEXT,
    eds_constate TEXT
);
