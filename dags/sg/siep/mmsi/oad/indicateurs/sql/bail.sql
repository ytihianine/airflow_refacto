DROP TABLE IF EXISTS temporaire.tmp_bien_bail;
DROP TABLE IF EXISTS siep.bien_bail;
CREATE TABLE IF NOT EXISTS siep.bien_bail (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    date_debut_bail DATE,
    date_fin_bail DATE,
    duree_bail INTEGER,
    type_contrat TEXT
);
