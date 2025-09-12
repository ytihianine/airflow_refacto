DROP TABLE IF EXISTS temporaire.tmp_bien_deet_energie_ges;
DROP TABLE IF EXISTS siep.bien_deet_energie_ges;
CREATE TABLE IF NOT EXISTS siep.bien_deet_energie_ges (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    bat_assujettis_deet BOOLEAN,
    deet_commentaire TEXT,
    annee1_conso_ef_et_ges INTEGER,
    annee2_conso_ef_et_ges INTEGER,
    annee1_conso_eau INTEGER,
    annee2_conso_eau INTEGER,
    conso_eau_annee1 FLOAT,
    conso_eau_annee2 FLOAT,
    conso_ef_annee1 FLOAT,
    conso_ef_annee2 FLOAT,
    emission_ges_annee1 FLOAT,
    emission_ges_annee2 FLOAT
);
