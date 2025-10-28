DROP TABLE IF EXISTS temporaire.tmp_bien_deet_energie_ges;
DROP TABLE IF EXISTS siep.bien_deet_energie_ges;
CREATE TABLE IF NOT EXISTS siep.bien_deet_energie_ges (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
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
    emission_ges_annee2 FLOAT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
