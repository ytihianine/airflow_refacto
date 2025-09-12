DROP TABLE IF EXISTS temporaire.tmp_bien_strategie;
DROP TABLE IF EXISTS siep.bien_strategie;
CREATE TABLE IF NOT EXISTS siep.bien_strategie (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    segmentation_sdir_spsi TEXT,
    segmentation_theorique_sdir_spsi TEXT,
    statut_osc TEXT,
    perimetre_spsi_initial TEXT,
    perimetre_spsi_maj TEXT
);
