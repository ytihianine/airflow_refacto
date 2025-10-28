DROP TABLE IF EXISTS temporaire.tmp_bien_strategie;
DROP TABLE IF EXISTS siep.bien_strategie;
CREATE TABLE IF NOT EXISTS siep.bien_strategie (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    segmentation_sdir_spsi TEXT,
    segmentation_theorique_sdir_spsi TEXT,
    statut_osc TEXT,
    perimetre_spsi_initial TEXT,
    perimetre_spsi_maj TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
