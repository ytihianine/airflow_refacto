DROP TABLE IF EXISTS temporaire.tmp_bien_reglementation;
DROP TABLE IF EXISTS siep.bien_reglementation;
CREATE TABLE IF NOT EXISTS siep.bien_reglementation (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
	classement_monument_historique TEXT,
	igh BOOLEAN,
	reglementation TEXT,
	reglementation_corrigee TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
