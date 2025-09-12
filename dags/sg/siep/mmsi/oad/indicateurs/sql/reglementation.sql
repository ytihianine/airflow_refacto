DROP TABLE IF EXISTS temporaire.tmp_bien_reglementation;
DROP TABLE IF EXISTS siep.bien_reglementation;
CREATE TABLE IF NOT EXISTS siep.bien_reglementation (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
	classement_monument_historique TEXT,
	igh BOOLEAN,
	reglementation TEXT,
	reglementation_corrigee TEXT
);
