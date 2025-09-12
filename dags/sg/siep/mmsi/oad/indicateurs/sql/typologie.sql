DROP TABLE IF EXISTS temporaire.tmp_bien_typologie;
DROP TABLE IF EXISTS siep.bien_typologie;
CREATE TABLE IF NOT EXISTS siep.bien_typologie (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
	usage_detaille_du_bien TEXT REFERENCES siep.ref_typologie(usage_detaille_du_bien)
);
