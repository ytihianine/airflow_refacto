DROP TABLE IF EXISTS temporaire.tmp_bien_note;
DROP TABLE IF EXISTS siep.bien_note;
CREATE TABLE IF NOT EXISTS siep.bien_note (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
	completude INTEGER,
	modernisation FLOAT,
	optimisation FLOAT,
	preservation FLOAT
);
