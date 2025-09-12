DROP TABLE IF EXISTS temporaire.tmp_bien_proprietaire;
DROP TABLE IF EXISTS siep.bien_proprietaire;
CREATE TABLE IF NOT EXISTS siep.bien_proprietaire (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
	numero_proprietaire TEXT,
	type_proprietaire_detail TEXT,
	statut_occupation TEXT NOT NULL,
	type_proprietaire TEXT NOT NULL,
	locatif_domanial TEXT
);
