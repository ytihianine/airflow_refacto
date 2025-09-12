DROP TABLE IF EXISTS temporaire.tmp_bien_surface;
DROP TABLE IF EXISTS siep.bien_surface;
CREATE TABLE IF NOT EXISTS siep.bien_surface (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
	surface_aire_amenagee FLOAT,
	contenance_cadastrale FLOAT,
	sba FLOAT,
	sba_optimisee FLOAT,
	shon FLOAT,
	sub FLOAT,
	sub_optimisee FLOAT,
	sun FLOAT,
	surface_de_plancher FLOAT
);
