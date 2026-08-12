DROP TABLE IF EXISTS siep.bien_surface CASCADE;
CREATE TABLE IF NOT EXISTS siep.bien_surface (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
	surface_aire_amenagee FLOAT,
	contenance_cadastrale FLOAT,
	sba FLOAT,
	sba_optimisee FLOAT,
	shon FLOAT,
	sub FLOAT,
	sub_optimisee FLOAT,
	sun FLOAT,
	surface_de_plancher FLOAT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
