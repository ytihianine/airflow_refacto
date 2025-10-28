DROP TABLE IF EXISTS temporaire.tmp_bien_accessibilite;
DROP TABLE IF EXISTS siep.bien_accessibilite;
CREATE TABLE IF NOT EXISTS siep.bien_accessibilite (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    attestation_accessibilite BOOLEAN,
    beneficie_derogation BOOLEAN,
    fait_objet_adap BOOLEAN,
    date_mise_en_accessibilite DATE,
    motif_derogation TEXT,
    numero_adap TEXT,
    presence_registre_accessibilite BOOLEAN,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS temporaire.tmp_bien_accessibilite_detail;
DROP TABLE IF EXISTS siep.bien_accessibilite_detail;
CREATE TABLE IF NOT EXISTS siep.bien_accessibilite_detail (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    composant_bien TEXT NOT NULL,
    niveau TEXT NOT NULL,
    niveau_fonctionnel TEXT,
    niveau_reglementaire TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, composant_bien, niveau, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp)
) PARTITION BY RANGE (import_timestamp);
