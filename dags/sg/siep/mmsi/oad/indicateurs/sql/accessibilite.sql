DROP TABLE IF EXISTS temporaire.tmp_bien_accessibilite;
DROP TABLE IF EXISTS siep.bien_accessibilite;
CREATE TABLE IF NOT EXISTS siep.bien_accessibilite (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    attestation_accessibilite BOOLEAN,
    beneficie_derogation BOOLEAN,
    fait_objet_adap BOOLEAN,
    date_mise_en_accessibilite DATE,
    motif_derogation TEXT,
    numero_adap TEXT,
    presence_registre_accessibilite BOOLEAN
);

DROP TABLE IF EXISTS temporaire.tmp_bien_accessibilite_detail;
DROP TABLE IF EXISTS siep.bien_accessibilite_detail;
CREATE TABLE IF NOT EXISTS siep.bien_accessibilite_detail (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    composant_bien TEXT,
    niveau TEXT,
    niveau_fonctionnel TEXT,
    niveau_reglementaire TEXT
);
