DROP TABLE IF EXISTS siep.bien_georisques;
CREATE TABLE IF NOT EXISTS siep.bien_georisques (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    risque_categorie TEXT,
    risque_libelle TEXT,
    risque_present BOOLEAN
);

DROP TABLE IF EXISTS siep.bien_georisques_api;
CREATE TABLE IF NOT EXISTS siep.bien_georisques_api (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    statut TEXT,
    statut_code INT,
    raison TEXT
);
