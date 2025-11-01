DROP TABLE IF EXISTS siep.bien_georisque;
CREATE TABLE IF NOT EXISTS siep.bien_georisque (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    risque_categorie TEXT,
    risque_libelle TEXT,
    risque_present BOOLEAN
);

DROP TABLE IF EXISTS siep.bien_georisque_api;
CREATE TABLE IF NOT EXISTS siep.bien_georisque_api (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    statut TEXT,
    statut_code INT,
    raison TEXT
);
