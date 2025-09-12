DROP TABLE IF EXISTS siep.bien_eligibilite_fcu;
CREATE TABLE IF NOT EXISTS siep.bien_eligibilite_fcu (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    is_eligible BOOLEAN,
    distance INT,
    in_pdp BOOLEAN,
    is_based_on_iris BOOLEAN,
    futur_network BOOLEAN,
    id_fcu TEXT,
    name TEXT,
    gestionnaire_fcu TEXT,
    rate_enrr DOUBLE PRECISION,
    rate_co2 DOUBLE PRECISION,
    api_status TEXT,
    api_status_code INT,
    api_raison TEXT
);
