/*
    TABLES MENSUELLES
*/


/*
    TABLES ANNUELLES
*/
DROP TABLE IF EXISTS siep.facture_annuelle_unpivot CASCADE;
CREATE TABLE IF NOT EXISTS siep.facture_annuelle_unpivot (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    annee INT,
    fluide TEXT,
    type_facture TEXT,
    montant_facture DOUBLE PRECISION,
    import_timestamp TIMESTAMP,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire, annee, fluide, type_facture)
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS siep.facture_annuelle_unpivot_comparaison CASCADE;
CREATE TABLE IF NOT EXISTS siep.facture_annuelle_unpivot_comparaison (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    annee INT,
    annee_comparaison INT,
    fluide TEXT,
    type_facture TEXT,
    montant_facture DOUBLE PRECISION,
    montant_facture_comparaison DOUBLE PRECISION,
    diff_vs_comparaison DOUBLE PRECISION,
    import_timestamp TIMESTAMP,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire, annee, annee_comparaison, fluide, type_facture)
) PARTITION BY RANGE (import_timestamp);
