/*
    TABLES MENSUELLES
*/

DROP TABLE IF EXISTS siep.conso_mensuelle CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_mensuelle (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    date_conso DATE NULL,
    ratio_electricite DOUBLE PRECISION,
    ratio_autres_fluides DOUBLE PRECISION,
    degres_jours_de_chauffage DOUBLE PRECISION,
    dju_moyen DOUBLE PRECISION,
    degres_jours_de_refroidissement DOUBLE PRECISION,
    -- Elec
    conso_elec DOUBLE PRECISION,
    conso_elec_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_elec_corr_dju DOUBLE PRECISION,
    conso_elec_surfacique DOUBLE PRECISION,
    -- conso_elec_surfacique_corr_dju DOUBLE PRECISION,
    facture_elec_ht DOUBLE PRECISION,
    facture_elec_ttc DOUBLE PRECISION,
    -- Gaz
    conso_gaz_pcs DOUBLE PRECISION,
    conso_gaz_pci DOUBLE PRECISION,
    conso_gaz_pci_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_gaz_corr_dju DOUBLE PRECISION,
    conso_gaz_surfacique DOUBLE PRECISION,
    -- conso_gaz_surfacique_corr_dju DOUBLE PRECISION,
    facture_gaz_ht DOUBLE PRECISION,
    facture_gaz_ttc DOUBLE PRECISION,
    -- Eau
    conso_eau DOUBLE PRECISION,
    facture_eau_htva DOUBLE PRECISION,
    facture_eau_ttc DOUBLE PRECISION,
    -- Réseau de chaleur
    conso_reseau_chaleur DOUBLE PRECISION,
    conso_reseau_chaleur_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_reseau_chaleur_corr_dju DOUBLE PRECISION,
    conso_reseau_chaleur_surfacique DOUBLE PRECISION,
    -- conso_reseau_chaleur_surfacique_corr_dju DOUBLE PRECISION,
    facture_reseau_chaleur_htva DOUBLE PRECISION,
    facture_reseau_chaleur_ttc DOUBLE PRECISION,
    -- Réseau de froid
    conso_reseau_froid DOUBLE PRECISION,
    conso_reseau_froid_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_reseau_froid_corr_dju DOUBLE PRECISION,
    conso_reseau_froid_surfacique DOUBLE PRECISION,
    -- conso_reseau_froid_surfacique_corr_dju DOUBLE PRECISION,
    facture_reseau_froid_htva DOUBLE PRECISION,
    facture_reseau_froid_ttc DOUBLE PRECISION,
    -- Fioul
    -- conso_fioul_pcs DOUBLE PRECISION,
    conso_fioul_pci DOUBLE PRECISION,
    conso_fioul_pci_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_fioul_corr_dju DOUBLE PRECISION,
    conso_fioul_surfacique DOUBLE PRECISION,
    -- conso_fioul_surfacique_corr_dju DOUBLE PRECISION,
    facture_fioul_htva DOUBLE PRECISION,
    facture_fioul_ttc DOUBLE PRECISION,
    -- Granule de bois et biomasse
    conso_granule_bois DOUBLE PRECISION,
    conso_granule_bois_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_biomasse_corr_dju DOUBLE PRECISION,
    -- conso_biomasse_surfacique DOUBLE PRECISION,
    -- conso_biomasse_surfacique_corr_dju DOUBLE PRECISION,
    facture_granule_bois_htva DOUBLE PRECISION,
    facture_granule_bois_ttc DOUBLE PRECISION,
    -- Propane
    conso_propane DOUBLE PRECISION,
    conso_propane_surfacique DOUBLE PRECISION,
    -- conso_surfacique_propane DOUBLE PRECISION,
    facture_propane_htva DOUBLE PRECISION,
    facture_propane_ttc DOUBLE PRECISION,
    -- Photovolatïque
    conso_photovoltaique DOUBLE PRECISION,
    conso_surfacique_photovoltaique DOUBLE PRECISION,
    facture_photovoltaique_ht DOUBLE PRECISION,
    facture_photovoltaique_ttc DOUBLE PRECISION,
    -- Complétude des données
    conso_presente_elec INT NOT NULL,
    conso_presente_granule_bois INT NOT NULL,
    conso_presente_fioul_pci INT NOT NULL,
    conso_presente_propane INT NOT NULL,
    conso_presente_reseau_chaleur INT NOT NULL,
    conso_presente_reseau_froid INT NOT NULL,
    conso_presente_gaz_pcs INT NOT NULL,
    conso_presente_photovoltaique INT NOT NULL,
    conso_presente_eau INT NOT NULL,
    ligne_avec_conso BOOLEAN NOT NULL,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire, date_conso)
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS siep.conso_mensuelle_brute_unpivot CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_mensuelle_brute_unpivot (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    annee_conso INT,
    date_conso DATE NULL,
    type_energie TEXT,
    conso_brute DOUBLE PRECISION,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire, date_conso, type_energie)
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS siep.conso_mensuelle_corr_unpivot CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_mensuelle_corr_unpivot (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    annee_conso INT,
    date_conso DATE NULL,
    type_energie TEXT,
    conso_corr_dju_mmsi DOUBLE PRECISION,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire, date_conso, type_energie)
) PARTITION BY RANGE (import_timestamp);


/*
    TABLES ANNUELLES
*/

DROP TABLE IF EXISTS siep.conso_annuelle CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_annuelle (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    annee INT,
    -- Elec
    conso_elec DOUBLE PRECISION,
    conso_elec_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_elec_corr_dju DOUBLE PRECISION,
    conso_elec_surfacique DOUBLE PRECISION,
    -- conso_elec_surfacique_corr_dju DOUBLE PRECISION,
    facture_elec_ht DOUBLE PRECISION,
    facture_elec_ttc DOUBLE PRECISION,
    -- Gaz
    conso_gaz_pcs DOUBLE PRECISION,
    conso_gaz_pci DOUBLE PRECISION,
    conso_gaz_pci_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_gaz_corr_dju DOUBLE PRECISION,
    conso_gaz_surfacique DOUBLE PRECISION,
    -- conso_gaz_surfacique_corr_dju DOUBLE PRECISION,
    facture_gaz_ht DOUBLE PRECISION,
    facture_gaz_ttc DOUBLE PRECISION,
    -- Eau
    conso_eau DOUBLE PRECISION,
    facture_eau_htva DOUBLE PRECISION,
    facture_eau_ttc DOUBLE PRECISION,
    -- Réseau de chaleur
    conso_reseau_chaleur DOUBLE PRECISION,
    conso_reseau_chaleur_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_reseau_chaleur_corr_dju DOUBLE PRECISION,
    conso_reseau_chaleur_surfacique DOUBLE PRECISION,
    -- conso_reseau_chaleur_surfacique_corr_dju DOUBLE PRECISION,
    facture_reseau_chaleur_htva DOUBLE PRECISION,
    facture_reseau_chaleur_ttc DOUBLE PRECISION,
    -- Réseau de froid
    conso_reseau_froid DOUBLE PRECISION,
    conso_reseau_froid_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_reseau_froid_corr_dju DOUBLE PRECISION,
    conso_reseau_froid_surfacique DOUBLE PRECISION,
    -- conso_reseau_froid_surfacique_corr_dju DOUBLE PRECISION,
    facture_reseau_froid_htva DOUBLE PRECISION,
    facture_reseau_froid_ttc DOUBLE PRECISION,
    -- Fioul
    -- conso_fioul_pcs DOUBLE PRECISION,
    conso_fioul_pci DOUBLE PRECISION,
    conso_fioul_pci_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_fioul_corr_dju DOUBLE PRECISION,
    conso_fioul_surfacique DOUBLE PRECISION,
    -- conso_fioul_surfacique_corr_dju DOUBLE PRECISION,
    facture_fioul_htva DOUBLE PRECISION,
    facture_fioul_ttc DOUBLE PRECISION,
    -- Granule de bois et biomasse
    conso_granule_bois DOUBLE PRECISION,
    conso_granule_bois_corr_dju_mmsi DOUBLE PRECISION,
    -- conso_biomasse_corr_dju DOUBLE PRECISION,
    -- conso_biomasse_surfacique DOUBLE PRECISION,
    -- conso_biomasse_surfacique_corr_dju DOUBLE PRECISION,
    facture_granule_bois_htva DOUBLE PRECISION,
    facture_granule_bois_ttc DOUBLE PRECISION,
    -- Propane
    conso_propane DOUBLE PRECISION,
    conso_propane_surfacique DOUBLE PRECISION,
    facture_propane_htva DOUBLE PRECISION,
    facture_propane_ttc DOUBLE PRECISION,
    -- Photovolatïque
    conso_photovoltaique DOUBLE PRECISION,
    conso_surfacique_photovoltaique DOUBLE PRECISION,
    facture_photovoltaique_ht DOUBLE PRECISION,
    facture_photovoltaique_ttc DOUBLE PRECISION,
    -- Complétude des données
    conso_presente_elec INT NOT NULL,
    conso_presente_granule_bois INT NOT NULL,
    conso_presente_fioul_pci INT NOT NULL,
    conso_presente_propane INT NOT NULL,
    conso_presente_reseau_chaleur INT NOT NULL,
    conso_presente_reseau_froid INT NOT NULL,
    conso_presente_gaz_pcs INT NOT NULL,
    conso_presente_photovoltaique INT NOT NULL,
    conso_presente_eau INT NOT NULL,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire, annee)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS siep.conso_annuelle_unpivot CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_annuelle_unpivot (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    annee INT,
    fluide TEXT,
    type_conso TEXT,
    conso DOUBLE PRECISION,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire, annee, fluide, type_conso)
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS siep.conso_annuelle_unpivot_comparaison CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_annuelle_unpivot_comparaison (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    annee INT,
    annee_comparaison INT,
    fluide TEXT,
    type_conso TEXT,
    conso DOUBLE PRECISION,
    conso_comparaison DOUBLE PRECISION,
    diff_vs_comparaison DOUBLE PRECISION,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire, annee, annee_comparaison, fluide, type_conso)
) PARTITION BY RANGE (import_timestamp);


DROP TABLE IF EXISTS siep.conso_statut_par_fluide CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_statut_par_fluide (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    annee INT,
    code_bat_gestionnaire TEXT,
    type_fluide TEXT,
    conso_presente INT,
    statut_du_fluide TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, annee, code_bat_gestionnaire, type_fluide)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS siep.conso_avant_2019 CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_avant_2019 (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    statut_conso_avant_2019 BOOLEAN,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS siep.conso_statut_fluide_global CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_statut_fluide_global (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_gestionnaire TEXT,
    statut_elec TEXT,
    statut_gaz TEXT,
    statut_reseau_chaleur TEXT,
    statut_reseau_froid TEXT,
    statut_fluide_global TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_gestionnaire)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS siep.conso_statut_batiment CASCADE;
CREATE TABLE IF NOT EXISTS siep.conso_statut_batiment (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    annee INT,
    code_bat_gestionnaire TEXT,
    statut_batiment TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, annee, code_bat_gestionnaire)
) PARTITION BY RANGE (import_timestamp);
