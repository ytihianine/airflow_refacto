DROP TABLE IF EXISTS siep.conso_mensuelle;
CREATE TABLE IF NOT EXISTS siep.conso_mensuelle (
    id BIGSERIAL PRIMARY KEY,
    code_bat_gestionnaire TEXT,
    date_conso DATE,
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
    -- conso_surfacique_propane DOUBLE PRECISION,
    facture_propane_htva DOUBLE PRECISION,
    facture_propane_ttc DOUBLE PRECISION,
    -- Photovolatïque
    conso_photovoltaique DOUBLE PRECISION,
    conso_surfacique_photovoltaique DOUBLE PRECISION,
    facture_photovoltaique_ht DOUBLE PRECISION,
    facture_photovoltaique_ttc DOUBLE PRECISION
);


DROP TABLE IF EXISTS siep.conso_annuelle;
CREATE TABLE IF NOT EXISTS siep.conso_annuelle (
    id BIGSERIAL PRIMARY KEY,
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
    -- conso_surfacique_propane DOUBLE PRECISION,
    facture_propane_htva DOUBLE PRECISION,
    facture_propane_ttc DOUBLE PRECISION,
    -- Photovolatïque
    conso_photovoltaique DOUBLE PRECISION,
    conso_surfacique_photovoltaique DOUBLE PRECISION,
    facture_photovoltaique_ht DOUBLE PRECISION,
    facture_photovoltaique_ttc DOUBLE PRECISION
);



DROP TABLE IF EXISTS siep.conso_statut_par_fluide;
CREATE TABLE IF NOT EXISTS siep.conso_statut_par_fluide (
    id BIGSERIAL PRIMARY KEY,
    code_bat_gestionnaire TEXT,
    type_fluide TEXT,
    -- conso_fluide_depuis_2019 TEXT,
    statut_du_fluide TEXT
);

DROP TABLE IF EXISTS siep.conso_avant_2019;
CREATE TABLE IF NOT EXISTS siep.conso_avant_2019 (
    id BIGSERIAL PRIMARY KEY,
    code_bat_gestionnaire TEXT,
    statut_conso_avant_2019 BOOLEAN
);

DROP TABLE IF EXISTS siep.conso_statut_fluide_global;
CREATE TABLE IF NOT EXISTS siep.conso_statut_fluide_global (
    id BIGSERIAL PRIMARY KEY,
    code_bat_gestionnaire TEXT,
    statut_elec TEXT,
    statut_gaz TEXT,
    statut_reseau_chaleur TEXT,
    statut_reseau_froid TEXT,
    statut_fluide_global TEXT
);

DROP TABLE IF EXISTS siep.conso_statut_batiment;
CREATE TABLE IF NOT EXISTS siep.conso_statut_batiment (
    id BIGSERIAL PRIMARY KEY,
    code_bat_gestionnaire TEXT,
    statut_conso_avant_2019 BOOLEAN,
    statut_fluide_global TEXT,
    statut_batiment TEXT
);


DROP TABLE IF EXISTS siep.conso_mensuelle_brute_unpivot;
CREATE TABLE IF NOT EXISTS siep.conso_mensuelle_brute_unpivot (
    id BIGSERIAL PRIMARY KEY,
    code_bat_gestionnaire TEXT,
    annee_conso INT,
    date_conso DATE,
    type_energie TEXT,
    conso_brute DOUBLE PRECISION
);

DROP TABLE IF EXISTS siep.conso_mensuelle_corr_unpivot;
CREATE TABLE IF NOT EXISTS siep.conso_mensuelle_corr_unpivot (
    id BIGSERIAL PRIMARY KEY,
    code_bat_gestionnaire TEXT,
    annee_conso INT,
    date_conso DATE,
    type_energie TEXT,
    conso_corr_dju_mmsi DOUBLE PRECISION
);
