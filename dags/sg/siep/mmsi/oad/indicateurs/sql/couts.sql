DROP TABLE IF EXISTS temporaire.tmp_bien_couts;
DROP TABLE IF EXISTS siep.bien_couts;
CREATE TABLE IF NOT EXISTS siep.bien_couts (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    annee_charges_copropriete INTEGER,
    annee_charges_locatives INTEGER,
    annee_loyers_budgetaires INTEGER,
    annee1_charges_fonct INTEGER,
    annee1_loyer_ht_hc_ttc FLOAT,
    annee2_charges_fonc FLOAT,
    annee2_loyer_ht_hc_ttc FLOAT,
    charges_copropriete FLOAT,
    charges_fonc_annee2 FLOAT,
    charges_fonc_annee1 FLOAT,
    charges_locatives FLOAT,
    codhc_surfacique_sub FLOAT,
    loyer_surfacique_sub FLOAT,
    loyer_annuel_ht_annee1 FLOAT,
    loyer_annuel_ht_annee2 FLOAT,
    loyer_budgetaire_ttc FLOAT,
    loyer_hc_ttc_annee1 FLOAT,
    loyer_hc_ttc_annee2 FLOAT,
    loyers_budgetaires_2018 FLOAT,
    plafond_loyer_surfacique_sub FLOAT
);
