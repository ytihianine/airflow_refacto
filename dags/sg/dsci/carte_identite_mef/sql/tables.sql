CREATE SCHEMA IF NOT EXISTS dsci;

/*  =========
EFFECTIFS
=========== */
DROP TABLE IF EXISTS dsci.effectif_perimetre;
CREATE TABLE IF NOT EXISTS dsci.effectif_perimetre (
    id SERIAL PRIMARY KEY,
    type_etp TEXT,
    annee INTEGER,
    type_budget TEXT,
    perimetre TEXT,
    valeur FLOAT
);

DROP TABLE IF EXISTS dsci.effectif_direction;
CREATE TABLE IF NOT EXISTS dsci.effectif_direction (
    id SERIAL PRIMARY KEY,
    annee INTEGER,
    direction TEXT,
    nombre_agents INTEGER
);

DROP TABLE IF EXISTS dsci.effectif_direction_perimetre;
CREATE TABLE IF NOT EXISTS dsci.effectif_direction_perimetre (
    id SERIAL PRIMARY KEY,
    annee INT,
    source TEXT,
    direction TEXT,
    etp INTEGER,
    etp_ac INTEGER,
    etp_sd INTEGER,
    etp_scn INTEGER,
    commentaire TEXT
);

DROP TABLE IF EXISTS dsci.plafond_etpt;
CREATE TABLE IF NOT EXISTS dsci.plafond_etpt (
    id SERIAL PRIMARY KEY,
    source TEXT,
    annee INTEGER,
    designation_ministere_ou_budget_annexe TEXT,
    plafond_en_etpt FLOAT,
    part_du_total FLOAT
);

DROP TABLE IF EXISTS dsci.db_plafond_etpt;
CREATE TABLE IF NOT EXISTS dsci.db_plafond_etpt (
    id SERIAL PRIMARY KEY,
    source TEXT,
    annee INTEGER,
    type_budgetaire TEXT,
    type_de_valeur TEXT,
    type_de_budget TEXT,
    designation_ministere_ou_budget_annexe TEXT,
    valeur FLOAT,
    part_du_total FLOAT,
    unite TEXT
);

DROP TABLE IF EXISTS dsci.effectifs_departements;
CREATE TABLE IF NOT EXISTS dsci.effectifs_departements (
    id SERIAL PRIMARY KEY,
    departement TEXT,
    effectif INT
    annee INTEGER
);

/*  =========
BUDGETS
=========== */
DROP TABLE IF EXISTS dsci.budget_total;
CREATE TABLE IF NOT EXISTS dsci.budget_total (
    id SERIAL PRIMARY KEY,
    libelle TEXT,
    somme_cp_t2 BIGINT,
    somme_cp_ht2 BIGINT,
    somme_cp_t2_ht2_bt BIGINT,
    part_du_total FLOAT,
    annee INTEGER,
    type_budget TEXT
);

DROP TABLE IF EXISTS dsci.budget_general;
CREATE TABLE IF NOT EXISTS dsci.budget_general (
    id SERIAL PRIMARY KEY,
    libelle TEXT,
    somme_cp_t2 BIGINT,
    somme_cp_ht2 BIGINT,
    somme_cp_t2_ht2 BIGINT,
    part_du_total FLOAT,
    annee INTEGER,
    type_budget TEXT

);

DROP TABLE IF EXISTS dsci.evolution_budget_mef;
CREATE TABLE IF NOT EXISTS dsci.evolution_budget_mef (
    id SERIAL PRIMARY KEY,
    entite TEXT,
    annee INTEGER,
    type_budget TEXT,
    montant_budget BIGINT,
    rang INTEGER
);

DROP TABLE IF EXISTS dsci.evolution_budget_mef;
CREATE TABLE  IF NOT EXISTS dsci.evolution_budget_mef (
    entite TEXT,
    annee INT,
    type_budget TEXT,
    montant_budget DECIMAL(10, 2),
    rang INT
);

-- DROP TABLE IF EXISTS dsci.montant_intervention_invest;
-- CREATE TABLE IF NOT EXISTS dsci.montant_intervention_invest (
--     id SERIAL PRIMARY KEY,
--     source TEXT,
--     ministere TEXT,
--     type_montant TEXT,
--     montant BIGINT,
--     part_montant FLOAT
-- );
DROP TABLE IF EXISTS dsci.montant_intervention_invest;
CREATE TABLE IF NOT EXISTS dsci.montant_intervention_invest (
    id SERIAL PRIMARY KEY,
    annee INTEGER,
    source_montant TEXT,
    ministere TEXT,
    type_montant TEXT,
    montant BIGINT,
    part_montant FLOAT
);

DROP TABLE IF EXISTS dsci.budget_pilotable;
CREATE TABLE IF NOT EXISTS dsci.budget_pilotable (
    id SERIAL PRIMARY KEY,
    source TEXT,
    ministere TEXT,
    montant_pde FLOAT
);

DROP TABLE IF EXISTS dsci.masse_salariale;
CREATE TABLE IF NOT EXISTS dsci.masse_salariale (
    id SERIAL PRIMARY KEY,
    annee INTEGER,
    phase_budgetaire TEXT,
    type_budget TEXT,
    ministere_simplifie TEXT,
    designation_ministere_ou_compte TEXT,
    masse_salariale BIGINT
);

CREATE TABLE IF NOT EXISTS dsci.budget_ministere (
    id SERIAL PRIMARY KEY,
    annee INTEGER,
    phase_budgetaire TEXT,
    type_credit TEXT,
    ministere_simplifie TEXT,
    ministere TEXT,
    budget_general BIGINT,
    budget_annexe BIGINT,
    compte_affection_speciale BIGINT,
    compte_concours_financiers BIGINT,
    budget_total BIGINT
);


/*  =========
CLIMAT SOCIAL
=========== */
DROP TABLE IF EXISTS dsci.engagement_agent;
CREATE TABLE IF NOT EXISTS dsci.engagement_agent (
    id SERIAL PRIMARY KEY,
    indicateur TEXT,
    annee INTEGER,
    valeur FLOAT
);

DROP TABLE IF EXISTS dsci.election_resultat;
CREATE TABLE IF NOT EXISTS dsci.election_resultat (
    id SERIAL PRIMARY KEY,
    syndicat TEXT,
    taux_participation FLOAT
);


DROP TABLE IF EXISTS dsci.old_teletravail;
CREATE TABLE IF NOT EXISTS dsci.old_teletravail (
    id SERIAL PRIMARY KEY,
    date_relevee DATE,
    dgfip FLOAT,
    dgddi FLOAT,
    ac FLOAT,
    insee FLOAT,
    dgccrf FLOAT,
    ministeriel FLOAT,
    source TEXT,
    commentaire TEXT
);

DROP TABLE IF EXISTS dsci.teletravail;
CREATE TABLE IF NOT EXISTS dsci.teletravail (
    id SERIAL PRIMARY KEY,
    date DATE,
    taux_tt DOUBLE PRECISION
);

DROP TABLE IF EXISTS dsci.teletravail_frequence;
CREATE TABLE IF NOT EXISTS dsci.teletravail_frequence (
    id SERIAL PRIMARY KEY,
    annee INT,
    raison TEXT,
    valeur INT
);

DROP TABLE IF EXISTS dsci.teletravail_opinion;
CREATE TABLE IF NOT EXISTS dsci.teletravail_opinion (
    id SERIAL PRIMARY KEY,
    annee INT,
    opinion TEXT,
    valeur INT
);
