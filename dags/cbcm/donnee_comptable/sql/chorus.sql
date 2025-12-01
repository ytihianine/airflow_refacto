-- Scripts SQL générés automatiquement depuis fichiers Parquet
-- Date de génération: 2025-12-01 10:13:48.782623
-- ======================================================================

CREATE SCHEMA IF NOT EXISTS donnee_comptable;
-- Table générée depuis: zjdp.parquet
-- Nombre de lignes: 38605
DROP TABLE donnee_comptable."demande_achat_journal_pieces" CASCADE;
CREATE TABLE donnee_comptable."demande_achat_journal_pieces" (
    "id" SERIAL,
    "id_dp_cf_cc" TEXT,
    "id_dp" TEXT,
    "cf_cc" TEXT,
    "annee_exercice" INTEGER,
    "annee_exercice_piece_fi" INTEGER,
    "num_piece_reference" BIGINT,
    "societe" TEXT,
    "centre_financier" TEXT,
    "centre_cout" TEXT,
    "nb_poste" INTEGER,
    "unique_multi" TEXT,
    "import_timestamp" TIMESTAMP,
    "import_date" DATE,
    PRIMARY KEY ("id_dp_cf_cc", "annee_exercice", "import_timestamp")
) PARTITION BY RANGE ("import_timestamp");

-- Table générée depuis: zdep53.parquet
DROP TABLE donnee_comptable."demande_paiement" CASCADE;
CREATE TABLE donnee_comptable."demande_paiement" (
    "id" SERIAL,
    "id_dp" TEXT,
    "annee_exercice" INTEGER,
    "societe" TEXT,
    "nature_sous_nature" DOUBLE PRECISION,
    "type_piece_dp" TEXT,
    "centre_financier" TEXT,
    "date_comptable" TIMESTAMP,
    "num_dp" BIGINT,
    "montant_dp" DOUBLE PRECISION,
    "statut_piece" TEXT,
    "mois" INTEGER,
    "mois_nom" TEXT,
    "nat_snat_nom" TEXT,
    "nat_snat_groupe" TEXT,
    "import_timestamp" TIMESTAMP,
    "import_date" DATE,
    PRIMARY KEY ("id_dp", "annee_exercice", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

-- Table générée depuis: zdep61.parquet
-- Nombre de lignes: 501
DROP TABLE donnee_comptable."demande_paiement_carte_achat" CASCADE;
CREATE TABLE donnee_comptable."demande_paiement_carte_achat" (
    "id" SERIAL,
    "id_dp" TEXT,
    "num_piece_dp_carte_achat" BIGINT,
    "societe" TEXT,
    "annee_exercice" INTEGER,
    "automatisation_wf_cpt" TEXT,
    "niveau_carte_achat" TEXT,
    "statut_dp_carte_achat" TEXT,
    "import_timestamp" TIMESTAMP,
    "import_date" DATE,
    PRIMARY KEY ("id_dp", "annee_exercice", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

-- Table générée depuis: infdep56.parquet
-- Nombre de lignes: 39458
DROP TABLE donnee_comptable."delai_global_paiement" CASCADE;
CREATE TABLE donnee_comptable."delai_global_paiement" (
    "id" BIGINT,
    "societe" TEXT,
    "annee_exercice" INTEGER,
    "periode_comptable" INTEGER,
    "type_piece" TEXT,
    "nature_sous_nature" TEXT,
    "centre_financier" TEXT,
    "centre_cout" TEXT,
    "service_executant" TEXT,
    "nb_dp_dgp" INTEGER,
    "montant_dp_dgp" DOUBLE PRECISION,
    "delai_global_paiement" DOUBLE PRECISION,
    "cf_cc" TEXT,
    "mois_nom" TEXT,
    "import_timestamp" TIMESTAMP,
    "import_date" DATE,
    PRIMARY KEY ("id", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

-- Table générée depuis: infbud57.parquet
-- Nombre de lignes: 17648
DROP TABLE donnee_comptable."demande_achat" CASCADE;
CREATE TABLE donnee_comptable."demande_achat" (
    "id" BIGINT,
    "id_da" BIGINT,
    "date_creation_da" TIMESTAMP,
    "annee_exercice" INTEGER,
    "centre_financier" TEXT,
    "centre_cout" TEXT,
    "date_replication" TIMESTAMP,
    "montant_cumule_postes_da_traites" DOUBLE PRECISION,
    "delai_traitement_da" DOUBLE PRECISION,
    "cf_cc" TEXT,
    "mois" INTEGER,
    "mois_nom" TEXT,
    "mois_nombre" TEXT,
    "delai_traitement_classe" VARCHAR(255),
    "import_timestamp" TIMESTAMP,
    "import_date" DATE,
    PRIMARY KEY ("id", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

-- Table générée depuis: infbud55.parquet
-- Nombre de lignes: 14776
DROP TABLE donnee_comptable."demande_paiement_flux" CASCADE;
CREATE TABLE donnee_comptable."demande_paiement_flux" (
    "id" BIGINT,
    "id_dp" TEXT,
    "num_dp_flux" BIGINT,
    "annee_exercice" INTEGER,
    "societe" TEXT,
    "dp_flux_3" TEXT,
    "import_timestamp" TIMESTAMP,
    "import_date" DATE,
    PRIMARY KEY ("id", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

-- Table générée depuis: zlisteej.parquet
-- Nombre de lignes: 6794
DROP TABLE donnee_comptable."engagement_juridique" CASCADE;
CREATE TABLE donnee_comptable."engagement_juridique" (
    "id_ej" BIGINT,
    "type_ej" TEXT,
    "orga" TEXT,
    "gac" TEXT,
    "montant_ej" DOUBLE PRECISION,
    "date_creation_ej" TIMESTAMP,
    "centre_financier" TEXT,
    "centre_cout" TEXT,
    "cf_cc" TEXT,
    "ej_cf_cc" TEXT,
    "annee_exercice" INTEGER,
    "mois_ej" INTEGER,
    "nb_poste_ej" INTEGER,
    "unique_multi" TEXT,
    "type_ej_nom" TEXT,
    "import_timestamp" TIMESTAMP,
    "import_date" DATE,
    PRIMARY KEY ("id_ej", "annee_exercice", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

-- Table générée depuis: zsfp_suivi.parquet
-- Nombre de lignes: 3186
DROP TABLE donnee_comptable."demande_paiement_sfp" CASCADE;
CREATE TABLE donnee_comptable."demande_paiement_sfp" (
    "id" SERIAL,
    "id_dp" TEXT,
    "num_piece_sfp" BIGINT,
    "societe" TEXT,
    "annee_exercice" INTEGER,
    "num_ej_sfp" DOUBLE PRECISION,
    "type_flux_sfp" TEXT,
    "statut_sfp" TEXT,
    "automatisation_wf_cpt" TEXT,
    "import_timestamp" TIMESTAMP,
    "import_date" DATE,
    PRIMARY KEY ("id_dp", "annee_exercice", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);
