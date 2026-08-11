-- Scripts SQL générés automatiquement depuis fichiers Parquet
-- ======================================================================

DROP SCHEMA IF EXISTS donnee_comptable CASCADE;
CREATE SCHEMA IF NOT EXISTS donnee_comptable;
-- Table générée depuis: zjdp.parquet
DROP TABLE donnee_comptable."demande_paiement_journal_pieces" CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_journal_pieces (
	"id_row" bigint GENERATED ALWAYS AS IDENTITY,
	annee_exercice int,
	annee_exercice_piece_fi int,
	num_piece_reference bigint,
	societe text,
	centre_financier text,
	centre_cout text,
	id_dp text,
	cf_cc text,
	id_dp_cf_cc text,
	nb_poste int,
	nb_unique_cf_cc int,
	unique_multi text,
	montant_poste float,
	texte_de_poste text,
	import_timestamp TIMESTAMP NOT NULL,
	snapshot_id UUID NOT NULL,
	snapshot_id_parent UUID NULL,
	PRIMARY KEY ("id_row")
) PARTITION BY RANGE (import_timestamp);


-- Table générée depuis: zdep61.parquet
DROP TABLE donnee_comptable."demande_paiement_carte_achat" CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_carte_achat (
	"id_row" bigint GENERATED ALWAYS AS IDENTITY,
	num_piece_dp_carte_achat int,
	societe text,
	annee_exercice int,
	automatisation_wf_cpt text,
	niveau_carte_achat text,
	statut_dp_carte_achat text,
	id_dp text,
	import_timestamp TIMESTAMP NOT NULL,
	snapshot_id UUID NOT NULL,
	snapshot_id_parent UUID NULL,
	PRIMARY KEY ("id_row")
) PARTITION BY RANGE (import_timestamp);

-- Table générée depuis: infdep56.parquet
DROP TABLE donnee_comptable."delai_global_paiement" CASCADE;
CREATE TABLE donnee_comptable.delai_global_paiement (
	"id_row" bigint GENERATED ALWAYS AS IDENTITY,
	id_dgp uuid,
	societe text,
	annee_exercice int,
	type_piece text,
	nature_sous_nature text,
	centre_financier text,
	centre_cout text,
	service_executant text,
	nb_dp_dgp int,
	montant_dp_dgp float8 NULL,
	delai_global_paiement float8 NULL,
	cf_cc text,
	mois int,
	mois_nom text,
	mois_nombre_nom text,
	import_timestamp TIMESTAMP NOT NULL,
	snapshot_id UUID NOT NULL,
	snapshot_id_parent UUID NULL,
	PRIMARY KEY ("id_row")
) PARTITION BY RANGE (import_timestamp);

-- Table générée depuis: infbud57.parquet
DROP TABLE donnee_comptable."demande_achat" CASCADE;
CREATE TABLE donnee_comptable.demande_achat (
	"id_row" bigint GENERATED ALWAYS AS IDENTITY,
	id_da int,
	nb_id_da int,
	unique_multi text,
	date_creation_da timestamp not null,
	annee_exercice int,
	centre_financier text,
	centre_cout text,
	date_replication timestamp not null,
	montant_cumule_postes_da_traites float8 NULL,
	delai_traitement_da float8 NULL,
	cf_cc text,
	mois int,
	mois_nom text,
	mois_nombre_nom text,
	delai_traitement_classe varchar(255) NULL,
	import_timestamp TIMESTAMP NOT NULL,
	snapshot_id UUID NOT NULL,
	snapshot_id_parent UUID NULL,
	PRIMARY KEY ("id_row")
) PARTITION BY RANGE (import_timestamp);

-- Table générée depuis: infbud55.parquet
DROP TABLE donnee_comptable."demande_paiement_flux" CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_flux (
	"id_row" bigint GENERATED ALWAYS AS IDENTITY,
	id_dp text,
	num_dp_flux bigint,
	annee_exercice int,
	societe text,
	dp_flux_3 text,
	import_timestamp TIMESTAMP NOT NULL,
	snapshot_id UUID NOT NULL,
	snapshot_id_parent UUID NULL,
	PRIMARY KEY ("id_row")
) PARTITION BY RANGE (import_timestamp);


-- Table générée depuis: zsfp_suivi.parquet
DROP TABLE donnee_comptable."demande_paiement_sfp" CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_sfp (
	"id_row" bigint GENERATED ALWAYS AS IDENTITY,
	num_piece_sfp int,
	societe text,
	annee_exercice int,
	num_ej_sfp float8 NULL,
	type_flux_sfp text,
	statut_sfp text,
	automatisation_wf_cpt text,
	id_dp text,
	import_timestamp TIMESTAMP NOT NULL,
	snapshot_id UUID NOT NULL,
	snapshot_id_parent UUID NULL,
	PRIMARY KEY ("id_row")
) PARTITION BY RANGE (import_timestamp);


-- Table générée depuis: zlisteej.parquet
DROP TABLE donnee_comptable."engagement_juridique" CASCADE;
CREATE TABLE donnee_comptable.engagement_juridique (
	"id_row" bigint GENERATED ALWAYS AS IDENTITY,
	id_ej bigint,
	type_ej text,
	orga text,
	gac text,
	montant_ej float8 NULL,
	date_creation_ej timestamp not null,
	centre_financier text,
	centre_cout text,
	cf_cc text,
	ej_cf_cc text,
	annee_exercice int,
	mois int,
	mois_nom text,
	mois_nombre_nom text,
	nb_poste_ej int,
	unique_multi text,
	type_ej_nom text,
	import_timestamp TIMESTAMP NOT NULL,
	snapshot_id UUID NOT NULL,
	snapshot_id_parent UUID NULL,
	PRIMARY KEY ("id_row")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE donnee_comptable."demande_paiement" CASCADE;
CREATE TABLE donnee_comptable."demande_paiement" (
	"id_row" bigint GENERATED ALWAYS AS IDENTITY,
	id_dp text,
	annee_exercice int,
	societe text,
	nature_sous_nature float,
	type_piece_dp text,
	centre_financier text,
	date_comptable date,
	num_dp bigint,
	montant_dp float,
	statut_piece text,
	mois int,
	mois_nom text,
	mois_nombre_nom text,
	nat_snat_nom text,
	nat_snat_groupe text,
	import_timestamp TIMESTAMP NOT NULL,
	snapshot_id UUID NOT NULL,
	snapshot_id_parent UUID NULL,
	PRIMARY KEY ("id_row")
) PARTITION BY RANGE (import_timestamp);

-- Table construite
DROP TABLE donnee_comptable.demande_paiement_complet CASCADE;
CREATE TABLE donnee_comptable.demande_paiement_complet (
	"id_row" bigint GENERATED ALWAYS AS IDENTITY,
	id_dp text,
	annee_exercice int,
	societe text,
	nature_sous_nature float,
	type_piece_dp text,
	date_comptable date NULL,
	num_dp bigint,
	montant_dp double precision,
	statut_piece text,
    texte_de_poste text,
	mois int,
	mois_nom text,
	mois_nombre_nom text,
	nat_snat_nom text,
	nat_snat_groupe text,
	annee_exercice_piece_fi bigint,
	num_piece_reference bigint,
	centre_financier_demande_paiement text,
	centre_financier_journal_pieces text,
	centre_financier text,
	centre_cout text,
	cf_cc text,
	id_dp_cf_cc text,
	nb_poste bigint,
	nb_unique_cf_cc int,
	unique_multi text,
	montant_poste_journal_pieces float,
	montant_poste float,
	num_piece_dp_carte_achat bigint,
	automatisation_wf_cpt text,
	niveau_carte_achat text,
	statut_dp_carte_achat text,
	num_dp_flux bigint,
	dp_flux_3 text,
	num_piece_sfp bigint,
	num_ej_sfp bigint,
	type_flux_sfp text,
	flux_3_automatisation_compta text,
	statut_sfp text,
	import_timestamp TIMESTAMP NOT NULL,
	snapshot_id UUID NOT NULL,
	snapshot_id_parent UUID NULL,
	PRIMARY KEY ("id_row")
) PARTITION BY RANGE (import_timestamp);
