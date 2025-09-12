DROP TABLE IF EXISTS cgefi.barometre;
CREATE TABLE IF NOT EXISTS cgefi.barometre (
	id bigserial PRIMARY KEY,
	denomination TEXT,
	sigle TEXT,
	siren TEXT,
	date_creation_organisme TEXT,
	date_modification_organisme TEXT,
	publication_afs TEXT,
	afs_fiche_rh_detaillee TEXT,
	cartographie_attendue INTEGER,
	efc_attendu INTEGER,
	organisme_sensible BOOLEAN,
	statut_juridique TEXT,
	operateur_etat TEXT,
	etat TEXT,
	date_archivage TEXT,
	entite TEXT,
	type_controle TEXT,
	autorite TEXT,
	date_de_fin TEXT,
	controleur TEXT,
	fiche_signaletique INTEGER,
	fiche_rh INTEGER,
	bilan INTEGER,
	compte_de_resultat INTEGER,
	fiche_financiere INTEGER,
	carto INTEGER,
	efc INTEGER,
	recos INTEGER,
	rapport_annuel_attendu BOOLEAN,
	date_du_retour_attendue DATE,
	date_du_retour DATE,
	type_rapport TEXT,
	hors_corpus BOOLEAN
);

CREATE TABLE IF NOT EXISTS cgefi.sigle_mission (
	id bigserial PRIMARY KEY,
	mission_actuelle TEXT,
	entite_de_controle TEXT,
	sigle TEXT
);

DROP TABLE IF EXISTS cgefi.organisme;
CREATE TABLE IF NOT EXISTS cgefi.organisme (
	id bigserial PRIMARY KEY,
	denomination TEXT,
	sigle TEXT,
	siren BIGINT,
	date_creation_organisme DATE,
	date_modification_organisme DATE,
	publication_afs BOOLEAN,
	afs_fiche_rh_detaillee BOOLEAN,
	organisme_sensible BOOLEAN,
	statut_juridique TEXT,
	operateur_etat BOOLEAN,
	etat TEXT,
	date_archivage DATE,
	entite TEXT,
	type_controle TEXT,
	autorite TEXT,
	date_de_fin DATE,
	hors_corpus BOOLEAN
);

DROP TABLE IF EXISTS cgefi.organisme_cartographie;
CREATE TABLE IF NOT EXISTS cgefi.organisme_cartographie (
	id bigserial PRIMARY KEY,
	denomination TEXT,
	sigle TEXT,
	siren BIGINT,
	cartographie_attendue BOOLEAN,
	cartographie_recue BOOLEAN
);

DROP TABLE IF EXISTS cgefi.organisme_efc;
CREATE TABLE IF NOT EXISTS cgefi.organisme_efc (
	id bigserial PRIMARY KEY,
	denomination TEXT,
	sigle TEXT,
	siren BIGINT,
	efc_attendu BOOLEAN,
	date_efc_recu DATE,
	efc_recu BOOLEAN
);

DROP TABLE IF EXISTS cgefi.organisme_fiche_signaletique;
CREATE TABLE IF NOT EXISTS cgefi.organisme_fiche_signaletique (
	id bigserial PRIMARY KEY,
	denomination TEXT,
	sigle TEXT,
	siren BIGINT,
	effectifs_totaux FLOAT,
	immobilise_net FLOAT,
	circulant_hors_tresorerie FLOAT,
	tresorerie FLOAT,
	charges_de_personnels_cpte_64 FLOAT,
	charges_de_fonctionnement_externe FLOAT,
	impots_et_taxes FLOAT,
	interventions FLOAT,
	dotations_amortissement_provisions FLOAT,
	fonds_de_roulement FLOAT,
	en_nb_de_jours_charges_brutes_exploitation FLOAT,
	besoin_en_fonds_de_roulement FLOAT,
	duree_expression_tresorerie_nette_en_jours FLOAT,
	variation_des_emplois_stables FLOAT,
	variation_des_ressources_stables FLOAT,
	fiche_rh BOOLEAN,
	bilan BOOLEAN,
	compte_de_resultat BOOLEAN,
	fiche_financiere BOOLEAN,
	fiche_signaletique BOOLEAN
);

DROP TABLE IF EXISTS cgefi.organisme_rapports_annuels;
CREATE TABLE IF NOT EXISTS cgefi.organisme_rapports_annuels (
	id bigserial PRIMARY KEY,
	denomination TEXT,
	sigle TEXT,
	siren BIGINT,
	type_rapport TEXT,
	date_du_retour_attendue DATE,
	date_du_retour DATE,
	rapport_annuel_recu BOOLEAN
);

DROP TABLE IF EXISTS cgefi.organisme_recommandations;
CREATE TABLE IF NOT EXISTS cgefi.organisme_recommandations (
	id bigserial PRIMARY KEY,
	denomination TEXT,
	sigle TEXT,
	siren BIGINT,
	rubrique_de_rattachement TEXT,
	date_creation DATE,
	date_cible DATE,
	date_de_modification DATE,
	date_de_changement_etat DATE,
	objet_recommandation TEXT,
	code_recommandation	INTEGER,
	support_recommandation TEXT,
	destinataire TEXT,
	suite_et_commentaire TEXT,
	taux_avancement FLOAT,
	finalite TEXT,
	type_de_recommandation TEXT,
	importance_recommandation TEXT
);
