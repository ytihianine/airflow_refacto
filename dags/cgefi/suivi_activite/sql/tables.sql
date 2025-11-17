DROP SCHEMA IF EXISTS cgefi_poc CASCADE;
CREATE SCHEMA IF NOT EXISTS cgefi_poc;

-- table: ref_type_organisme
CREATE TABLE cgefi_poc.ref_type_organisme (
	id SERIAL PRIMARY KEY,
	type_organisme TEXT
);

-- table: ref_secteur_professionnel
CREATE TABLE cgefi_poc.ref_secteur_professionnel (
	id SERIAL PRIMARY KEY,
	secteur_professionnel TEXT
);

-- table: ref_actions
CREATE TABLE cgefi_poc.ref_actions (
	id SERIAL PRIMARY KEY,
	type_action TEXT
);

-- table: ref_type_de_frais
CREATE TABLE cgefi_poc.ref_type_de_frais (
	id SERIAL PRIMARY KEY,
	type_de_frais TEXT
);

-- table: ref_formation_specialite
CREATE TABLE cgefi_poc.ref_formation_specialite (
	id SERIAL PRIMARY KEY,
	code INTEGER,
	intitule TEXT
);

-- table: ref_sous_type_de_frais
CREATE TABLE cgefi_poc.ref_sous_type_de_frais (
	id SERIAL PRIMARY KEY,
	id_type_de_frais INTEGER REFERENCES cgefi_poc.ref_type_de_frais(id),
	sous_type_de_frais TEXT
);

-- table: ref_actif_type
CREATE TABLE cgefi_poc.ref_actif_type (
	id SERIAL PRIMARY KEY,
	actif TEXT
);

-- table: ref_actif_sous_type
CREATE TABLE cgefi_poc.ref_actif_sous_type (
	id SERIAL PRIMARY KEY,
	id_type_actif INTEGER REFERENCES cgefi_poc.ref_actif_type(id),
	actif_sous_type TEXT
);


-- table: ref_passif_type
CREATE TABLE cgefi_poc.ref_passif_type (
	id SERIAL PRIMARY KEY,
	type_passif TEXT
);

-- table: ref_passif_sous_type
CREATE TABLE cgefi_poc.ref_passif_sous_type (
	id SERIAL PRIMARY KEY,
	id_type_passif INTEGER REFERENCES cgefi_poc.ref_passif_type(id),
	sous_type_passif TEXT
);


-- table: region_atpro
CREATE TABLE cgefi_poc.region_atpro (
	id SERIAL PRIMARY KEY,
	region TEXT,
	nom_generique TEXT
);

-- table: organisme
CREATE TABLE cgefi_poc.organisme (
	id SERIAL PRIMARY KEY,
	libelle_court TEXT,
	libelle_long TEXT
);

-- table: organisme_type
CREATE TABLE cgefi_poc.organisme_type (
	id SERIAL PRIMARY KEY,
	id_type_organisme INTEGER REFERENCES cgefi_poc.ref_type_organisme(id)
);


-- table: controleur
CREATE TABLE cgefi_poc.controleur (
	id SERIAL PRIMARY KEY,
	mail TEXT,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id)
);


-- table: process_6_a01
CREATE TABLE cgefi_poc.process_6_a01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	contributions_reverses_a_france_competences FLOAT,
	nombre_d_adherents INTEGER,
	montant_des_contributions_percues FLOAT
);

-- table: process_6_d01
CREATE TABLE cgefi_poc.process_6_d01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	type_depense TEXT,
	collecte_comptabilisee FLOAT,
	decaissements FLOAT,
	assiettes_des_frais FLOAT,
	taux_applicables_plafond FLOAT,
	plafond_des_frais FLOAT,
	frais_realises FLOAT,
	ratio_frais_plafond FLOAT
);

-- table: process_6_e01
CREATE TABLE cgefi_poc.process_6_e01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	type_d_actions TEXT,
	nombre_de_stagiaires INTEGER,
	nombres_d_heures_stagiaires FLOAT,
	couts_pedagogiques FLOAT,
	couts_annexes FLOAT
);

-- table: process_6_g02
CREATE TABLE cgefi_poc.process_6_g02 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	type_d_actions TEXT,
	categorie_duree TEXT,
	nb_formation INTEGER
);

-- table: process_6_h01
CREATE TABLE cgefi_poc.process_6_h01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	categorie_du_personnel TEXT,
	moins_de_25_ans INTEGER,
	de_25_a_34_ans INTEGER,
	de_35_a_44_ans INTEGER,
	de_45_a_50_ans INTEGER,
	c51_ans_et_plus INTEGER,
	non_repartis INTEGER
);

-- table: process_4_b01
CREATE TABLE cgefi_poc.process_4_b01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id)
);

-- table: process_4_c01
CREATE TABLE cgefi_poc.process_4_c01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	type_versements TEXT,
	categorie TEXT,
	moins_de_11 INTEGER,
	de_11_a_19 INTEGER,
	de_20_a_49 INTEGER,
	de_50_a_199 INTEGER,
	de_200_a_249 INTEGER,
	de_250_a_499 INTEGER,
	de_500_a_1999 INTEGER
);

-- table: process_4_b02
CREATE TABLE cgefi_poc.process_4_b02 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id)
);

-- table: process_4_e01
CREATE TABLE cgefi_poc.process_4_e01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	restant_a_financer TEXT,
	montants FLOAT
);

-- table: process_4_e02
CREATE TABLE cgefi_poc.process_4_e02 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	charge_a_payer_pour_engagements_de_financement_de_formation TEXT,
	montants FLOAT
);

-- table: process_4_g01
CREATE TABLE cgefi_poc.process_4_g01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id)
);

-- table: process_4_e11
CREATE TABLE cgefi_poc.process_4_e11 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	restants_a_financer TEXT,
	montants FLOAT
);

-- table: process_4_g02
CREATE TABLE cgefi_poc.process_4_g02 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id)
);

-- table: process_4_h01
CREATE TABLE cgefi_poc.process_4_h01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id)
);

-- table: process_6_b01
CREATE TABLE cgefi_poc.process_6_b01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	type_personnel TEXT,
	effectifs INTEGER,
	dont_permanent INTEGER,
	etp FLOAT,
	masse_salariale_brute FLOAT,
	charges_patronales FLOAT
);

-- table: process_6_g03
CREATE TABLE cgefi_poc.process_6_g03 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	type_d_actions TEXT,
	niveau_formation TEXT,
	nb_formation INTEGER
);

-- table: process_atpro_a01
CREATE TABLE cgefi_poc.process_atpro_a01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_region INTEGER REFERENCES cgefi_poc.region_atpro(id),
	projet_de_transition_professionnelle TEXT,
	nombre_de_projets_deposes INTEGER,
	nombre_de_projets_recevables INTEGER,
	nombre_de_projets_eligibles INTEGER,
	nombre_de_projets_pris_en_charge INTEGER,
	montant_des_couts_pedagogiques_des_projets_pris_en_charge FLOAT,
	montant_des_remunerations_des_projets_pris_en_charge FLOAT,
	montant_des_frais_annexes_des_projets_pris_en_charge FLOAT,
	montant_des_frais_engages_somme_h_i_j_ FLOAT,
	montant_net_des_frais_engages FLOAT,
	montant_net_des_frais_engages_avec_annulations_probables FLOAT,
	duree_totale_des_formations_prise_en_charge INTEGER
);


-- table: process_atpro_f01
CREATE TABLE cgefi_poc.process_atpro_f01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_region INTEGER REFERENCES cgefi_poc.region_atpro(id),
	id_type_actif INTEGER,
	id_sous_type_actif INTEGER,
	ptp FLOAT,
	cif_cdi FLOAT,
	cif_cdd FLOAT,
	moyens_communs FLOAT
);

-- table: process_atpro_g02
CREATE TABLE cgefi_poc.process_atpro_g02 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_region INTEGER REFERENCES cgefi_poc.region_atpro(id),
	id_compte INTEGER,
	id_detail INTEGER,
	cif_cdd DOUBLE PRECISION,
	cif_cdi DOUBLE PRECISION,
	ptp DOUBLE PRECISION
);

-- table: process_atpro_h01
CREATE TABLE cgefi_poc.process_atpro_h01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_region INTEGER REFERENCES cgefi_poc.region_atpro(id),
	restant_a_financer TEXT,
	montants FLOAT
);

-- table: process_atpro_j01
CREATE TABLE cgefi_poc.process_atpro_j01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_region INTEGER REFERENCES cgefi_poc.region_atpro(id),
	plus_ou_moins_value_latente FLOAT
);

-- table: process_atpro_f02
CREATE TABLE cgefi_poc.process_atpro_f02 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_region INTEGER REFERENCES cgefi_poc.region_atpro(id),
	id_type_passif INTEGER REFERENCES cgefi_poc.ref_passif_type(id),
	id_sous_type_passif INTEGER REFERENCES cgefi_poc.ref_passif_sous_type(id),
	ptp FLOAT,
	cif_cdi FLOAT,
	cif_cdd FLOAT,
	moyens_communs FLOAT
);

-- table: process_4_a01
CREATE TABLE cgefi_poc.process_4_a01 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	id_type_actif INTEGER REFERENCES cgefi_poc.ref_actif_type(id),
	id_sous_type_actif INTEGER REFERENCES cgefi_poc.ref_actif_sous_type(id),
	plan_developpement_competences_moins_de_50 FLOAT,
	dont_pigistes FLOAT,
	dont_plan_developpement_competences_intermittents FLOAT,
	alternance FLOAT,
	compte_personnel_de_formation FLOAT,
	versements_conventionnels FLOAT,
	versements_volontaires FLOAT,
	non_salaries FLOAT,
	moyens_communs FLOAT,
	plan_de_formation_50_a_299_salaries FLOAT,
	cif_cdi FLOAT,
	cif_cdd FLOAT,
	fpspp_france_competences FLOAT
);

-- table: process_4_a02
CREATE TABLE cgefi_poc.process_4_a02 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	id_type_actif INTEGER REFERENCES cgefi_poc.ref_actif_type(id),
	id_sous_type_actif INTEGER REFERENCES cgefi_poc.ref_actif_sous_type(id),
	plan_developpement_competences_moins_de_50 FLOAT,
	dont_pigistes FLOAT,
	dont_plan_developpement_competences_intermittents FLOAT,
	alternance FLOAT,
	compte_personnel_de_formation FLOAT,
	versements_conventionnels FLOAT,
	versements_volontaires FLOAT,
	non_salaries FLOAT,
	moyens_communs FLOAT,
	plan_de_formation_50_a_299_salaries FLOAT,
	cif_cdi FLOAT,
	cif_cdd FLOAT,
	fpspp_france_competences FLOAT
);

-- table: process_4_h02
CREATE TABLE cgefi_poc.process_4_h02 (
	id SERIAL PRIMARY KEY,
	annee INTEGER,
	id_organisme INTEGER REFERENCES cgefi_poc.organisme(id),
	id_type_de_frais INTEGER REFERENCES cgefi_poc.ref_type_de_frais(id),
	id_sous_type_de_frais INTEGER REFERENCES cgefi_poc.ref_sous_type_de_frais(id),
	decomposition_des_charges_de_fonctionnement FLOAT,
	dont_frais_de_la_com FLOAT
);
