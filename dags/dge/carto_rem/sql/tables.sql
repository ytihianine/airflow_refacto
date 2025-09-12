DROP SCHEMA cartographie_remuneration;
CREATE SCHEMA cartographie_remuneration;

/*
    Référentiels
*/

-- table: ref_niveau_diplome
CREATE TABLE ref_niveau_diplome (
	id integer primary key,
	niveau_diplome TEXT,
	ancien_nom TEXT
);

-- table: ref_experience_pro
CREATE TABLE ref_experience_pro (
	id integer primary key,
	experience_pro TEXT
);

-- table: ref_base_recrutement
CREATE TABLE ref_base_remuneration (
	id integer primary key,
	base_remuneration TEXT
);

-- table: ref_base_revalorisation
CREATE TABLE ref_base_revalorisation (
	id integer primary key,
	base_revalorisation TEXT
);

-- table: ref_valeur_point_indice
CREATE TABLE ref_valeur_point_indice (
	id integer primary key,
	valeur_point_d_indice FLOAT,
	date_d_application DATE
);

/*
    Données
*/

CREATE TABLE cartographie_remuneration.agent (
    id SERIAL PRIMARY KEY,
    matricule_agent BIGINT UNIQUE,
    nom_usuel TEXT NOT NULL,
    prenom TEXT NOT NULL,
    genre TEXT,
    date_de_naissance DATE,
    corps TEXT,
    grade TEXT,
    echelon INTEGER
);

-- table: cartographie_remuneration.agent_diplome
CREATE TABLE cartographie_remuneration.agent_diplome (
	id integer primary key,
	id_niveau_diplome INTEGER REFERENCES cartographie_remuneration.ref_niveau_diplome(id),
	libelle_diplome TEXT,
	annee_d_obtention INTEGER,
	matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(id),
);

-- table: cartographie_remuneration.agent_revalorisation
CREATE TABLE cartographie_remuneration.agent_revalorisation (
	id integer primary key,
	id_base_revalorisation INTEGER REFERENCES cartographie_remuneration.ref_base_revalorisation(id),
	date_derniere_revalorisation DATE,
	valorisation_validee FLOAT,
	historique TEXT,
	date_dernier_renouvellement DATE,
	matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(id),
);

-- table: cartographie_remuneration.agent_contrat
CREATE TABLE cartographie_remuneration.agent_contrat (
	id integer primary key,
	date_premier_contrat_mef DATE,
	date_debut_contrat_actuel_dge DATE,
	duree_contrat_en_cours_dge INTEGER,
	duree_cumulee_contrats_tout_contrat_mef TEXT,
	date_fin_contrat_cdd_en_cours_au_soir DATE,
	date_de_cdisation DATE,
	duree_en_jours_si_coupure_de_contrat INTEGER,
	matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(id),
);

/* CREATE TABLE cartographie_remuneration.agent_formation (
    id SERIAL PRIMARY KEY,
    matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(id),
    diplome_niveau TEXT,
    diplome_libelle TEXT,
    diplome_date_obtention DATE,
    experience_pro_totale TEXT,
    experience_pro_qualifiante_poste TEXT
); */

CREATE TABLE cartographie_remuneration.agent_poste (
    id SERIAL PRIMARY KEY,
    matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(id),
    numero_poste TEXT,
    categorie TEXT,
    type_de_fonction_libelle_court TEXT,
    type_de_fonction_libelle_long TEXT,
    date_recrutement_structure DATE
);

CREATE TABLE cartographie_remuneration.agent_remuneration (
    id SERIAL PRIMARY KEY,
    matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(id),
    -- base_recrutement TEXT, = Niveau de diplôme ?
    indice_majore INT,
    type_indemnitaire TEXT,
    region_indemnitaire TEXT,
    region_indemnitaire_valeur FLOAT,
    total_indemnitaire_annuel FLOAT,
    total_annuel_ifse FLOAT,
    totale_brute_annuel FLOAT,
    plafond_part_variable FLOAT,
    plafond_part_variable_collective FLOAT,
    present_cartographie BOOLEAN,
    observations TEXT
);
