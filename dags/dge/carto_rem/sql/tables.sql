DROP SCHEMA IF EXISTS cartographie_remuneration CASCADE;
CREATE SCHEMA cartographie_remuneration;

/*
    Référentiels
*/

-- table: cartographie_remuneration.ref_niveau_diplome
CREATE TABLE cartographie_remuneration.ref_niveau_diplome (
	id SERIAL primary key,
	niveau_diplome TEXT,
	ancien_nom TEXT
);

-- table: cartographie_remuneration.ref_experience_pro
CREATE TABLE cartographie_remuneration.ref_experience_pro (
	id SERIAL primary key,
	experience_pro TEXT
);

-- table: cartographie_remuneration.ref_base_recrutement
CREATE TABLE cartographie_remuneration.ref_base_remuneration (
	id SERIAL primary key,
	base_remuneration TEXT
);

-- table: cartographie_remuneration.ref_base_revalorisation
CREATE TABLE cartographie_remuneration.ref_base_revalorisation (
	id SERIAL primary key,
	base_revalorisation TEXT
);

-- table: cartographie_remuneration.ref_valeur_point_indice
CREATE TABLE cartographie_remuneration.ref_valeur_point_indice (
	id SERIAL primary key,
	valeur_point_d_indice FLOAT,
	date_d_application DATE
);

/*
    Données
*/

CREATE TABLE cartographie_remuneration.agent (
    id SERIAL PRIMARY KEY,
    matricule_agent BIGINT UNIQUE,
    nom_usuel TEXT,
    prenom TEXT,
    genre TEXT,
    date_de_naissance DATE,
    corps TEXT,
    grade TEXT,
    echelon INTEGER
);

-- table: cartographie_remuneration.agent_diplome
CREATE TABLE cartographie_remuneration.agent_diplome (
	id SERIAL primary key,
	id_niveau_diplome INTEGER REFERENCES cartographie_remuneration.ref_niveau_diplome(id),
	libelle_diplome TEXT,
	annee_d_obtention INTEGER,
	matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(matricule_agent)
);

-- table: cartographie_remuneration.agent_revalorisation
CREATE TABLE cartographie_remuneration.agent_revalorisation (
	id SERIAL primary key,
	id_base_revalorisation INTEGER REFERENCES cartographie_remuneration.ref_base_revalorisation(id),
	date_derniere_revalorisation DATE,
	valorisation_validee FLOAT,
	historique TEXT,
	date_dernier_renouvellement DATE,
	matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(matricule_agent)
);

-- table: cartographie_remuneration.agent_contrat
CREATE TABLE cartographie_remuneration.agent_contrat (
	id SERIAL primary key,
	date_premier_contrat_mef DATE,
	date_debut_contrat_actuel_dge DATE,
	duree_contrat_en_cours_dge INTEGER,
	duree_cumulee_contrats_tout_contrat_mef TEXT,
	date_fin_contrat_cdd_en_cours_au_soir DATE,
	date_de_cdisation DATE,
	duree_en_jours_si_coupure_de_contrat INTEGER,
	matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(matricule_agent)
);

CREATE TABLE cartographie_remuneration.agent_poste (
    id SERIAL PRIMARY KEY,
    matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(matricule_agent),
    numero_poste TEXT,
    categorie TEXT,
    type_de_fonction_libelle_court TEXT,
    type_de_fonction_libelle_long TEXT,
    date_recrutement_structure DATE
);

CREATE TABLE cartographie_remuneration.agent_remuneration (
    id SERIAL PRIMARY KEY,
    matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(matricule_agent),
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

/*
	VIEWS
*/
CREATE MATERIALIZED VIEW cartographie_remuneration.vw_agent_cartographie_remuneration AS
WITH cte_niveau_diplome AS (
  SELECT
    crad.matricule_agent as matricule_agent, crad.id_niveau_diplome as id_niveau_diplome, crrnd.niveau_diplome
  FROM cartographie_remuneration.ref_niveau_diplome crrnd
  LEFT JOIN cartographie_remuneration.agent_diplome crad
    ON crrnd.id = crad.id_niveau_diplome
)
SELECT
  -- agent
  cra.matricule_agent,
  cra.nom_usuel,
  cra.prenom,
  cra.date_de_naissance,
  cra.genre,
  cra.corps,
  cra.grade,
  cra.echelon,
  -- agent_diplome
  crad.id_niveau_diplome,
  crad.libelle_diplome,
  crad.annee_d_obtention,
  -- agent_experience_pro
  -- agent_poste
  crap.categorie,
  crap.type_de_fonction_libelle_court,
  crap.date_recrutement_structure,
  -- agent_remuneration
  crar.indice_majore,
  crar.type_indemnitaire,
  crar.region_indemnitaire,
  crar.region_indemnitaire_valeur,
  crar.total_indemnitaire_annuel,
  crar.total_annuel_ifse,
  crar.totale_brute_annuel,
  crar.plafond_part_variable,
  crar.plafond_part_variable_collective,
  crar.present_cartographie,
  -- cte_diplome
  cte_nd.niveau_diplome
FROM cartographie_remuneration.agent cra
LEFT JOIN cartographie_remuneration.agent_diplome crad
  ON cra.matricule_agent = crad.matricule_agent
LEFT JOIN cartographie_remuneration.agent_poste crap
  ON cra.matricule_agent = crap.matricule_agent
LEFT JOIN cartographie_remuneration.agent_remuneration crar
  ON cra.matricule_agent = crar.matricule_agent
LEFT JOIN cte_niveau_diplome cte_nd
  ON cra.matricule_agent = cte_nd.matricule_agent
