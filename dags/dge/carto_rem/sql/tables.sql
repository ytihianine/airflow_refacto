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
	base_remuneration TEXT,
  base_rem_dge TEXT
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
    id SERIAL,
    matricule_agent BIGINT UNIQUE,
    nom_usuel TEXT,
    prenom TEXT,
    genre TEXT,
    date_de_naissance DATE,
    qualite_statutaire TEXT,
    date_acces_corps DATE,
    corps TEXT,
    grade TEXT,
    echelon INTEGER,
    PRIMARY KEY (matricule_agent)
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
  date_entree_dge DATE,
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
    libelle_du_poste TEXT,
    fonction_dge_libelle_court TEXT,
    fonction_dge_libelle_long TEXT,
    date_recrutement_structure DATE
);

CREATE TABLE cartographie_remuneration.agent_remuneration (
    id SERIAL PRIMARY KEY,
    matricule_agent BIGINT REFERENCES cartographie_remuneration.agent(matricule_agent),
    -- base_recrutement TEXT, = Niveau de diplôme ?
    indice_majore INT,
    type_indemnitaire TEXT,
    id_base_remuneration INT,
    region_indemnitaire TEXT,
    region_indemnitaire_valeur FLOAT,
    remuneration_principale FLOAT,
    total_indemnitaire_annuel FLOAT,
    total_annuel_ifse FLOAT,
    totale_brute_annuel FLOAT,
    plafond_part_variable FLOAT,
    plafond_part_variable_collective FLOAT,
    present_cartographie BOOLEAN,
    observations TEXT,
    FOREIGN KEY(id_base_remuneration) REFERENCES cartographie_remuneration.ref_base_remuneration(id)
);

CREATE TABLE cartographie_remuneration.agent_experience_pro (
  id SERIAL,
  matricule_agent BIGINT,
  experience_pro_totale DOUBLE PRECISION,
  experience_pro_qualifiante DOUBLE PRECISION,
  PRIMARY KEY (id),
  FOREIGN KEY(matricule_agent) REFERENCES cartographie_remuneration.agent(matricule_agent)
);

/*
	VIEWS
*/
CREATE MATERIALIZED VIEW cartographie_remuneration.vw_agent_cartographie_remuneration AS
WITH cte_niveau_diplome AS (
  SELECT
    crad.matricule_agent as matricule_agent,
    crad.id_niveau_diplome as id_niveau_diplome,
    crrnd.niveau_diplome as niveau_diplome,
    crad.libelle_diplome as libelle_diplome,
    crad.annee_d_obtention as annee_d_obtention
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
  date_part('year', cra.date_de_naissance)::int as annee_naissance,
  cra.genre,
  cra.qualite_statutaire,
  cra.date_acces_corps,
  date_part('year', cra.date_acces_corps)::int as annee_acces_corps,
  cra.corps,
  cra.grade,
  cra.echelon,
  -- agent_contrat
  crac.date_debut_contrat_actuel_dge,
  crac.date_entree_dge,
  -- agent_experience_pro
  craep.experience_pro_totale,
  craep.experience_pro_qualifiante_sur_poste,
  -- agent_poste
  crap.categorie,
  crap.libelle_du_poste,
  crap.fonction_dge_libelle_court,
  crap.fonction_dge_libelle_long,
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
  CASE
    WHEN crar.plafond_part_variable IS NULL AND crar.plafond_part_variable_collective IS NULL
      THEN NULL
    ELSE
      COALESCE(crar.plafond_part_variable, 0) + COALESCE(crar.plafond_part_variable_collective, 0)
  END as plafond_part_variable_totale,
  crar.present_cartographie,
  -- cte_diplome
  cte_diplome.niveau_diplome,
  cte_diplome.id_niveau_diplome,
  cte_diplome.libelle_diplome,
  cte_diplome.annee_d_obtention
FROM cartographie_remuneration.agent cra
LEFT JOIN cartographie_remuneration.agent_contrat crac
  ON cra.matricule_agent = crac.matricule_agent
LEFT JOIN cartographie_remuneration.agent_experience_pro craep
  ON cra.matricule_agent = craep.matricule_agent
LEFT JOIN cartographie_remuneration.agent_poste crap
  ON cra.matricule_agent = crap.matricule_agent
LEFT JOIN cartographie_remuneration.agent_remuneration crar
  ON cra.matricule_agent = crar.matricule_agent
LEFT JOIN cte_niveau_diplome cte_diplome
  ON cra.matricule_agent = cte_diplome.matricule_agent
;
