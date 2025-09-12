DROP SCHEMA IF EXISTS activite_dsci CASCADE;
CREATE SCHEMA IF NOT EXISTS activite_dsci;

-- table: ref_region
CREATE TABLE activite_dsci.ref_region (
	id SERIAL PRIMARY KEY,
	region TEXT
);


-- table: ref_typologie_accompagnement
CREATE TABLE activite_dsci.ref_typologie_accompagnement (
	id SERIAL PRIMARY KEY,
	typologie_accompagnement TEXT
);

-- table: ref_bureau
CREATE TABLE activite_dsci.ref_bureau (
	id SERIAL PRIMARY KEY,
	bureau TEXT
);

-- table: ref_direction
CREATE TABLE activite_dsci.ref_direction (
	id SERIAL PRIMARY KEY,
	direction TEXT,
	libelle_long TEXT,
	administration TEXT
);

-- table: ref_semainier
CREATE TABLE activite_dsci.ref_semainier (
	id SERIAL PRIMARY KEY,
	date_semaine DATE,
	semaine INTEGER
);

-- table: ref_competence_particuliere
CREATE TABLE activite_dsci.ref_competence_particuliere (
	id SERIAL PRIMARY KEY,
	competence TEXT
);

-- table: ref_pole
CREATE TABLE activite_dsci.ref_pole (
	id SERIAL PRIMARY KEY,
	id_bureau INTEGER REFERENCES activite_dsci.ref_bureau(id),
	pole TEXT
);

-- table: ref_type_accompagnement
CREATE TABLE activite_dsci.ref_type_accompagnement (
	id SERIAL PRIMARY KEY,
	type_d_accompagnement TEXT,
	id_pole INTEGER REFERENCES activite_dsci.ref_pole(id)
);

-- table: ref_qualite_service
CREATE TABLE activite_dsci.ref_qualite_service (
	id SERIAL PRIMARY KEY,
	qualite_de_service TEXT
);

-- table: ref_promotion_fac
CREATE TABLE activite_dsci.ref_promotion_fac (
	id SERIAL PRIMARY KEY,
	promotion TEXT
);

-- table: ref_certification
CREATE TABLE activite_dsci.ref_certification (
	id SERIAL PRIMARY KEY,
	competence TEXT
);

-- table: ref_profil_correspondant
CREATE TABLE activite_dsci.ref_profil_correspondant (
	id SERIAL PRIMARY KEY,
	profil_correspondant TEXT,
	intitule_long TEXT
);

-- table: struc_bilaterales
CREATE TABLE activite_dsci.bilaterale (
	id SERIAL PRIMARY KEY,
	id_direction INTEGER REFERENCES activite_dsci.ref_direction(id), -- ADD REFERENCE
	date_de_rencontre DATE,
	intitule TEXT
);

-- table: struc_bilaterale_remontee
CREATE TABLE activite_dsci.bilaterale_remontee (
	id SERIAL PRIMARY KEY,
	id_bilaterale INTEGER REFERENCES activite_dsci.bilaterale(id),
	id_bureau INTEGER REFERENCES activite_dsci.ref_bureau(id),
	information_a_remonter TEXT,
	id_direction INTEGER REFERENCES activite_dsci.ref_direction(id)
);

-- table: struc_correspondant
CREATE TABLE activite_dsci.correspondant (
	id SERIAL PRIMARY KEY,
	mail TEXT,
	nom_complet TEXT,
	id_direction INTEGER REFERENCES activite_dsci.ref_direction(id),
	entite TEXT,
	id_region INTEGER REFERENCES activite_dsci.ref_region(id),
	actif BOOLEAN,
	id_promotion_fac INTEGER ,
	nom TEXT,
	prenom TEXT,
	-- competence_particuliere text default null,
	date_debut_inactivite DATE default null,
	poste TEXT,
	cause_inactivite TEXT,
	connaissance_communaute text default null,
	direction_hors_mef TEXT,
	est_certifie_fac BOOLEAN,
	actif_communaute_fac BOOLEAN
);

-- table: correspondant new
CREATE TABLE activite_dsci.correspondant_profil (
	id SERIAL PRIMARY KEY,
	mail TEXT,
	id_type_correspondant INTEGER REFERENCES activite_dsci.ref_profil_correspondant(id)
);

-- table: struc_accompagnement_mi
CREATE TABLE activite_dsci.accompagnement_mi (
	id SERIAL PRIMARY KEY,
	intitule TEXT,
	id_direction INTEGER REFERENCES activite_dsci.ref_direction(id),
	date_de_realisation date default null,
	statut TEXT,
	id_pole INTEGER REFERENCES activite_dsci.ref_pole(id),
	id_type_accompagnement INTEGER REFERENCES activite_dsci.ref_type_accompagnement(id),
	informations_complementaires TEXT,
	est_certifiant BOOLEAN,
	places_max INTEGER,
	nb_inscrits INTEGER ,
	places_restantes INTEGER,
	est_ouvert_notation BOOLEAN
);

-- table: struc_accompagnement_mi_satisfaction
CREATE TABLE activite_dsci.accompagnement_mi_satisfaction (
	id SERIAL PRIMARY KEY,
	id_accompagnement INTEGER REFERENCES activite_dsci.accompagnement_mi(id),
	nombre_de_participants INTEGER,
	nombre_de_reponses INTEGER,
	taux_de_reponse float,
	note_moyenne_de_satisfaction float,
	unite TEXT,
	id_type_accompagnement INTEGER REFERENCES activite_dsci.ref_type_accompagnement(id)
);


-- table: struc_animateur_interne
CREATE TABLE activite_dsci.struc_animateur_interne (
	id SERIAL PRIMARY KEY,
	id_accompagnement INTEGER , -- ADD REFERENCE
	id_animateur_interne INTEGER  -- ADD REFERENCE
);

-- table: struc_animateur_externe
CREATE TABLE activite_dsci.struc_animateur_externe (
	id SERIAL PRIMARY KEY,
	id_accompagnement INTEGER , -- ADD REFERENCE
	animateur_externe TEXT
);



-- table: struc_quest_satisfaction_accompagnement_cci
CREATE TABLE activite_dsci.struc_quest_satisfaction_accompagnement_cci (
	id SERIAL PRIMARY KEY,
	mail TEXT,
	appreciation_globale INTEGER,
	points_forts TEXT,
	points_d_ameliorations TEXT,
	score_de_recommandation TEXT,
	autres_elements TEXT,
	id_etape_de_cadrage INTEGER,
	id_aide_methodologique INTEGER,
	id_pilotage_et_suivi INTEGER,
	id_respect_calendrier INTEGER,
	id_reactivite INTEGER,
	id_adaptabilite INTEGER,
	id_relationnel_client INTEGER,
	id_qualite_des_livrables INTEGER,
	id_atteinte_objectifs INTEGER,
	id_accompagnement_dsci integer);




-- table: struc_quest_satisfaction_formation_fac
CREATE TABLE activite_dsci.struc_quest_satisfaction_formation_fac (
	id SERIAL PRIMARY KEY,
	mail blob default null,
	promotion INTEGER ,
	note_module_1 numeric default 0,
	commentaire_m1 blob default null,
	note_module_2 TEXT,
	commentaire_m2 blob default null,
	note_module_3 numeric default 0,
	commentaire_m3 blob default null,
	nps numeric default 0,
	utilite blob default null,
	envies_pour_la_suite text default null,
	besoin blob default null,
	quest_formation INTEGER ,
	id_formation INTEGER
);

-- table: struc_quest_inscription_passinnov
CREATE TABLE activite_dsci.struc_quest_inscription_passinnov (
	id SERIAL PRIMARY KEY,
	mail TEXT,
	direction INTEGER ,
	region INTEGER ,
	role TEXT,
	passinnov INTEGER ,
	id_accompagnement INTEGER ,
	is_duplicate numeric default 0);



-- table: struc_animateur_fac
CREATE TABLE activite_dsci.struc_animateur_fac (
	id SERIAL PRIMARY KEY,
	accompagnement INTEGER ,
	animateur INTEGER ,
	certifications_souhaitees text default null,
	cert_possibles_txt TEXT,
	certifications_validees text default null,
	cert_souhaitees_txt TEXT,
	cert_validees_txt TEXT
);

CREATE TABLE activite_dsci.struc_animateur_fac_certification (
	id SERIAL PRIMARY KEY,
	accompagnement INTEGER ,
	animateur INTEGER ,
	certifications_souhaitees text default null,
	cert_possibles_txt TEXT,
	certifications_validees text default null,
	cert_souhaitees_txt TEXT,
	cert_validees_txt TEXT
);



-- table: struc_quest_satisfaction_passinnov
CREATE TABLE activite_dsci.struc_quest_satisfaction_passinnov (
	id SERIAL PRIMARY KEY,
	mail TEXT,
	note_globale TEXT,
	commentaires TEXT,
	quest_passinnov INTEGER ,
	id_passinnov INTEGER ,
	is_duplicate blob default null);

-- table: struc_laboratoires_territoriaux
CREATE TABLE activite_dsci.struc_laboratoires_territoriaux (
	id SERIAL PRIMARY KEY,
	nom blob default null,
	id_direction INTEGER ,
	id_region INTEGER
);






-- table: struc_quest_accompagnement_fac_hors_bercylab
CREATE TABLE activite_dsci.struc_quest_accompagnement_fac_hors_bercylab (
	id SERIAL PRIMARY KEY,
	intitule_de_l_accompagnement TEXT,
	type_d_accompagnement text default null,
	date_de_realisation date default null,
	statut TEXT,
	direction INTEGER ,
	synthese_de_l_accompagnement TEXT,
	region INTEGER ,
	participants text default null,
	facilitateur_1 INTEGER ,
	facilitateur_2 INTEGER ,
	facilitateur_3 INTEGER ,
	facilitateurs text default null
);

/**************
-- TO COMPLETE
**************/
-- table: struc_accompagnement_dsci
CREATE TABLE activite_dsci.struc_accompagnement_dsci (
	id SERIAL PRIMARY KEY,
	annee numeric default 0,
	statut TEXT,
	typologie text default null,
	prestataire TEXT,
	commentaires_complements TEXT,
	ressources_documentaires TEXT,
	debut_previsionnel_de_l_accompagnement date default null,
	fin_previsionnelle_de_l_accompagnement date default null,
	intitule_de_l_accompagnement TEXT,
	autres_participants TEXT,
	direction2 INTEGER ,
	service_bureau TEXT,
	sous_dir_bureau_ TEXT,
	equipe_s_dsci text default null,
	porteur_dsci text default null,
	nom_du_prestataire TEXT,
	equipe_dsci_txt TEXT,
	formulaire_cci TEXT,
	date_de_cloture_questionnaire date default null,
	porteur_metier blob default null
);

-- table: struc_effectif_dsci
CREATE TABLE activite_dsci.struc_effectif_dsci (
	id SERIAL PRIMARY KEY,
	mail TEXT,
	bureau INTEGER ,
	pole INTEGER ,
	bureau_texte TEXT,
	nom_complet TEXT,
	agent_present boolean default 0,
	fonction TEXT,
	created_at TEXT,
	updated_at TEXT,
	created_by TEXT,
	updated_by TEXT,
	absent_depuis date default null
);

-- table: struc_charge_agent_cci
CREATE TABLE activite_dsci.struc_charge_agent_cci (
	id SERIAL PRIMARY KEY,
	agent_e_ INTEGER ,
	semaine INTEGER ,
	missions INTEGER ,
	temps_passe numeric default 0,
	type_de_charge TEXT,
	equipe blob default null,
	trimestre TEXT,
	annee blob default null,
	taux_de_charge numeric default 0
);

-- table: struc_accompagnement_opportunite_cci
CREATE TABLE activite_dsci.struc_accompagnement_opportunite_cci (
	id SERIAL PRIMARY KEY,
	accompagnement INTEGER ,
	date_de_reception date default null,
	type_de_canal TEXT,
	expression_de_besoin_transmise boolean default 0,
	proposition_d_accompagnement_transmise boolean default 0,
	date_de_proposition_d_accompagnement date default null,
	decision TEXT,
	date_prise_de_decision date default null,
	convention_d_accompagnement boolean default 0,
	commentaires TEXT,
	precision_canal TEXT,
	statut blob default null
);

-- table: correspondant_maj
CREATE TABLE activite_dsci.correspondant_maj (
	id SERIAL PRIMARY KEY,
	type_de_correspondant text default null,
	nom_complet TEXT,
	mail TEXT,
	direction INTEGER ,
	region INTEGER ,
	entite TEXT,
	direction_hors_mef2 TEXT,
	actif boolean default 0,
	type_correspondant_text TEXT,
	competence_particuliere text default null,
	poste TEXT,
	cause_inactivite TEXT,
	connaissance_communaute text default null,
	competence_autre2 blob default null,
	nbre_de_facilitations numeric default 0,
	souhait_certification2 boolean default 0,
	nom TEXT,
	prenom TEXT,
	nbre_d_ateliers_facilites_au_1er_semestre_2025_sans_bercylab2 numeric default 0,
	nbre_d_ateliers_facilites_au_1er_semestre_2025_avec_bercylab2 numeric default 0,
	nbre_d_ateliers_facilites_en_2024_sans_bercylab numeric default 0,
	nbre_d_ateliers_facilites_en_2024_avec_bercylab2 numeric default 0
);
