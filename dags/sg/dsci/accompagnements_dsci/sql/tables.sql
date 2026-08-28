DROP SCHEMA IF EXISTS activite_dsci CASCADE;
CREATE SCHEMA IF NOT EXISTS activite_dsci;

/*
    Référentiels
*/

CREATE TABLE activite_dsci."ref_typologie_accompagnement" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "typologie_accompagnement" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."ref_bureau" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "bureau" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."ref_profil_correspondant" CASCADE;
CREATE TABLE activite_dsci."ref_profil_correspondant" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "profil_correspondant" text,
    "intitule_long" text,
    "created_by" text,
    "updated_by" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."ref_direction" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "direction" text,
    "libelle_long" text,
    "administration" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."ref_region" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "region" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."ref_certification" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "competence" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."ref_pole" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_bureau" int,
    "pole" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."ref_type_accompagnement" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "type_d_accompagnement" text,
    "id_pole" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."ref_semainier" CASCADE;
CREATE TABLE activite_dsci."ref_semainier" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "annee" int,
    "mois" text,
    "trimestre" text,
    "date_semaine" date,
    "semaine" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."ref_qualite_service" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "qualite_de_service" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."ref_competence_particuliere" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "competence" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

/*
    Données : onglet global
*/

CREATE TABLE activite_dsci."effectif_dsci" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "mail" text,
    "id_bureau" int,
    "id_pole" int,
    "nom_complet" text,
    "agent_present" boolean,
    "fonction" text,
    "absent_depuis" date,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."accompagnement_dsci" CASCADE;
CREATE TABLE activite_dsci."accompagnement_dsci" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "annee" numeric,
    "statut" text,
    "recours_prestataire" text,
    "commentaires_complements" text,
    "ressources_documentaires" text,
    "debut_previsionnel_de_l_accompagnement" date,
    "fin_previsionnelle_de_l_accompagnement" date,
    "intitule_de_l_accompagnement" text,
    "autres_participants" text,
    "id_direction" int,
    "service_bureau" text,
    "sous_dir_bureau_" text,
    "nom_du_prestataire" text,
    "date_de_cloture_questionnaire" date,
    "porteur_metier" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."accompagnement_dsci_typologie" CASCADE;
CREATE TABLE activite_dsci."accompagnement_dsci_typologie" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_accompagnement" integer,
    "id_typologie" integer,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."accompagnement_dsci_equipe" CASCADE;
CREATE TABLE activite_dsci."accompagnement_dsci_equipe" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_accompagnement" integer,
    "id_equipe_s_dsci" integer,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."accompagnement_dsci_porteur" CASCADE;
CREATE TABLE activite_dsci."accompagnement_dsci_porteur" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_accompagnement" integer,
    "id_porteur_dsci" integer,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."bilaterale" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_direction" int,
    "date_de_rencontre" date,
    "intitule" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."bilaterale_remontee" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_bilaterale" int,
    "id_bureau" int,
    "information_a_remonter" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."correspondant" CASCADE;
CREATE TABLE activite_dsci."correspondant" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "mail" text,
    "nom_complet" text,
    "id_direction" int,
    "entite" text,
    "id_region" int,
    "actif" boolean,
    "id_promotion_fac" int,
    "est_certifie_fac" boolean,
    "actif_communaute_fac" boolean,
    "direction_hors_mef" text,
    "prenom" text,
    "nom" text,
    "date_debut_inactivite" date,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."correspondant_profil" CASCADE;
CREATE TABLE activite_dsci."correspondant_profil" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_correspondant" integer,
    "id_type_de_correspondant" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."correspondant_competence_particuliere" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_correspondant" integer,
    "id_competence_particuliere" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."correspondant_connaissance_communaute" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_correspondant" integer,
    "connaissance_communaute" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

/*
    Données : onglet Mission Innovation mi
*/

DROP TABLE IF EXISTS activite_dsci."accompagnement_mi" CASCADE;
CREATE TABLE activite_dsci."accompagnement_mi" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "est_ouvert_notation" boolean,
    "est_certifiant" boolean,
    "places_max" int,
    "nb_inscrits" int,
    "places_restantes" int,
    "intitule" text,
    "id_direction" int,
    "date_de_realisation" date,
    "statut" text,
    "id_pole" int,
    "id_type_d_accompagnement" int,
    "informations_complementaires" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."accompagnement_mi_satisfaction" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_accompagnement" int,
    "nombre_de_participants" numeric,
    "nombre_de_reponses" numeric,
    "taux_de_reponse" numeric,
    "note_moyenne_de_satisfaction" numeric,
    "unite" text,
    "id_type_d_accompagnement" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."animateur_interne" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_accompagnement" int,
    "id_animateur" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."animateur_externe" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_accompagnement" int,
    "animateur" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."animateur_fac" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_accompagnement" int,
    "id_animateur" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."animateur_fac_certification" CASCADE;
CREATE TABLE activite_dsci."animateur_fac_certification" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_animateur_fac" integer,
    "id_certifications_souhaitees" integer,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."animateur_fac_certification_valide" CASCADE;
CREATE TABLE activite_dsci."animateur_fac_certification_valide" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_animateur_fac" integer,
    "id_certifications_validees" integer,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."laboratoires_territoriaux" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "nom" text,
    "id_direction" int,
    "id_region" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

------------------------------------- questionnaires mi -------------------------------

CREATE TABLE activite_dsci."pleniere_quest_inscription" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "is_duplicate" int,
    "id_direction" int,
    "mail" text,
    "id_pleniere" int,
    "id_id_accompagnement" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."pleniere_quest_satisfaction" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "mail" text,
    "ce_que_j_ai_apprecie" text,
    "ce_qui_peut_etre_ameliore" text,
    "note_globale" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."passinnov_quest_inscription" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_region" int,
    "mail" text,
    "id_direction" int,
    "id_passinnov" int,
    "id_id_accompagnement" int,
    "role" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."passinnov_quest_satisfaction" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "is_duplicate" int,
    "mail" text,
    "id_id_passinnov" int,
    "commentaires" text,
    "id_quest_passinnov" int,
    "note_globale" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."formation_codev_quest_inscription"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "mail" text,
    "id_direction" int,
    "formation_codev" text,
    "experience_codev" text,
    "details_experience" text,
    "difficultes" text,
    "attentes" text,
    "id_session_formation_codev" int,
    "id_id_accompagnement" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."formation_fac_quest_satisfaction" CASCADE;
CREATE TABLE activite_dsci."formation_fac_quest_satisfaction"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_quest_formation" int,
    "mail" text,
    "id_promotion" int,
    "note_module_1" int,
    "commentaire_m1" text,
    "note_module_2" int,
    "commentaire_m2" text,
    "note_module_3" int,
    "commentaire_m3" text,
    "nps" int,
    "utilite" text,
    "besoin" text,
    "id_id_formation" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."formation_fac_envie_suite_quest_satisfaction" CASCADE;
CREATE TABLE activite_dsci."formation_fac_envie_suite_quest_satisfaction"(
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_formation_fac" integer,
    "envies_pour_la_suite" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."fac_hors_bercylab_quest_accompagnement" CASCADE;
CREATE TABLE activite_dsci."fac_hors_bercylab_quest_accompagnement" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "id_facilitateur_1" int,
    "id_facilitateur_2" int,
    "id_facilitateur_3" int,
    "id_direction" int,
    "synthese_de_l_accompagnement" text,
    "id_region" int,
    "date_de_realisation" date,
    "intitule_de_l_accompagnement" text,
    "statut" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."fac_hors_bercylab_quest_accompagnement_facilitateurs" CASCADE;
CREATE TABLE activite_dsci."fac_hors_bercylab_quest_accompagnement_facilitateurs" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_formation_fac_hors_bercylab" integer,
    "id_facilitateurs" integer,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."fac_hors_bercylab_quest_type_accompagnement" CASCADE;
CREATE TABLE activite_dsci."fac_hors_bercylab_quest_type_accompagnement" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_formation_fac_hors_bercylab" integer,
    "type_d_accompagnement" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS activite_dsci."fac_hors_bercylab_quest_accompagnement_participants" CASCADE;
CREATE TABLE activite_dsci."fac_hors_bercylab_quest_accompagnement_participants" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id_formation_fac_hors_bercylab" integer,
    "participants" text,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

/*
    Données : onglet Cellule Conseil Interne cci
*/

CREATE TABLE activite_dsci."charge_agent_cci" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "trimestre" text,
    "type_de_charge" text,
    "equipe" text,
    "id_missions" int,
    "id_semaine" int,
    "id_agent_e_" int,
    "temps_passe" numeric,
    "taux_de_charge" numeric,
    "annee" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

CREATE TABLE activite_dsci."accompagnement_cci_opportunite" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "date_prise_de_decision" date,
    "date_de_proposition_d_accompagnement" date,
    "decision" text,
    "date_de_reception" date,
    "id_accompagnement" int,
    "expression_de_besoin_transmise" boolean,
    "type_de_canal" text,
    "statut" text,
    "convention_d_accompagnement" boolean,
    "commentaires" text,
    "precision_canal" text,
    "proposition_d_accompagnement_transmise" boolean,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);

------------------------------------- questionnaires cci -------------------------------

CREATE TABLE activite_dsci."accompagnement_cci_quest_satisfaction" (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    "id" INTEGER,
    "appreciation_globale" text,
    "points_d_ameliorations" text,
    "points_forts" text,
    "id_adaptabilite" int,
    "id_formulaire_accompagnement" int,
    "id_relationnel_client" int,
    "id_qualite_des_livrables" int,
    "id_atteinte_objectifs" int,
    "score_de_recommandation" text,
    "id_pilotage_et_suivi" int,
    "autres_elements" text,
    "id_etape_de_cadrage" int,
    "id_aide_methodologique" int,
    "id_reactivite" int,
    "id_respect_calendrier" int,
    "mail" text,
    "id_accompagnement" int,
    import_timestamp TIMESTAMP NOT NULL,
    snapshot_id UUID NOT NULL,
    snapshot_id_parent UUID NULL,
    PRIMARY KEY ("id_row", "import_timestamp")
) PARTITION BY RANGE (import_timestamp);
