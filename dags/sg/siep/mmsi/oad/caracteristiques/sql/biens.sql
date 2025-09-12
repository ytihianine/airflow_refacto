---=========== Tables ===========---
DROP TABLE IF EXISTS temporaire.tmp_bien;
DROP TABLE IF EXISTS siep.bien;
CREATE TABLE IF NOT EXISTS siep.bien (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT UNIQUE NOT NULL,
    code_site BIGINT REFERENCES siep.site(code_site),
    libelle_bat_ter TEXT,
    gestion_categorie TEXT,
    gestion_mono_multi_mef TEXT,
    gestion_mono_multi_min TEXT,
    groupe_autorisation TEXT,
    categorie_administrative_liste_bat TEXT,
    categorie_administrative_principale_bat TEXT,
    gestionnaire_principal_code BIGINT REFERENCES siep.gestionnaire(code_gestionnaire),
    gestionnaire_type_simplifie_bat TEXT,
    gestionnaire_principal_libelle TEXT,
    gestionnaire_principal_libelle_simplifie TEXT,
    gestionnaire_principal_libelle_abrege TEXT,
    gestionnaire_principal_ministere TEXT,
    -- gest_personnalite_juridique TEXT,
    gest_princ_personnalite_juridique TEXT,
    gest_princ_personnalite_juridique_simplifiee TEXT,
    gest_princ_personnalite_juridique_precision TEXT,
    gestionnaire_principal_lien_mef TEXT,
    gestionnaire_presents_liste_mef TEXT,
    date_construction_annee_corrigee INTEGER,
    periode TEXT,
    presence_mef_bat TEXT
);

DROP TABLE IF EXISTS temporaire.tmp_bien_gestionnaire;
DROP TABLE IF EXISTS siep.bien_gestionnaire;
CREATE TABLE IF NOT EXISTS siep.bien_gestionnaire (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    code_gestionnaire BIGINT NOT NULL REFERENCES siep.gestionnaire(code_gestionnaire),
    code_bat_gestionnaire TEXT UNIQUE NOT NULL,
    indicateur_poste_gest_source FLOAT,
    indicateur_resident_gest_source FLOAT,
    indicateur_sub_gest_source FLOAT
);

DROP TABLE IF EXISTS temporaire.tmp_bien_occupant;
DROP TABLE IF EXISTS siep.bien_occupant;
CREATE TABLE IF NOT EXISTS siep.bien_occupant (
    id BIGSERIAL PRIMARY KEY,
    code_bat_ter BIGINT NOT NULL REFERENCES siep.bien(code_bat_ter),
    code_gestionnaire BIGINT NOT NULL REFERENCES siep.gestionnaire(code_gestionnaire),
    code_bat_gestionnaire TEXT NOT NULL REFERENCES siep.bien_gestionnaire(code_bat_gestionnaire),
    occupant TEXT,
    direction_locale_occupante TEXT,
    comprend_service_ac TEXT,
    direction_locale_occupante_principale TEXT,
    service_occupant TEXT,
    indicateur_sub_occ_source FLOAT,
    indicateur_sub_occ FLOAT,
    indicateur_surface_mef_occ FLOAT,
    indicateur_poste_occ_source FLOAT,
    indicateur_poste_occ FLOAT,
    indicateur_resident_occ_source FLOAT,
    indicateur_resident_occ FLOAT,
    indicateur_resident_reconstitue_occ FLOAT,
    coherence_erreur_possible_presence_occupant TEXT,
    coherence_indicateur_sub_revu_occ BOOLEAN,
    coherence_indicateur_resident_revu_occ BOOLEAN,
    categorie_administrative TEXT,
    categorie_administrative_simplifiee TEXT,
    filtre_spsi_initial BOOLEAN,
    filtre_spsi_maj BOOLEAN,
    indicateur_surface_spsi_m_sub_occ_initial FLOAT,
    indicateur_surface_spsi_m_sub_occ_maj FLOAT
);

---=========== Vues ===========---
CREATE MATERIALIZED VIEW siep.bien_caracteristiques_complet_gestionnaire_vw AS
  WITH bien_typologie_simplifie AS (
    SELECT
    -- bien_typologie
    sbt.code_bat_ter as code_bat_ter,
    sbt.usage_detaille_du_bien as usage_detaille_du_bien,
    -- ref_typologie
    srt.bati_non_bati as bati_non_bati,
    srt.type_de_bien as type_de_bien,
    srt.famille_de_bien as famille_de_bien,
    srt.famille_de_bien_simplifiee as famille_de_bien_simplifiee
    FROM siep.bien_typologie sbt
    LEFT JOIN siep.ref_typologie srt
      ON srt.usage_detaille_du_bien = sbt.usage_detaille_du_bien
  ),
  bien_occupant_agrege AS (
    SELECT sbo.code_bat_gestionnaire,
    -- colonnes agrégées
    SUM(sbo.indicateur_sub_occ) as sum_indicateur_sub_occ,
    SUM(sbo.indicateur_poste_occ) as sum_indicateur_poste_occ,
    SUM(sbo.indicateur_resident_occ) as sum_indicateur_resident_occ,
    SUM(sbo.indicateur_resident_reconstitue_occ) as sum_indicateur_resident_reconstitue_occ,
    SUM(sbo.indicateur_sub_occ_source) as sum_indicateur_sub_occ_source,
    SUM(sbo.indicateur_poste_occ_source) as sum_indicateur_poste_occ_source,
    SUM(sbo.indicateur_resident_occ_source) as sum_indicateur_resident_occ_source
    FROM siep.bien_occupant sbo
    GROUP BY code_bat_gestionnaire
  )
    SELECT
    -- bien_gestionnaire
    sbg.code_gestionnaire as code_gestionnaire,
    sbg.code_bat_gestionnaire as code_bat_gestionnaire,
    -- bien
    sb.code_site AS code_site,
    sb.code_bat_ter AS code_bat_ter,
    sb.categorie_administrative_liste_bat AS categorie_administrative_liste_bat,
    sb.categorie_administrative_principale_bat AS categorie_administrative_principale_bat,
    sb.date_construction_annee_corrigee AS date_construction_annee_corrigee,
    sb.gestionnaire_type_simplifie_bat AS gestionnaire_type_simplifie_bat,
    sb.gestionnaire_presents_liste_mef AS gestionnaire_presents_liste_mef,
    sb.gest_princ_personnalite_juridique AS gest_princ_personnalite_juridique,
    sb.gest_princ_personnalite_juridique_simplifiee AS gest_princ_personnalite_juridique_simplifiee,
    sb.gest_princ_personnalite_juridique_precision AS gest_princ_personnalite_juridique_precision,
    sb.gestionnaire_principal_code AS gestionnaire_principal_code,
    sb.gestionnaire_principal_libelle AS gestionnaire_principal_libelle,
    sb.gestionnaire_principal_libelle_simplifie AS gestionnaire_principal_libelle_simplifie,
    sb.gestionnaire_principal_lien_mef AS gestionnaire_principal_lien_mef,
    sb.gestionnaire_principal_ministere AS gestionnaire_principal_ministere,
    sb.libelle_bat_ter AS libelle_bat_ter,
    sb.gestion_mono_multi_mef AS gestion_mono_multi_mef,
    sb.gestion_categorie AS gestion_categorie,
    sb.gestion_mono_multi_min AS gestion_mono_multi_min,
    -- bien_information_complementaire
    sbic.etat_bat as etat_du_batiment,
    sbic.date_sortie_bat as date_sortie_bat,
    sbic.date_sortie_site as date_sortie_site,
    sbic.date_derniere_renovation as date_derniere_renovation,
    sbic.annee_reference as annee_reference,
    sbic.efa as efa,
    -- gestionnaire
    sg.libelle_gestionnaire as gestionnaire_libelle,
    sg.ministere as gestionnaire_ministere,
    sg.libelle_simplifie as gestionnaire_libelle_simplifie,
    sg.libelle_abrege as gestionnaire_libelle_abrege,
    sg.lien_mef_gestionnaire as gestionnaire_lien_mef,
    sg.personnalite_juridique as gestionnaire_personnalite_juridique,
    sg.personnalite_juridique_precision as gestionnaire_personnalite_juridique_precision,
    sg.personnalite_juridique_simplifiee as gestionnaire_personnalite_juridique_simplifie,
    -- bien_deet_energie_ges
    sbdeg.bat_assujettis_deet as bat_assujettis_deet,
    -- bien_strategie
    sbs.perimetre_spsi_initial AS perimetre_spsi_initial,
    sbs.perimetre_spsi_maj AS perimetre_spsi_maj,
    -- conso_statut_batiment
    scsb.statut_conso_avant_2019 as statut_conso_avant_2019,
    scsb.statut_fluide_global as statut_fluide_global,
    scsb.statut_batiment as statut_batiment,
    -- CTE bien_typologie_simplifie
    bts.usage_detaille_du_bien as usage_detaille_du_bien,
    bts.bati_non_bati as bati_non_bati,
    bts.type_de_bien as type_de_bien,
    bts.famille_de_bien as famille_de_bien,
    bts.famille_de_bien_simplifiee as famille_de_bien_simplifiee,
    -- CTE bien_occupant_agrege
    boa.sum_indicateur_sub_occ as sum_indicateur_sub_occ,
    boa.sum_indicateur_poste_occ as sum_indicateur_poste_occ,
    boa.sum_indicateur_resident_occ as sum_indicateur_resident_occ,
    boa.sum_indicateur_resident_reconstitue_occ as sum_indicateur_resident_reconstitue_occ,
    boa.sum_indicateur_sub_occ_source as sum_indicateur_sub_occ_source,
    boa.sum_indicateur_poste_occ_source as sum_indicateur_poste_occ_source,
    boa.sum_indicateur_resident_occ_source as sum_indicateur_resident_occ_source
    FROM siep.bien_gestionnaire as sbg
    LEFT JOIN siep.bien sb
      ON sbg.code_bat_ter = sb.code_bat_ter
    LEFT JOIN siep.bien_information_complementaire sbic
      ON sbic.code_bat_gestionnaire = sbg.code_bat_gestionnaire
    LEFT JOIN siep.conso_statut_batiment scsb
      ON scsb.code_bat_gestionnaire = sbg.code_bat_gestionnaire
    LEFT JOIN siep.bien_strategie sbs
      ON sbs.code_bat_ter = sbg.code_bat_ter
    LEFT JOIN siep.gestionnaire sg
      ON sg.code_gestionnaire = sbg.code_gestionnaire
    LEFT JOIN siep.bien_deet_energie_ges sbdeg
      ON sbdeg.code_bat_ter = sbg.code_bat_ter
    LEFT JOIN bien_typologie_simplifie bts
      ON bts.code_bat_ter = sbg.code_bat_ter
    LEFT JOIN bien_occupant_agrege boa
      ON boa.code_bat_gestionnaire = sbg.code_bat_gestionnaire
  ;
