---=========== Tables ===========---
DROP TABLE IF EXISTS temporaire.tmp_bien;
DROP TABLE IF EXISTS siep.bien;
CREATE TABLE IF NOT EXISTS siep.bien (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    code_site BIGINT ,
    libelle_bat_ter TEXT,
    gestion_categorie TEXT,
    gestion_mono_multi_mef TEXT,
    gestion_mono_multi_min TEXT,
    groupe_autorisation TEXT,
    categorie_administrative_liste_bat TEXT,
    categorie_administrative_principale_bat TEXT,
    gestionnaire_principal_code BIGINT,
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
    presence_mef_bat TEXT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	  snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, import_timestamp),
    FOREIGN KEY(code_site, import_timestamp) REFERENCES siep.site(code_site, import_timestamp),
    FOREIGN KEY(gestionnaire_principal_code, import_timestamp) REFERENCES siep.gestionnaire(code_gestionnaire, import_timestamp)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS temporaire.tmp_bien_gestionnaire;
DROP TABLE IF EXISTS siep.bien_gestionnaire;
CREATE TABLE IF NOT EXISTS siep.bien_gestionnaire (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    code_gestionnaire BIGINT NOT NULL,
    code_bat_gestionnaire TEXT NOT NULL,
    indicateur_poste_gest_source FLOAT,
    indicateur_resident_gest_source FLOAT,
    indicateur_sub_gest_source FLOAT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	  snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, code_gestionnaire, code_bat_gestionnaire, import_timestamp),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp),
    FOREIGN KEY(code_gestionnaire, import_timestamp) REFERENCES siep.gestionnaire(code_gestionnaire, import_timestamp)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS temporaire.tmp_bien_occupant;
DROP TABLE IF EXISTS siep.bien_occupant;
CREATE TABLE IF NOT EXISTS siep.bien_occupant (
    id BIGSERIAL,
    code_bat_ter BIGINT NOT NULL,
    code_gestionnaire BIGINT NOT NULL,
    code_bat_gestionnaire TEXT NOT NULL,
    occupant TEXT NOT NULL,
    direction_locale_occupante TEXT,
    comprend_service_ac TEXT,
    direction_locale_occupante_principale TEXT,
    service_occupant TEXT NOT NULL,
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
    indicateur_surface_spsi_m_sub_occ_maj FLOAT,
    import_timestamp TIMESTAMP NOT NULL,
    import_date DATE NOT NULL,
	  snapshot_id UUID,
    PRIMARY KEY (code_bat_ter, code_gestionnaire, code_bat_gestionnaire, occupant, service_occupant, import_timestamp),
    FOREIGN KEY(code_bat_ter, code_gestionnaire, code_bat_gestionnaire, import_timestamp)
      REFERENCES siep.bien_gestionnaire(code_bat_ter, code_gestionnaire, code_bat_gestionnaire, import_timestamp)
) PARTITION BY RANGE (import_timestamp);

---=========== Vues ===========---
DROP VIEW siep.bien_caracteristiques_complet_gestionnaire_vw;
CREATE VIEW siep.bien_caracteristiques_complet_gestionnaire_vw AS
	WITH bien_typologie_simplifie AS (
        SELECT sbt.code_bat_ter,
            sbt.usage_detaille_du_bien,
            sbt.import_timestamp,
            srt.bati_non_bati,
            srt.type_de_bien,
            srt.famille_de_bien,
            srt.famille_de_bien_simplifiee
        FROM siep.bien_typologie sbt
        LEFT JOIN siep.ref_typologie srt ON srt.usage_detaille_du_bien = sbt.usage_detaille_du_bien
        ), bien_occupant_agrege AS (
        SELECT sbo.code_bat_gestionnaire,
        	sbo.import_timestamp,
            sum(sbo.indicateur_sub_occ) AS sum_indicateur_sub_occ,
            sum(sbo.indicateur_surface_mef_occ) AS sum_indicateur_surface_mef_occ,
            sum(sbo.indicateur_poste_occ) AS sum_indicateur_poste_occ,
            sum(sbo.indicateur_resident_occ) AS sum_indicateur_resident_occ,
            sum(sbo.indicateur_resident_reconstitue_occ) AS sum_indicateur_resident_reconstitue_occ,
            sum(sbo.indicateur_sub_occ_source) AS sum_indicateur_sub_occ_source,
            sum(sbo.indicateur_poste_occ_source) AS sum_indicateur_poste_occ_source,
            sum(sbo.indicateur_resident_occ_source) AS sum_indicateur_resident_occ_source
        FROM siep.bien_occupant sbo
        GROUP BY sbo.code_bat_gestionnaire, sbo.import_timestamp
        )
 SELECT
    sb.snapshot_id,
    sb.import_timestamp,
    sbg.code_gestionnaire,
    sbg.code_bat_gestionnaire,
    sb.code_site,
    sb.code_bat_ter,
    sb.categorie_administrative_liste_bat,
    sb.categorie_administrative_principale_bat,
    sb.date_construction_annee_corrigee,
    sb.gestionnaire_type_simplifie_bat,
    sb.gestionnaire_presents_liste_mef,
    sb.gest_princ_personnalite_juridique,
    sb.gest_princ_personnalite_juridique_simplifiee,
    sb.gest_princ_personnalite_juridique_precision,
    sb.gestionnaire_principal_code,
    sb.gestionnaire_principal_libelle,
    sb.gestionnaire_principal_libelle_simplifie,
    sb.gestionnaire_principal_lien_mef,
    sb.gestionnaire_principal_ministere,
    sb.libelle_bat_ter,
    sb.gestion_mono_multi_mef,
    sb.gestion_categorie,
    sb.gestion_mono_multi_min,
    sbic.etat_bat AS etat_du_batiment,
    sbic.date_sortie_bat,
    sbic.date_sortie_site,
    sbic.date_derniere_renovation,
    sbic.annee_reference,
    sbic.efa,
    sg.libelle_gestionnaire AS gestionnaire_libelle,
    sg.ministere AS gestionnaire_ministere,
    sg.libelle_simplifie AS gestionnaire_libelle_simplifie,
    sg.libelle_abrege AS gestionnaire_libelle_abrege,
    sg.lien_mef_gestionnaire AS gestionnaire_lien_mef,
    sg.personnalite_juridique AS gestionnaire_personnalite_juridique,
    sg.personnalite_juridique_precision AS gestionnaire_personnalite_juridique_precision,
    sg.personnalite_juridique_simplifiee AS gestionnaire_personnalite_juridique_simplifie,
    sbdeg.bat_assujettis_deet,
    sbs.perimetre_spsi_initial,
    sbs.perimetre_spsi_maj,
    sbp.locatif_domanial,
    sbr.reglementation_corrigee,
    scsb.statut_conso_avant_2019,
    scsb.statut_fluide_global,
    scsb.statut_batiment,
    bts.usage_detaille_du_bien,
    bts.bati_non_bati,
    bts.type_de_bien,
    bts.famille_de_bien,
    bts.famille_de_bien_simplifiee,
    boa.sum_indicateur_sub_occ,
    boa.sum_indicateur_surface_mef_occ,
    boa.sum_indicateur_poste_occ,
    boa.sum_indicateur_resident_occ,
    boa.sum_indicateur_resident_reconstitue_occ,
    boa.sum_indicateur_sub_occ_source,
    boa.sum_indicateur_poste_occ_source,
    boa.sum_indicateur_resident_occ_source
   FROM siep.bien_gestionnaire sbg
    LEFT JOIN siep.bien sb
    	ON sbg.code_bat_ter = sb.code_bat_ter AND sbg.import_timestamp = sb.import_timestamp
    LEFT JOIN siep.bien_information_complementaire sbic
    	ON sbic.code_bat_gestionnaire = sbg.code_bat_gestionnaire AND sbg.import_timestamp = sbic.import_timestamp
    LEFT JOIN siep.conso_statut_batiment scsb
    	ON scsb.code_bat_gestionnaire = sbg.code_bat_gestionnaire AND sbg.import_timestamp = scsb.import_timestamp
    LEFT JOIN siep.bien_strategie sbs
    	ON sbs.code_bat_ter = sbg.code_bat_ter AND sbg.import_timestamp = sbs.import_timestamp
    LEFT JOIN siep.gestionnaire sg
    	ON sg.code_gestionnaire = sbg.code_gestionnaire AND sbg.import_timestamp = sg.import_timestamp
    LEFT JOIN siep.bien_deet_energie_ges sbdeg
    	ON sbdeg.code_bat_ter = sbg.code_bat_ter AND sbg.import_timestamp = sbdeg.import_timestamp
    LEFT JOIN siep.bien_proprietaire sbp
    	ON sbg.code_bat_ter = sbp.code_bat_ter AND sbg.import_timestamp = sbp.import_timestamp
    LEFT JOIN siep.bien_reglementation sbr
    	ON sbg.code_bat_ter = sbr.code_bat_ter AND sbg.import_timestamp = sbr.import_timestamp
    LEFT JOIN bien_typologie_simplifie bts
    	ON bts.code_bat_ter = sbg.code_bat_ter AND sbg.import_timestamp = bts.import_timestamp
    LEFT JOIN bien_occupant_agrege boa
    	ON boa.code_bat_gestionnaire = sbg.code_bat_gestionnaire AND sbg.import_timestamp = boa.import_timestamp
  WHERE sb.snapshot_id IS NOT NULL
  ;
