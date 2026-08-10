---=========== Tables ===========---
DROP TABLE IF EXISTS temporaire.tmp_bien;
DROP TABLE IF EXISTS siep.bien;
CREATE TABLE IF NOT EXISTS siep.bien (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
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
    import_timestamp TIMESTAMP,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter),
    FOREIGN KEY(code_site, import_timestamp) REFERENCES siep.site(code_site, import_timestamp),
    FOREIGN KEY(gestionnaire_principal_code, import_timestamp) REFERENCES siep.gestionnaire(code_gestionnaire, import_timestamp)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS temporaire.tmp_bien_gestionnaire;
DROP TABLE IF EXISTS siep.bien_gestionnaire;
CREATE TABLE IF NOT EXISTS siep.bien_gestionnaire (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
    code_bat_ter BIGINT NOT NULL,
    code_gestionnaire BIGINT NOT NULL,
    code_bat_gestionnaire TEXT NOT NULL,
    indicateur_poste_gest_source FLOAT,
    indicateur_resident_gest_source FLOAT,
    indicateur_sub_gest_source FLOAT,
    import_timestamp TIMESTAMP,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter, code_gestionnaire, code_bat_gestionnaire),
    FOREIGN KEY(code_bat_ter, import_timestamp) REFERENCES siep.bien(code_bat_ter, import_timestamp),
    FOREIGN KEY(code_gestionnaire, import_timestamp) REFERENCES siep.gestionnaire(code_gestionnaire, import_timestamp)
) PARTITION BY RANGE (import_timestamp);

DROP TABLE IF EXISTS temporaire.tmp_bien_occupant;
DROP TABLE IF EXISTS siep.bien_occupant;
CREATE TABLE IF NOT EXISTS siep.bien_occupant (
    id_row bigint GENERATED ALWAYS AS IDENTITY,
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
    import_timestamp TIMESTAMP,
    PRIMARY KEY (id_row, import_timestamp),
    UNIQUE (import_timestamp, code_bat_ter, code_gestionnaire, code_bat_gestionnaire, occupant, service_occupant),
    FOREIGN KEY(code_bat_ter, code_gestionnaire, code_bat_gestionnaire, import_timestamp)
      REFERENCES siep.bien_gestionnaire(code_bat_ter, code_gestionnaire, code_bat_gestionnaire, import_timestamp)
) PARTITION BY RANGE (import_timestamp);

---=========== Vues ===========---
DROP MATERIALIZED VIEW siep.bien_caracteristiques_complet_gestionnaire_vw;
CREATE MATERIALIZED VIEW siep.bien_caracteristiques_complet_gestionnaire_vw AS
    WITH oad_timestamps AS (
        SELECT DISTINCT snapshot_id, import_timestamp
        FROM siep.bien
    ),
    osfi_timestamps AS (
        SELECT DISTINCT snapshot_id, import_timestamp
        FROM siep.bien_information_complementaire
    ),
    couples_timestamps AS (
        -- Produit cartésien uniquement sur les timestamps par snapshot
        SELECT
            oad.snapshot_id,
            oad.import_timestamp as oad_import_timestamp,
            osfi.import_timestamp as osfi_import_timestamp
        FROM oad_timestamps oad
        JOIN osfi_timestamps osfi
            ON oad.snapshot_id = osfi.snapshot_id
    ),
    cte_bien_gest_oad_osfi AS (
        -- Liste complète des biens entre l'OAD et l'OSFI par snapshot_id
        SELECT DISTINCT
            code_bat_ter,
            code_gestionnaire,
            code_bat_gestionnaire,
            snapshot_id
        FROM siep.bien_gestionnaire
        UNION
        SELECT DISTINCT
            code_bat_ter,
            code_gestionnaire,
            code_bat_gestionnaire,
            snapshot_id
        FROM siep.bien_information_complementaire
    ), cte_bien_occupant_agrege AS (
        -- Agrégation des indicateurs à la maille code_bat_gestionnaire (bien gestionnaire)
        SELECT
        sbo.code_bat_gestionnaire,
        sbo.snapshot_id,
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
        GROUP BY sbo.snapshot_id, sbo.import_timestamp, sbo.code_bat_gestionnaire
    ),
    -- CTE pour consolider l'état au niveau code_bat_ter
    cte_etat_bat_consolide AS (
        SELECT
            sbic.snapshot_id,
            sbic.import_timestamp,
            sbic.code_bat_ter,
            -- Compte les occurrences avec état "ouvert"
            COUNT(CASE WHEN sbic.etat_bat = 'Ouvert' THEN 1 END) as nb_ouvert,
            -- Compte les occurrences avec état "fermé"
            COUNT(CASE WHEN sbic.etat_bat = 'Fermé' THEN 1 END) as nb_ferme,
            -- Compte les occurrences avec état différent de N/A
            COUNT(CASE WHEN sbic.etat_bat IS NOT NULL THEN 1 END) as nb_etat_non_null,
            -- Vérifie si le bâtiment existe dans l'OAD
            MAX(CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM siep.bien sb
                    WHERE sb.snapshot_id = sbic.snapshot_id
                    AND sb.code_bat_ter = sbic.code_bat_ter
                ) THEN 1
                ELSE 0
            END) as existe_dans_oad
        FROM siep.bien_information_complementaire sbic
        WHERE sbic.code_bat_ter IS NOT NULL
        GROUP BY sbic.snapshot_id, sbic.import_timestamp, sbic.code_bat_ter
    ),
    cte_etat_bat_final AS (
        SELECT
            snapshot_id,
            import_timestamp,
            code_bat_ter,
            CASE
                -- Si au moins un gestionnaire marque le bâtiment comme "ouvert"
                WHEN nb_ouvert > 0 THEN 'Ouvert'
                -- Si aucun ouvert mais au moins un fermé
                WHEN nb_ouvert = 0 AND nb_ferme > 0 THEN 'Fermé'
                -- Si tous les états sont N/A
                WHEN nb_etat_non_null = 0 THEN
                    CASE
                        WHEN existe_dans_oad = 1 THEN 'Ouvert'
                        ELSE 'Fermé'
                    END
                -- Cas par défaut : prendre un état non N/A s'il existe
                ELSE 'Indéterminé'
            END as etat_bat_recalcule
        FROM cte_etat_bat_consolide
    )
    SELECT
    -- bien_gestionnaire issues de l'OAD et OSFI
    cte_bgoo.code_bat_ter,
    cte_bgoo.code_bat_gestionnaire,
    cte_bgoo.code_gestionnaire,
    cte_bgoo.snapshot_id,
    -- siep.bien sb && siep.bien_information_complementaire sbic
    COALESCE(sb.code_site, sbic.code_site) as code_site,
    sb.categorie_administrative_liste_bat as categorie_administrative_liste_bat,
    sb.categorie_administrative_principale_bat as categorie_administrative_principale_bat,
    sb.date_construction_annee_corrigee as date_construction_annee_corrigee,
    sb.gestionnaire_type_simplifie_bat as gestionnaire_type_simplifie_bat,
    sb.gestionnaire_presents_liste_mef as gestionnaire_presents_liste_mef,
    sb.gest_princ_personnalite_juridique as gest_princ_personnalite_juridique,
    sb.gest_princ_personnalite_juridique_simplifiee as gest_princ_personnalite_juridique_simplifiee,
    sb.gest_princ_personnalite_juridique_precision as gest_princ_personnalite_juridique_precision,
    sb.gestionnaire_principal_code as gestionnaire_principal_code,
    sb.gestionnaire_principal_libelle as gestionnaire_principal_libelle,
    sb.gestionnaire_principal_libelle_simplifie as gestionnaire_principal_libelle_simplifie,
    sb.gestionnaire_principal_lien_mef as gestionnaire_principal_lien_mef,
    sb.gestionnaire_principal_ministere as gestionnaire_principal_ministere,
    sb.libelle_bat_ter as libelle_bat_ter,
    COALESCE(sb.gestion_mono_multi_min, sbic.gestion_mono_multi_min) as gestion_mono_multi_min,
    COALESCE(sb.gestion_mono_multi_mef, sbic.gestion_mono_multi_mef) as gestion_mono_multi_mef,
    COALESCE(sb.gestion_categorie, sbic.gestion_categorie) as gestion_categorie,
    -- siep.gestionnaire
    sgest.ministere AS ministere,
    sgest.libelle_gestionnaire AS libelle_gestionnaire,
    COALESCE(sgest.libelle_simplifie, 'Indéterminé') AS libelle_simplifie,
    sgest.libelle_abrege AS libelle_abrege,
    sgest.lien_mef_gestionnaire AS lien_mef_gestionnaire,
    sgest.personnalite_juridique AS gestionnaire_personnalite_juridique,
    sgest.personnalite_juridique_precision AS gestionnaire_personnalite_juridique_precision,
    sgest.personnalite_juridique_simplifiee AS gestionnaire_personnalite_juridique_simplifie,
    -- siep.bien_strategie sbs
    sbs.perimetre_spsi_initial as perimetre_spsi_initial,
    sbs.perimetre_spsi_maj as perimetre_spsi_maj,
    -- siep.bien_deet_energie_ges sbdeg && siep.bien_information_complementaire sbic
    sbdeg.bat_assujettis_deet as bat_assujettis_deet_oad,
    sbic.bat_assujettis_deet as bat_assujettis_deet_osfi,
    -- siep.bien_proprietaire sbp
    sbp.locatif_domanial as locatif_domanial,
    -- siep.bien_reglementation sbr
    sbr.reglementation_corrigee as reglementation_corrigee,
    -- siep.bien_typologie sbt && siep.bien_information_complementaire sbic
    COALESCE(sbt.usage_detaille_du_bien, sbic.usage_detaille_du_bien, 'Indéterminé') as usage_detaille_du_bien,
    -- siep.ref_typologie srt
    srt.bati_non_bati as bati_non_bati,
    srt.type_de_bien as type_de_bien,
    srt.famille_de_bien as famille_de_bien,
    srt.famille_de_bien_simplifiee as famille_de_bien_simplifiee,
    -- cte_bien_occupant_agrege cte_boa && siep.bien_information_complementaire sbic
    cte_boa.sum_indicateur_sub_occ as sum_indicateur_sub_occ,
    cte_boa.sum_indicateur_surface_mef_occ as sum_indicateur_surface_mef_occ,
    COALESCE(cte_boa.sum_indicateur_poste_occ, sbic.indicateur_poste_occ) as sum_indicateur_poste_occ,
    COALESCE(cte_boa.sum_indicateur_resident_occ, sbic.indicateur_resident_occ) as sum_indicateur_resident_occ,
    cte_boa.sum_indicateur_resident_reconstitue_occ as sum_indicateur_resident_reconstitue_occ,
    COALESCE(cte_boa.sum_indicateur_sub_occ_source, sbic.indicateur_sub_occ_source) as sum_indicateur_sub_occ_source,
    COALESCE(cte_boa.sum_indicateur_poste_occ_source, sbic.indicateur_poste_occ_source) as sum_indicateur_poste_occ_source,
    cte_boa.sum_indicateur_resident_occ_source  as sum_indicateur_resident_occ_source,
    -- siep.bien_information_complementaire sbic && cte_etat_bat_final cte_ebf
    cte_ebf.etat_bat_recalcule as etat_bat_recalcule,
    sbic.date_sortie_bat as date_sortie_bat,
    sbic.date_sortie_site as date_sortie_site,
    sbic.date_derniere_renovation as date_derniere_renovation,
    sbic.annee_reference as annee_reference,
    sbic.efa as efa,
    -- siep.conso_statut_batiment scsb
    -- scsb.statut_fluide_global as statut_fluide_global,
    -- scsb.statut_batiment as statut_batiment,
    -- Date d'import
    ct.oad_import_timestamp,
    ct.osfi_import_timestamp
    FROM cte_bien_gest_oad_osfi cte_bgoo
    LEFT JOIN couples_timestamps ct
    ON ct.snapshot_id = cte_bgoo.snapshot_id
    -- Jointures datasets issus de l'OAD
    LEFT JOIN siep.bien sb
        ON sb.snapshot_id = cte_bgoo.snapshot_id
        AND sb.import_timestamp = ct.oad_import_timestamp
        AND sb.code_bat_ter = cte_bgoo.code_bat_ter
    LEFT JOIN siep.gestionnaire sgest
        ON sgest.snapshot_id = cte_bgoo.snapshot_id
        AND sgest.import_timestamp = ct.oad_import_timestamp
        AND sgest.code_gestionnaire = cte_bgoo.code_gestionnaire
    LEFT JOIN siep.bien_strategie sbs
        ON sbs.snapshot_id = cte_bgoo.snapshot_id
        AND sbs.import_timestamp = ct.oad_import_timestamp
        AND sbs.code_bat_ter = cte_bgoo.code_bat_ter
    LEFT JOIN siep.bien_deet_energie_ges sbdeg
        ON sbdeg.snapshot_id = cte_bgoo.snapshot_id
        AND sbdeg.import_timestamp = ct.oad_import_timestamp
        AND sbdeg.code_bat_ter = cte_bgoo.code_bat_ter
    LEFT JOIN siep.bien_proprietaire sbp
        ON sbp.snapshot_id = cte_bgoo.snapshot_id
        AND sbp.import_timestamp = ct.oad_import_timestamp
        AND sbp.code_bat_ter = cte_bgoo.code_bat_ter
    LEFT JOIN siep.bien_reglementation sbr
        ON sbr.snapshot_id = cte_bgoo.snapshot_id
        AND sbr.import_timestamp = ct.oad_import_timestamp
        AND sbr.code_bat_ter = cte_bgoo.code_bat_ter
    LEFT JOIN siep.bien_typologie sbt
        ON sbt.snapshot_id = cte_bgoo.snapshot_id
        AND sbt.import_timestamp = ct.oad_import_timestamp
        AND sbt.code_bat_ter = cte_bgoo.code_bat_ter
    -- Jointures datasets issus de l'OSFI
    LEFT JOIN siep.bien_information_complementaire sbic
        ON sbic.snapshot_id = cte_bgoo.snapshot_id
        AND sbic.import_timestamp = ct.osfi_import_timestamp
        AND sbic.code_bat_gestionnaire = cte_bgoo.code_bat_gestionnaire
    -- LEFT JOIN siep.conso_statut_batiment scsb
    --    ON scsb.snapshot_id = cte_bgoo.snapshot_id
    --    AND scsb.import_timestamp = ct.osfi_import_timestamp
    --    AND scsb.code_bat_gestionnaire = cte_bgoo.code_bat_gestionnaire
    -- Jointure avec ref_typologie basée sur usage_detaille_du_bien coalescé
    LEFT JOIN siep.ref_typologie srt
        -- ON srt.snapshot_id = cte_bgoo.snapshot_id
        ON srt.usage_detaille_du_bien = COALESCE(sbt.usage_detaille_du_bien, sbic.usage_detaille_du_bien)
    -- Jointure avec cte_bien_occupant_agrege
    LEFT JOIN cte_bien_occupant_agrege cte_boa
        ON cte_boa.snapshot_id = cte_bgoo.snapshot_id
        AND cte_boa.import_timestamp = ct.oad_import_timestamp
        AND cte_boa.code_bat_gestionnaire = cte_bgoo.code_bat_gestionnaire
    -- Jointure avec l'état recalculé au niveau code_bat_ter
    LEFT JOIN cte_etat_bat_final cte_ebf
        ON cte_ebf.snapshot_id = cte_bgoo.snapshot_id
        AND cte_ebf.import_timestamp = ct.osfi_import_timestamp
        AND cte_ebf.code_bat_ter = cte_bgoo.code_bat_ter
    ;
