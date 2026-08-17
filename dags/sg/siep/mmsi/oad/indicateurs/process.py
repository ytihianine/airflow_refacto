import numpy as np
import pandas as pd
from modules.utils.process.text import normalize_whitespace_columns


def filter_bien(df: pd.DataFrame, df_bien: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les biens présents dans la table bien
    df = pd.merge(
        left=df_bien["code_bat_ter"],
        right=df,
        on="code_bat_ter",
        how="inner",
    )

    return df


def filtrer_oad_indic(df_oad_indic: pd.DataFrame, df_biens: pd.DataFrame) -> pd.DataFrame:
    return filter_bien(df=df_oad_indic, df_bien=df_biens)


def process_oad_indic(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["code_bat_ter"])
    df = df.drop_duplicates(subset=["code_bat_ter"], ignore_index=True)
    return df


def process_accessibilite(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "attestation_accessibilite",
        "beneficie_derogation",
        "code_bat_ter",
        "date_mise_en_accessibilite",
        "fait_objet_adap",
        "motif_derogation",
        "numero_adap",
        "presence_registre_accessibilite",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.assign(
        date_mise_en_accessibilite=pd.to_datetime(df["date_mise_en_accessibilite"], dayfirst=True),
        motif_derogation=df["motif_derogation"].str.split().str.join(" "),
    ).replace({"Oui": True, "Non": False})

    return df


def process_accessibilite_detail(df: pd.DataFrame) -> pd.DataFrame:
    def get_niveau_fonctionnel(niveau: str) -> str:
        if niveau is None:
            return None

        correspondance = {
            "NR.F": "Oui",
            "Ne sait pas": "Ne sait pas",
            "R.F": "Oui",
            "R.NF": "Non",
            "Sans objet": "Sans objet",
        }
        niveau = niveau.strip()

        return correspondance.get(niveau, "NC")

    def get_niveau_reglementaire(niveau: str) -> str:
        if niveau is None:
            return None

        correspondance = {
            "NR.F": "Non",
            "Ne sait pas": "Ne sait pas",
            "R.F": "Oui",
            "R.NF": "Oui",
            "Sans objet": "Sans objet",
        }
        niveau = niveau.strip()

        return correspondance.get(niveau, "NC")

    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "accessibilite_acces",
        "accessibilite_accueil",
        "accessibilite_general",
        "accessibilite_sanitaire",
        "accessibilite_services",
        "code_bat_ter",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.melt(
        id_vars=["code_bat_ter"],
        value_vars=[
            "accessibilite_acces",
            "accessibilite_accueil",
            "accessibilite_general",
            "accessibilite_sanitaire",
            "accessibilite_services",
        ],
        var_name="composant_bien",
        value_name="niveau",
    ).assign(composant_bien=lambda df: df["composant_bien"].str.replace("accessibilite_", "").str.title())

    # Create calculated columns
    df["niveau_fonctionnel"] = list(map(get_niveau_fonctionnel, df["niveau"]))
    df["niveau_reglementaire"] = list(map(get_niveau_reglementaire, df["niveau"]))

    return df


def process_bacs(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "classe_gtb",
        "code_bat_ter",
        "commentaire_general",
        "commentaire_soumission",
        "date_derniere_inspection",
        "date_installation_gtb",
        "presence_gtb",
        "soumis_decret_bacs",
    ]
    df = df.loc[:, cols_to_keep]

    df = (
        df.assign(
            date_installation_gtb=pd.to_datetime(df["date_installation_gtb"], dayfirst=True),
            date_derniere_inspection=pd.to_datetime(df["date_derniere_inspection"], dayfirst=True),
            presence_gtb=df["presence_gtb"].str.lower().replace({"oui": True, "non": False}),
            split_decret_bacs=df["soumis_decret_bacs"].str.split(pat=" ", n=1),
            classe_gtb=df["classe_gtb"].str.upper(),
            commentaire_soumission=df["commentaire_soumission"].str.split().str.join(" "),
            commentaire_general=df["commentaire_general"].str.split().str.join(" "),
        )
        .assign(
            soumis_decret_bacs=lambda df: (
                df["split_decret_bacs"].str[0].str.lower().replace({"oui": True, "non": False})
            ),
            raison_soumis_decret_bacs=lambda df: (
                df["split_decret_bacs"].str[1].str.replace("(", "").str.replace(")", "")
            ),
        )
        .drop(columns=["split_decret_bacs"])
    )

    return df


def process_bails(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "date_debut_bail",
        "date_fin_bail",
        "duree_bail",
        "type_contrat",
    ]
    df = df.loc[:, cols_to_keep]

    df[["date_debut_bail", "date_fin_bail"]] = df[["date_debut_bail", "date_fin_bail"]].apply(
        pd.to_datetime, format="%d/%m/%Y", errors="coerce"
    )
    df["type_contrat"] = df["type_contrat"].str.split().str.join(" ")

    return df


def process_couts(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "annee_charges_copropriete",
        "annee_charges_locatives",
        "annee_loyers_budgetaires",
        "annee1_charges_fonct",
        "annee1_loyer_ht_hc_ttc",
        "annee2_charges_fonc",
        "annee2_loyer_ht_hc_ttc",
        "charges_copropriete",
        "charges_fonc_annee1",
        "charges_fonc_annee2",
        "charges_locatives",
        "code_bat_ter",
        "codhc_surfacique_sub",
        "loyer_annuel_ht_annee1",
        "loyer_annuel_ht_annee2",
        "loyer_budgetaire_ttc",
        "loyer_hc_ttc_annee1",
        "loyer_hc_ttc_annee2",
        "loyer_surfacique_sub",
        "loyers_budgetaires_2018",
        "plafond_loyer_surfacique_sub",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.assign(
        annee_charges_copropriete=df["annee_charges_copropriete"].replace("-", None),
        annee_charges_locatives=df["annee_charges_locatives"].replace("-", None),
        annee_loyers_budgetaires=df["annee_loyers_budgetaires"].replace("-", None),
        annee1_charges_fonct=df["annee1_charges_fonct"].replace("-", None),
        annee1_loyer_ht_hc_ttc=df["annee1_loyer_ht_hc_ttc"].replace("-", None),
        annee2_charges_fonc=df["annee2_charges_fonc"].replace("-", None),
        annee2_loyer_ht_hc_ttc=df["annee2_loyer_ht_hc_ttc"].replace("-", None),
    )

    return df


def process_deet_energie(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "annee1_conso_eau",
        "annee1_conso_ef_et_ges",
        "annee2_conso_eau",
        "annee2_conso_ef_et_ges",
        "bat_assujettis_deet",
        "code_bat_ter",
        "conso_eau_annee1",
        "conso_eau_annee2",
        "conso_ef_annee1",
        "conso_ef_annee2",
        "deet_commentaire",
        "emission_ges_annee1",
        "emission_ges_annee2",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.assign(
        annee1_conso_ef_et_ges=df["annee1_conso_ef_et_ges"].replace("-", None),
        annee2_conso_ef_et_ges=df["annee2_conso_ef_et_ges"].replace("-", None),
        annee1_conso_eau=df["annee1_conso_eau"].replace("-", None),
        annee2_conso_eau=df["annee2_conso_eau"].replace("-", None),
        bat_assujettis_deet=df["bat_assujettis_deet"].str.lower().replace({"oui": True, "non": False}),
        deet_commentaire=df["deet_commentaire"].str.split().str.join(" "),
    )

    return df


def process_eds(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "eds_ascenseurs_constate",
        "eds_ascenseurs_theorique",
        "eds_charpente_constate",
        "eds_charpente_theorique",
        "eds_clotures_constate",
        "eds_clotures_theorique",
        "eds_couv_ext_constate",
        "eds_couv_ext_theorique",
        "eds_distri_chauff_constate",
        "eds_distri_chauff_theorique",
        "eds_distri_clim_constate",
        "eds_distri_clim_theorique",
        "eds_elec_cfa_info_constate",
        "eds_elec_cfa_info_theorique",
        "eds_elec_cfa_reseaux_constate",
        "eds_elec_cfa_reseaux_theorique",
        "eds_elec_cfo_dist_prim_constate",
        "eds_elec_cfo_dist_prim_theorique",
        "eds_elec_cfo_dist_sec_constate",
        "eds_elec_cfo_dist_sec_theorique",
        "eds_elec_cfo_ecl_constate",
        "eds_elec_cfo_ecl_theorique",
        "eds_emiss_chauff_constate",
        "eds_emiss_chauff_theorique",
        "eds_emiss_clim_constate",
        "eds_emiss_clim_theorique",
        "eds_es_amen_ext_constate",
        "eds_es_amen_int_constate",
        "eds_es_clos_couvert_structure_constate",
        "eds_es_equipements_constate",
        "eds_escal_int_constate",
        "eds_escal_int_theorique",
        "eds_espaces_verts_constate",
        "eds_espaces_verts_theorique",
        "eds_etat_de_sante_general",
        "eds_facade_constate",
        "eds_facade_theorique",
        "eds_incendie_extinction_constate",
        "eds_incendie_extinction_theorique",
        "eds_incendie_securite_constate",
        "eds_incendie_securite_theorique",
        "eds_murs_int_constate",
        "eds_murs_int_theorique",
        "eds_ouvert_ext_constate",
        "eds_ouvert_ext_theorique",
        "eds_ouvert_int_constate",
        "eds_ouvert_int_theorique",
        "eds_plafonds_constate",
        "eds_plafonds_theorique",
        "eds_prod_chauff_constate",
        "eds_prod_chauff_theorique",
        "eds_prod_clim_constate",
        "eds_prod_clim_theorique",
        "eds_reseaux_constate",
        "eds_reseaux_theorique",
        "eds_sanit_distri_eau_constate",
        "eds_sanit_distri_eau_theorique",
        "eds_sanit_evac_constate",
        "eds_sanit_evac_theorique",
        "eds_sanit_prod_eau_constate",
        "eds_sanit_prod_eau_theorique",
        "eds_sanit_robin_constate",
        "eds_sanit_robin_theorique",
        "eds_signal_constate",
        "eds_signal_theorique",
        "eds_sols_constate",
        "eds_sols_theorique",
        "eds_structure_constate",
        "eds_structure_theorique",
        "eds_ventil_constate",
        "eds_ventil_theorique",
        "eds_voiries_constate",
        "eds_voiries_theorique",
    ]
    df = df.loc[:, cols_to_keep]

    common_col = ["code_bat_ter"]

    # Traitement partie "théorique"
    theorique_cols = [colname for colname in df.columns if colname.endswith("theorique")]
    df_theorique = df.loc[:, common_col + theorique_cols]
    df_theorique = df_theorique.melt(id_vars=common_col, var_name="composant_bien", value_name="eds_theorique")
    df_theorique = df_theorique.assign(
        composant_bien=lambda df: (
            df["composant_bien"].str.replace("_theorique", "").str.replace("eds_", "").str.replace("_", " ").str.title()
        )
    )

    # Traitement partie "constaté"
    constate_cols = [colname for colname in df.columns if colname.endswith("constate") or colname.endswith("general")]
    df_constate = df.loc[:, common_col + constate_cols]
    df_constate = df_constate.melt(id_vars=common_col, var_name="composant_bien", value_name="eds_constate")
    df_constate = df_constate.assign(
        composant_bien=lambda df: (
            df["composant_bien"].str.replace("_constate", "").str.replace("eds_", "").str.replace("_", " ").str.title()
        )
    )

    # Join
    df = pd.merge(df_theorique, df_constate, on=["code_bat_ter", "composant_bien"], how="outer")

    df = df.assign(
        composant_bien=lambda df: (
            df["composant_bien"]
            .str.replace("_constate", "")
            .str.replace("_theorique", "")
            .str.replace("eds_", "")
            .str.replace("_", " ")
            .str.title()
        )
    )

    return df


def process_exploitation(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "ee_env_commentaire",
        "ee_env_etat",
        "ee_equipement_tech_etat",
        "ee_equipements_tech_commentaire",
        "exploitation_ascenseur_commentaire",
        "exploitation_ascenseur_etat",
        "exploitation_bureautique_commentaire",
        "exploitation_bureautique_etat",
        "exploitation_chauffage_commentaire",
        "exploitation_chauffage_etat",
        "exploitation_e_c_s_commentaire",
        "exploitation_e_c_s_etat",
        "exploitation_eclairage_artificiel_commentaire",
        "exploitation_eclairage_artificiel_etat",
        "exploitation_menuiseries_ext_commentaire",
        "exploitation_menuiseries_ext_etat",
        "exploitation_murs_ext_commentaire",
        "exploitation_murs_ext_etat",
        "exploitation_planchers_bas_commentaire",
        "exploitation_planchers_bas_etat",
        "exploitation_refroidissement_commentaire",
        "exploitation_refroidissement_etat",
        "exploitation_toiture_commentaire",
        "exploitation_toiture_etat",
        "exploitation_ventilation_commentaire",
        "exploitation_ventilation_etat",
    ]
    df = df.loc[:, cols_to_keep]

    common_col = ["code_bat_ter"]

    # Traitement partie "théorique"
    commentaire_cols = [colname for colname in df.columns if colname.endswith("commentaire")]
    df_commentaire = df.loc[:, common_col + commentaire_cols]
    df_commentaire = df_commentaire.melt(id_vars=common_col, var_name="composant_bien", value_name="commentaire")
    df_commentaire = df_commentaire.assign(
        composant_bien=lambda df: (df["composant_bien"].str.replace("_commentaire", ""))
    )

    # Traitement partie "constaté"
    etat_cols = [colname for colname in df.columns if colname.endswith("etat")]
    df_etat = df.loc[:, common_col + etat_cols]
    df_etat = df_etat.melt(id_vars=common_col, var_name="composant_bien", value_name="etat_exploitation")
    df_etat = df_etat.assign(composant_bien=lambda df: (df["composant_bien"].str.replace("_etat", "")))

    # Join
    df = pd.merge(df_commentaire, df_etat, on=["code_bat_ter", "composant_bien"], how="outer")
    df = df.assign(
        composant_bien=lambda df: (
            df["composant_bien"]
            .str.replace("_commentaire", "")
            .str.replace("_etat", "")
            .str.replace("exploitation_", "")
            .str.replace("_", "")
            .str.title()
        ),
        commentaire=df["commentaire"].str.split().str.join(" "),
    )

    return df


def process_localisation(
    df_oad_carac: pd.DataFrame, df_oad_indic: pd.DataFrame, df_biens: pd.DataFrame
) -> pd.DataFrame:
    # Merging data from both sources
    df = pd.merge(left=df_oad_carac, right=df_oad_indic, on="code_bat_ter", how="left")

    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "latitude",
        "longitude",
        "adresse_normalisee",
        "adresse_source",
        "code_insee_normalise",
        "commune_mef_hmef",
        "commune_normalisee",
        "commune_source",
        "france_etranger",
        "metropole_outremer",
        "num_departement_normalisee",
        "num_departement_source",
        "code_iso_departement_normalise",
        "code_iso_region_normalise",
    ]
    df = df.loc[:, cols_to_keep]

    # Filtrer les lignes
    df = df.drop_duplicates(
        subset=["code_bat_ter"],
        keep="first",
        ignore_index=True,
    )
    df = filter_bien(df=df, df_bien=df_biens)

    # Cleaning data
    txt_cols = [
        "adresse_normalisee",
        "adresse_source",
        "commune_mef_hmef",
        "commune_normalisee",
        "commune_source",
        "france_etranger",
        "metropole_outremer",
        "num_departement_normalisee",
        "num_departement_source",
        "code_iso_departement_normalise",
        "code_iso_region_normalise",
    ]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Ajout des colonnes additionnelles
    conditions = [
        (df["num_departement_normalisee"] == "Etranger"),
        (df["num_departement_normalisee"].str.len() == 2),
        (df["num_departement_normalisee"].str.len() == 3),
    ]
    choices = ["Etranger", "Métropole", "Outre-mer"]
    df["metropole_outremer"] = np.select(condlist=conditions, choicelist=choices, default="Indéterminé")

    return df


def process_notes(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "completude",
        "modernisation",
        "optimisation",
        "preservation",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.assign(
        modernisation=df["modernisation"].str.replace(",", "."),
        optimisation=df["optimisation"].str.replace(",", "."),
        preservation=df["preservation"].str.replace(",", "."),
    )
    return df


def process_effectif(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "effectif_administratif",
        "effectif_physique",
        "nb_positions_de_travail",
        "nb_postes",
        "nb_residents",
    ]
    df = df.loc[:, cols_to_keep]

    return df


def process_proprietaire(df: pd.DataFrame) -> pd.DataFrame:
    def get_locatif_domanial(statut_occupation: str) -> str:
        correspondance = {
            "L'Etat ne possède pas et n'occupe pas en tant que services de l'Etat": "Locatif",
            "L'Etat ne possède pas et occupe en tant que services de l'Etat": "Locatif",
            "L'Etat possède et n'occupe pas en tant que services de l'Etat": "Domanial",
            "L'Etat possède et occupe en tant que services de l'Etat": "Domanial",
        }
        statut_occupation = statut_occupation.strip()

        return correspondance.get(statut_occupation, "NC")

    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "numero_proprietaire",
        "statut_occupation",
        "type_proprietaire",
        "type_proprietaire_detail",
    ]
    df = df.loc[:, cols_to_keep]

    # Create calculated column
    df["locatif_domanial"] = list(map(get_locatif_domanial, df["statut_occupation"]))

    return df


def process_reglementation(df: pd.DataFrame) -> pd.DataFrame:
    def get_reglementation_corrigee(reglementation: str) -> str:
        if reglementation is None:
            return None
        correspondance = {
            "ERP": "ERP",
            "Code du Travail (non ERP)": "Code du travail (non ERP)",
            "Code du travail (non ERP)": "Code du travail (non ERP)",
            "Autre": "Autre",
            "Bâtiment d'habitation": "Bâtiment d'habitation",
        }
        reglementation = reglementation.strip()

        return correspondance.get(reglementation, "NC")

    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "classement_monument_historique",
        "code_bat_ter",
        "igh",
        "reglementation",
    ]
    df = df.loc[:, cols_to_keep]

    df = df.assign(
        igh=(df["igh"].str.lower().replace({"oui": True, "non": False})),
    )

    # Create calculated column
    df["reglementation_corrigee"] = list(map(get_reglementation_corrigee, df["reglementation"]))

    return df


def process_strategie(df_oad_carac: pd.DataFrame, df_oad_indic: pd.DataFrame, df_biens: pd.DataFrame) -> pd.DataFrame:
    df = pd.merge(left=df_oad_carac, right=df_oad_indic, on="code_bat_ter", how="left")
    df = df.drop_duplicates(
        subset=["code_bat_ter"],
        keep="first",
        ignore_index=True,
    )
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "perimetre_spsi_initial",
        "perimetre_spsi_maj",
        "segmentation_sdir_spsi",
        "segmentation_theorique_sdir_spsi",
        "statut_osc",
    ]
    df = df.loc[:, cols_to_keep]
    df = filter_bien(df=df, df_bien=df_biens)

    return df


def process_surface(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "contenance_cadastrale",
        "sba",
        "sba_optimisee",
        "shon",
        "sub",
        "sub_optimisee",
        "sun",
        "surface_aire_amenagee",
        "surface_de_plancher",
    ]
    df = df.loc[:, cols_to_keep]
    return df


def process_valeur(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "valorisation_chorus",
    ]
    df = df.loc[:, cols_to_keep]
    return df


def process_typologie(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les colonnes de ce dataset
    cols_to_keep = [
        "code_bat_ter",
        "usage_detaille_du_bien",
    ]
    df = df.loc[:, cols_to_keep]
    return df
