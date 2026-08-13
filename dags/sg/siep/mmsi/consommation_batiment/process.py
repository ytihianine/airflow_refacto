import functools

import numpy as np
import pandas as pd
from dags.sg.siep.mmsi.consommation_batiment.enums import Statuts, TypeEnergie
from modules.utils.process.text import (
    convert_str_cols_to_date,
    normalize_whitespace_columns,
)


# ======================================================
# FONCTIONS DE TRAITEMENT DES DONNEES
# ======================================================
def _unpivot_conso_mens(df: pd.DataFrame, use_conso_corrigee: bool) -> pd.DataFrame:
    col_commune = ["code_bat_gestionnaire", "annee_conso", "date_conso"]

    if use_conso_corrigee:
        # Conso corrigée
        value_name = "conso_corr_dju_mmsi"
        col_conso = [
            "conso_elec_corr_dju_mmsi",
            "conso_gaz_pci_corr_dju_mmsi",
            "conso_reseau_chaleur_corr_dju_mmsi",
            "conso_reseau_froid_corr_dju_mmsi",
            "conso_fioul_pci_corr_dju_mmsi",
        ]
    else:
        # Conso brute
        value_name = "conso_brute"
        col_conso = [
            "conso_elec",
            "conso_gaz_pci",
            "conso_reseau_chaleur",
            "conso_reseau_froid",
            "conso_fioul_pci",
        ]

    df["annee_conso"] = df["date_conso"].dt.year
    df = pd.melt(
        frame=df,
        id_vars=col_commune,
        value_vars=col_conso,
        var_name="type_energie",
        value_name=value_name,
    )

    return df


def calculer_dju_moyen(df: pd.DataFrame) -> pd.DataFrame:
    df_moyen = df.groupby(by=["code_bat_gestionnaire", "mois_conso"], as_index=False).mean()
    df_moyen = df_moyen.rename(columns={"degres_jours_de_chauffage": "dju_moyen"})
    return df_moyen


def determiner_ratio_par_fluide(df: pd.DataFrame) -> pd.DataFrame:
    df["ratio_electricite"] = 1
    # autres fluides fait référence à tous les fluides sauf l'électricité
    ratios_autres_fluides = list(
        map(
            functools.partial(determiner_ratio, "gaz"),
            df["mois_conso"],
            df["degres_jours_de_chauffage"],
            df["dju_moyen"],
        )
    )
    df["ratio_autres_fluides"] = ratios_autres_fluides
    df.sort_values(by=["code_bat_gestionnaire", "annee_conso", "mois_conso"], inplace=True)
    return df


def convertir_pcs_en_pci(df: pd.DataFrame) -> pd.DataFrame:
    df["conso_gaz_pci"] = 0.9 * df["conso_gaz_pcs"]
    # Les données sources sont en pc pour le fioul désormais
    # df["conso_fioul_pci"] = 0.9 * df["conso_fioul_pcs"]
    return df


def calculer_consommation_corrigee(df: pd.DataFrame) -> pd.DataFrame:
    df["conso_elec_corr_dju_mmsi"] = df["conso_elec"] * df["ratio_electricite"]

    df["conso_gaz_pci_corr_dju_mmsi"] = df["conso_gaz_pci"] * df["ratio_autres_fluides"]
    df["conso_reseau_chaleur_corr_dju_mmsi"] = df["conso_reseau_chaleur"] * df["ratio_autres_fluides"]
    df["conso_reseau_froid_corr_dju_mmsi"] = df["conso_reseau_froid"] * df["ratio_autres_fluides"]
    df["conso_fioul_pci_corr_dju_mmsi"] = df["conso_fioul_pci"] * df["ratio_autres_fluides"]
    df["conso_granule_bois_corr_dju_mmsi"] = df["conso_granule_bois"] * df["ratio_autres_fluides"]
    return df


def determiner_ratio(type_energie: str, mois_conso: float, dju_ref: float, dju_moy: float) -> float | None:
    ratio = 1

    if pd.isna(mois_conso):
        return None

    if pd.isna(dju_ref):
        return None

    if pd.isna(dju_moy):
        return None

    if type_energie == TypeEnergie.electricite.value:
        return ratio

    mois_conso = int(mois_conso)
    mois_ete = [6, 7, 8, 9]  # "Juin", "Juillet", "Août", "Septembre"

    if mois_conso in mois_ete:
        return ratio

    try:
        ratio = 0.7 * (dju_moy / dju_ref) + 0.3

        if ratio > 1.4:
            ratio = 1.4

        if ratio < 0.6:
            ratio = 0.6
    except ZeroDivisionError:
        ratio = 0

    return ratio


def determiner_statut_fluide_global(
    statut_elec: str,
    statut_gaz: str,
    statut_reseau_chaud: str,
    statut_reseau_froid: str,
) -> str:
    lst_statut_par_fluide = [statut_gaz, statut_reseau_chaud, statut_reseau_froid]

    if statut_elec == Statuts.complet.value and all(
        element is None or element == Statuts.complet.value for element in lst_statut_par_fluide
    ):
        return Statuts.complet.value

    if statut_elec == Statuts.debut_exp.value and all(
        element is None or element == Statuts.debut_exp.value for element in lst_statut_par_fluide
    ):
        return Statuts.debut_exp.value

    if statut_elec == Statuts.fin_exp.value and all(
        element is None or element == Statuts.fin_exp.value for element in lst_statut_par_fluide
    ):
        return Statuts.fin_exp.value

    return Statuts.incomplet.value


def add_dju_moyen(df_conso_mens: pd.DataFrame) -> pd.DataFrame:

    df_dju_moyen = calculer_dju_moyen(
        df=df_conso_mens.loc[:, ["code_bat_gestionnaire", "mois_conso", "degres_jours_de_chauffage"]]
    )

    # Besoin de merge les DF pour récupérer la colonne DJU moyen
    df_conso_mens = pd.merge(
        left=df_conso_mens,
        right=df_dju_moyen,
        how="left",
        left_on=["code_bat_gestionnaire", "mois_conso"],
        right_on=["code_bat_gestionnaire", "mois_conso"],
    )

    return df_conso_mens


def corriger_consommation(df_conso_mens: pd.DataFrame) -> pd.DataFrame:
    # Etape 2 - 1: déterminer le ratio
    df_conso_mens = determiner_ratio_par_fluide(df=df_conso_mens)
    print(
        df_conso_mens[
            [
                "annee_conso",
                "mois_conso",
                "degres_jours_de_chauffage",
                "dju_moyen",
                "ratio_electricite",
                "ratio_autres_fluides",
            ]
        ].head()
    )

    # Etape 2 - 2: calculer la consommation corrigée
    df_conso_mens = calculer_consommation_corrigee(df=df_conso_mens)
    print(
        df_conso_mens[
            [
                "annee_conso",
                "mois_conso",
                "dju_moyen",
                "degres_jours_de_chauffage",
                "conso_gaz_pcs",
                "ratio_autres_fluides",
                "conso_gaz_pci_corr_dju_mmsi",
            ]
        ].head()
    )

    df_conso_mens = df_conso_mens.drop(columns=["annee_conso", "mois_conso"])
    print(df_conso_mens.columns)

    return df_conso_mens


# ======================================================
# BIEN INFO COMP
# ======================================================
def process_source_bien_info_comp(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliser les données textuelles
    txt_cols = [
        "code_site",
        "usage_detaille_du_bien",
        "famille_de_bien",
        "etat_bat",
        "efa",
    ]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Ajout des colonnes - backward compatibility
    date_cols = ["date_entree_occupant", "date_sortie_occupant"]
    if not set(date_cols).issubset(set(df.columns)):
        for col in date_cols:
            df[col] = pd.NaT
    else:
        # Normaliser les dates
        df = convert_str_cols_to_date(
            df=df,
            columns=date_cols,
            str_date_format="%d-%m-%Y %H:%M:%S",
            errors="raise",
        )

    # Regroupement
    df_grouped = df.groupby(by=["code_bat_ter"], as_index=False)["code_bat_gestionnaire"].count()
    df_grouped = df_grouped.rename(columns={"code_bat_gestionnaire": "nb_code_bat_gestionnaire"})  # type: ignore

    # Catégoriser les données
    df_grouped["gestion_mono_multi_mef"] = np.where(
        df_grouped["nb_code_bat_gestionnaire"] > 1,
        "Multi gest MEF",
        "Mono gest MEF",
    )

    # Jointure pour récupérer gestion_mono_multi_mef dans df
    df = pd.merge(
        left=df,
        right=df_grouped.loc[:, ["code_bat_ter", "gestion_mono_multi_mef"]],
        on="code_bat_ter",
        how="left",
    )

    # Colonnes additionnelles
    df["gestion_mono_multi_min"] = np.where(df["gestion_mono_multi_min"], "Multi", "Mono")
    df["code_site"] = pd.to_numeric(arg=df["code_site"].str.split().str[-1])

    conditions = [
        (df["gestion_mono_multi_min"] == "Mono") & (df["gestion_mono_multi_mef"] == "Mono gest MEF"),
        (df["gestion_mono_multi_min"] == "Mono") & (df["gestion_mono_multi_mef"] == "MEF multi gest"),
        (df["gestion_mono_multi_min"] == "Multi") & (df["gestion_mono_multi_mef"] == "Mixte dont MEF mono gest"),
        (df["gestion_mono_multi_min"] == "Multi") & (df["gestion_mono_multi_mef"] == "Mixte dont MEF multi gest"),
    ]
    choices = [
        "MEF mono gest",
        "MEF multi gest",
        "Mixte dont MEF mono gest",
        "Mixte dont MEF multi gest",
    ]
    df["gestion_categorie"] = np.select(condlist=conditions, choicelist=choices, default="Indéterminé")
    return df


# ======================================================
# CONSOMMATION MENSUELLE
# ======================================================
def process_conso_mensuelles(df: pd.DataFrame) -> pd.DataFrame:
    # Ajout d'informations additionnelles
    df["date_conso"] = pd.to_datetime(df["date_conso"])
    df["mois_conso"] = df["date_conso"].dt.month
    df["annee_conso"] = df["date_conso"].dt.year

    df["ligne_avec_conso"] = np.where(df["date_conso"].notna(), True, False)

    cols_conso = [col for col in df.columns if col.startswith("conso_") and "_surfacique" not in col]
    for col in cols_conso:
        colname = col.replace("conso_", "conso_presente_")
        df[colname] = np.where(df[col].notna(), 1, 0)

    # Conversion des fluides PCS en PCI
    df = convertir_pcs_en_pci(df=df)

    # Etape 1
    df = add_dju_moyen(df_conso_mens=df)

    # Etape 2
    df = corriger_consommation(df_conso_mens=df)

    return df


def process_unpivot_conso_mens_brute(df: pd.DataFrame) -> pd.DataFrame:
    df = _unpivot_conso_mens(df=df, use_conso_corrigee=False)
    return df


def process_unpivot_conso_mens_corrigee(df: pd.DataFrame) -> pd.DataFrame:
    df = _unpivot_conso_mens(df=df, use_conso_corrigee=True)
    return df


# ======================================================
# CONSOMMATION ANNUELLE
# ======================================================
def process_conso_annuelle(df: pd.DataFrame) -> pd.DataFrame:
    # Get Year from date
    df["annee"] = df["date_conso"].dt.year

    # Mise au bon format pour déterminer le statut de chaque fluide
    cols_id = ["code_bat_gestionnaire", "annee"]
    cols_to_drop = [
        "date_conso",
        "dju_moyen",
        "degres_jours_de_chauffage",
        "degres_jours_de_refroidissement",
        "ratio_electricite",
        "ratio_autres_fluides",
        "ligne_avec_conso",
        "snapshot_id",
        "snapshot_id_parent",
        "import_timestamp",
    ]

    df = df.drop(columns=cols_to_drop)
    cols_fluides = list(set(df.columns) - set(cols_id))

    # On calcule la conso totale pour chaque année
    df = df.groupby(by=cols_id)[cols_fluides].sum(min_count=1).reset_index()

    return df


def process_conso_annuelle_unpivot(df: pd.DataFrame) -> pd.DataFrame:
    df = process_conso_annuelle(df=df)
    correspondance_fluide = {
        "conso_elec": "elec",
        "conso_elec_corr_dju_mmsi": "elec",
        "conso_gaz_pcs": "gaz_pcs",
        "conso_gaz_pci": "gaz_pci",
        "conso_gaz_pci_corr_dju_mmsi": "gaz_pci",
        "conso_reseau_chaleur": "RCU",
        "conso_reseau_chaleur_corr_dju_mmsi": "RCU",
        "conso_reseau_froid": "RFU",
        "conso_reseau_froid_corr_dju_mmsi": "RFU",
        "conso_fioul_pci": "fioul",
        "conso_fioul_pci_corr_dju_mmsi": "fioul",
        "conso_granule_bois": "granule_bois",
        "conso_granule_bois_corr_dju_mmsi": "granule_bois",
        "conso_propane": "propane",
        "conso_photovoltaique": "photovoltaique",
    }
    cols_id = ["code_bat_gestionnaire", "annee"]
    cols_to_pivot = list(set(df.columns) - set(cols_id))

    df = pd.melt(
        frame=df,
        id_vars=cols_id,
        value_vars=cols_to_pivot,
        var_name="fluide",
        value_name="conso",
    )

    df = df.loc[df["fluide"].isin(list(correspondance_fluide.keys()))]
    df["type_conso"] = np.where(df["fluide"].str.contains("corr"), "CORRIGEE", "BRUTE")
    df["fluide"] = df["fluide"].replace(correspondance_fluide)

    return df


def process_conso_annuelle_unpivot_comparaison(df: pd.DataFrame) -> pd.DataFrame:
    # Filtrage initial des données
    df_filtered = df[(df["annee"] >= 2019)].copy()

    # Agrégation des consommations par année
    conso_par_annee = df_filtered.groupby(["code_bat_gestionnaire", "annee", "fluide", "type_conso"], as_index=False)[
        "conso"
    ].sum()

    # Création du référentiel complet (toutes les combinaisons bâtiment/fluide/type_conso)
    referentiel_complet = conso_par_annee.loc[:, ["code_bat_gestionnaire", "fluide", "type_conso"]].drop_duplicates()

    # Liste de toutes les années disponibles
    annees_disponibles = pd.DataFrame(data={"annee": conso_par_annee.loc[:, "annee"].unique()})

    # Produit cartésien : chaque bâtiment x fluide x année
    grille_complete = referentiel_complet.merge(right=annees_disponibles, how="cross")

    # Jointure avec les données réelles (LEFT JOIN)
    conso_complete = grille_complete.merge(
        right=conso_par_annee,
        on=["code_bat_gestionnaire", "fluide", "type_conso", "annee"],
        how="left",
    )

    # Remplacement des valeurs manquantes par 0
    conso_complete["conso"] = conso_complete["conso"].fillna(0)

    # Auto-jointure pour comparer chaque année avec toutes les années précédentes
    df = conso_complete.merge(
        right=conso_complete,
        on=["code_bat_gestionnaire", "fluide", "type_conso"],
        suffixes=("", "_comparaison"),
    )

    # Filtre : année actuelle >= année précédente
    df = df.loc[df["annee"] >= df["annee_comparaison"]]

    # Calcul de la différence
    df["diff_vs_comparaison"] = df["conso"] - df["conso_comparaison"]

    # Sélection et renommage des colonnes finales
    df = df.loc[
        :,
        [
            "code_bat_gestionnaire",
            "fluide",
            "type_conso",
            "annee",
            "annee_comparaison",
            "conso",
            "conso_comparaison",
            "diff_vs_comparaison",
        ],
    ]

    return df


def process_facture_annuelle_unpivot(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = ["import_date", "import_timestamp", "snapshot_id"]
    df = df.drop(columns=cols_to_drop)
    correspondance_facture = {
        "facture_elec_ht": "elec",
        "facture_elec_ttc": "elec",
        "facture_gaz_ht": "gaz",
        "facture_gaz_ttc": "gaz",
        "facture_eau_htva": "eau",
        "facture_eau_ttc": "eau",
        "facture_reseau_chaleur_htva": "RCU",
        "facture_reseau_chaleur_ttc": "RCU",
        "facture_reseau_froid_htva": "RFU",
        "facture_reseau_froid_ttc": "RFU",
        "facture_fioul_htva": "FIOUL",
        "facture_fioul_ttc": "FIOUL",
        "facture_granule_bois_htva": "granule bois",
        "facture_granule_bois_ttc": "granule bois",
        "facture_propane_htva": "propane",
        "facture_propane_ttc": "propane",
        "facture_photovoltaique_ht": "photovoltaique",
        "facture_photovoltaique_ttc": "photovoltaique",
    }

    cols_id = ["code_bat_gestionnaire", "annee"]
    cols_to_pivot = list(set(df.columns) - set(cols_id))

    # Unpivot du dataset source
    df = pd.melt(
        frame=df,
        id_vars=cols_id,
        value_vars=cols_to_pivot,
        var_name="fluide",
        value_name="montant_facture",
    )
    df = df.loc[df["fluide"].isin(list(correspondance_facture.keys()))]
    df["type_facture"] = np.where(df["fluide"].str.contains("htva|ht", regex=True), "HT/HTVA", "TTC")
    df["fluide"] = df["fluide"].replace(correspondance_facture)

    return df


def process_facture_annuelle_unpivot_comparaison(df: pd.DataFrame) -> pd.DataFrame:
    # Filtrage initial des données
    df_filtered = df[(df["annee"] >= 2019)].copy()

    # Agrégation des montants par année
    factures_par_annee = df_filtered.groupby(
        ["code_bat_gestionnaire", "annee", "fluide", "type_facture"], as_index=False
    )["montant_facture"].sum()

    # Création du référentiel complet (toutes les combinaisons bâtiment/fluide/type_facture)
    referentiel_complet = factures_par_annee.loc[
        :, ["code_bat_gestionnaire", "fluide", "type_facture"]
    ].drop_duplicates()

    # Liste de toutes les années disponibles
    annees_disponibles = pd.DataFrame(data={"annee": factures_par_annee.loc[:, "annee"].unique()})

    # Produit cartésien : chaque bâtiment x fluide x année
    grille_complete = referentiel_complet.merge(right=annees_disponibles, how="cross")

    # Jointure avec les données réelles (LEFT JOIN)
    factures_complete = grille_complete.merge(
        right=factures_par_annee,
        on=["code_bat_gestionnaire", "fluide", "type_facture", "annee"],
        how="left",
    )

    # Remplacement des valeurs manquantes par 0
    factures_complete["montant_facture"] = factures_complete["montant_facture"].fillna(0)

    # Auto-jointure pour comparer chaque année avec toutes les années précédentes
    df = factures_complete.merge(
        factures_complete,
        on=["code_bat_gestionnaire", "fluide", "type_facture"],
        suffixes=("", "_comparaison"),
    )

    # Filtre : année actuelle >= année précédente
    df = df.loc[df["annee"] >= df["annee_comparaison"]]

    # Calcul de la différence
    df["diff_vs_comparaison"] = df["montant_facture"] - df["montant_facture_comparaison"]

    # Sélection et renommage des colonnes finales
    df = df.loc[
        :,
        [
            "code_bat_gestionnaire",
            "fluide",
            "type_facture",
            "annee",
            "annee_comparaison",
            "montant_facture",
            "montant_facture_comparaison",
            "diff_vs_comparaison",
        ],
    ]

    return df


def process_conso_statut_par_fluide(df: pd.DataFrame) -> pd.DataFrame:
    # Unpivot du dataframe
    cols_id = ["code_bat_gestionnaire", "annee"]
    cols_conso_presente = [col for col in df.columns if col.startswith("conso_presente_")]
    df = pd.melt(
        frame=df,
        id_vars=cols_id,
        value_vars=cols_conso_presente,
        var_name="type_fluide",
        value_name="conso_presente",
    ).reset_index(drop=True)

    # Déterminer le statut de chaque fluide
    df["statut_du_fluide"] = np.where(df["conso_presente"] == 12, Statuts.complet.value, Statuts.incomplet.value)

    return df


def process_conso_statut_batiment(df: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les lignes concernant l'élec et le gaz
    df = df.loc[df["type_fluide"].str.contains(pat="elec|gaz_ci")]

    # Déterminer le statut global du bâtiment en fonction du statut de chaque fluide
    df_statut = (
        df.groupby(by=["code_bat_gestionnaire", "annee"])["statut_du_fluide"]
        .apply(
            func=lambda x: (
                Statuts.complet.value if (x.str.upper() == Statuts.complet.value).all() else Statuts.incomplet.value
            )
        )
        .reset_index()
    )
    df_statut = df_statut.rename(columns={"statut_du_fluide": "statut_batiment"})
    return df_statut
