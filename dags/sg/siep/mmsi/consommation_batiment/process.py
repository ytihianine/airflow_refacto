import functools
from enum import Enum
from typing import Union
import pandas as pd
import numpy as np


class Statuts(str, Enum):
    debut_exp = "DEBUT EXP"
    fin_exp = "FIN EXP"
    incomplet = "INCOMPLET"
    complet = "COMPLET"


class TypeEnergie(str, Enum):
    electricite = "électricité"
    gaz = "gaz"
    fioul = "fioul"
    reseau_chaud = "réseau de chaud"
    reseau_froid = "réseau de froid"


def process_source_conso_mens(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace("NC", None)
    return df


def process_source_bien_info_comp(df: pd.DataFrame, cols_mapping: dict) -> pd.DataFrame:
    df = df.rename(columns=cols_mapping, errors="raise")
    df = df.drop(columns=list(set(df.columns) - set(cols_mapping.values())))
    df = df.fillna(np.nan).replace([np.nan], [None])

    return df


def cleaning_dataFrame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    df.dropna(subset=["date_conso"], inplace=True)
    df["date_conso"] = pd.to_datetime(df["date_conso"])
    df["mois_conso"] = df["date_conso"].dt.month
    df["annee_conso"] = df["date_conso"].dt.year
    df = df.fillna(np.nan).replace([np.nan], [None])
    return df


def rename_columns(
    df: pd.DataFrame, colonnes_correspondances: dict[str, str]
) -> pd.DataFrame:
    columns_rename = {}
    for correspondance in colonnes_correspondances:
        columns_rename[correspondance[0]] = correspondance[1]
    df = df.rename(columns=columns_rename)
    df = df.fillna(np.nan).replace([np.nan], [None])
    return df


# ======================================================
# CONSOMMATION MENSUELLE
# ======================================================
def process_conso_mensuelles(df: pd.DataFrame) -> pd.DataFrame:
    df = cleaning_dataFrame(df)

    # Conversion des fluides PCS en PCI
    df = df.dropna(subset=["date_conso"])
    df = convertir_pcs_en_pci(df=df)

    # Etape 1
    df = add_dju_moyen(df_conso_mens=df)

    # Etape 2
    df = corriger_consommation(df_conso_mens=df)

    return df


def process_unpivot_conso_mens_brute(df: pd.DataFrame) -> pd.DataFrame:
    df = process_unpivot_conso_mens(df=df, use_conso_corrigee=False)
    return df


def process_unpivot_conso_mens_corrigee(df: pd.DataFrame) -> pd.DataFrame:
    df = process_unpivot_conso_mens(df=df, use_conso_corrigee=True)
    return df


def process_unpivot_conso_mens(
    df: pd.DataFrame, use_conso_corrigee: bool
) -> pd.DataFrame:
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
        df,
        id_vars=col_commune,
        value_vars=col_conso,
        var_name="type_energie",
        value_name=value_name,
    )

    return df


def calculer_dju_moyen(df: pd.DataFrame) -> pd.DataFrame:
    df = df.groupby(["code_bat_gestionnaire", "mois_conso"], as_index=False).mean()
    df = df.rename(columns={"degres_jours_de_chauffage": "dju_moyen"})
    df = df.fillna(np.nan).replace([np.nan], [None])
    return df


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
    df.sort_values(
        by=["code_bat_gestionnaire", "annee_conso", "mois_conso"], inplace=True
    )
    return df


def convertir_pcs_en_pci(df: pd.DataFrame) -> pd.DataFrame:
    df["conso_gaz_pci"] = 0.9 * df["conso_gaz_pcs"]
    # Les données sources sont en pc pour le fioul désormais
    # df["conso_fioul_pci"] = 0.9 * df["conso_fioul_pcs"]
    return df


def calculer_consommation_corrigee(df: pd.DataFrame) -> pd.DataFrame:
    df["conso_elec_corr_dju_mmsi"] = df["conso_elec"] * df["ratio_electricite"]

    df["conso_gaz_pci_corr_dju_mmsi"] = df["conso_gaz_pci"] * df["ratio_autres_fluides"]
    df["conso_reseau_chaleur_corr_dju_mmsi"] = (
        df["conso_reseau_chaleur"] * df["ratio_autres_fluides"]
    )
    df["conso_reseau_froid_corr_dju_mmsi"] = (
        df["conso_reseau_froid"] * df["ratio_autres_fluides"]
    )
    df["conso_fioul_pci_corr_dju_mmsi"] = (
        df["conso_fioul_pci"] * df["ratio_autres_fluides"]
    )
    df["conso_granule_bois_corr_dju_mmsi"] = (
        df["conso_granule_bois"] * df["ratio_autres_fluides"]
    )
    return df


def determiner_ratio(
    type_energie: str, mois_conso: float, dju_ref: float, dju_moy: float
) -> float:
    if mois_conso is None:
        return None

    if dju_ref is None:
        return None

    if dju_moy is None:
        return None

    if type_energie == TypeEnergie.electricite.value:
        ratio = 1
        return ratio

    mois_conso = int(mois_conso)
    mois_ete = [6, 7, 8, 9]  # "Juin", "Juillet", "Août", "Septembre"

    if mois_conso not in mois_ete:
        try:
            ratio = 0.7 * (dju_moy / dju_ref) + 0.3

            if ratio > 1.4:
                ratio = 1.4

            if ratio < 0.6:
                ratio = 0.6
        except ZeroDivisionError:
            ratio = 0

        return ratio
    ratio = 1
    return ratio


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
    ]

    df = df.drop(columns=cols_to_drop)
    cols_fluides = list(set(df.columns) - set(cols_id))

    # On calcule la conso totale pour chaque année
    df = df.groupby(cols_id)[cols_fluides].sum(min_count=1).reset_index()
    df = df.fillna(np.nan).replace([np.nan], [None])

    return df


def process_conso_statut_par_fluide(df: pd.DataFrame) -> pd.DataFrame:
    colonnes_fluides = [
        "conso_elec",
        "conso_gaz_pcs",
        "conso_reseau_chaleur",
        "conso_reseau_froid",
    ]
    annees_from_df = df["annee"].unique().tolist()
    annees_avant_2019, annees_depuis_2019 = split_years(annees=annees_from_df)

    print("Etape 1: transformation de la structure du DataFrame")
    df = transfo_pour_statut_fluide(colonnes_fluides=colonnes_fluides, df=df)
    print("LOGS columns df: ", df.columns)

    print("Etape 2: Déterminer le statut pour chaque fluide")
    df["conso_fluide_depuis_2019"] = df[annees_depuis_2019].apply(
        lambda row: [int(conso) if conso is not None else None for conso in row], axis=1
    )
    df["statut_du_fluide"] = df["conso_fluide_depuis_2019"].apply(
        determiner_statut_fluide_specifique
    )
    print("LOGS columns df: ", df.columns)

    # Dropping years columns
    df = df.drop(columns=annees_from_df + ["conso_fluide_depuis_2019"])

    return df


def process_conso_avant_2019(df: pd.DataFrame) -> pd.DataFrame:
    annees_from_df = df["annee"].unique().tolist()
    annees_avant_2019, annees_depuis_2019 = split_years(annees=annees_from_df)

    df = check_conso_before_2019(df=df, annees_avant_2019=annees_avant_2019)
    return df


def process_conso_statut_fluide_global(df: pd.DataFrame) -> pd.DataFrame:
    print("Etape 1: transformation de la structure du DataFrame")
    df = transfo_pour_statut_global(df=df)
    print("LOGS columns df: ", df.columns)
    print(df.head(), "\n")

    print("Etape 2: Déterminer le statut global des fluides")
    df["statut_fluide_global"] = list(
        map(
            determiner_statut_fluide_global,
            df["statut_elec"],
            df["statut_gaz"],
            df["statut_reseau_chaleur"],
            df["statut_reseau_froid"],
        )
    )
    print("LOGS columns df: ", df.columns)

    return df


def process_conso_statut_batiment(
    df_conso_statut_fluide_global: pd.DataFrame, df_conso_avant_2019: pd.DataFrame
) -> pd.DataFrame:
    print("Etape 1: join entre les datasets obtenus lors des 2 précédentes tâches")
    df = pd.merge(
        left=df_conso_statut_fluide_global,
        right=df_conso_avant_2019,
        how="inner",
        on="code_bat_gestionnaire",
    )
    print("LOGS columns df: ", df.columns)
    print(df.head())

    print("Etape 2: Déterminer le statut du bâtiment")
    df["statut_batiment"] = statut_batiment(df=df)
    print("LOGS columns df: ", df.columns)

    cols_to_keep = [
        "code_bat_gestionnaire",
        "statut_conso_avant_2019",
        "statut_fluide_global",
        "statut_batiment",
    ]
    df = df.drop(columns=list(set(df.columns) - set(cols_to_keep)))
    return df


def transfo_pour_statut_fluide(
    colonnes_fluides: list[str], df: pd.DataFrame
) -> pd.DataFrame:
    # Unpivot du dataframe
    df = pd.melt(
        df,
        id_vars=["code_bat_gestionnaire", "annee"],
        value_vars=colonnes_fluides,
        var_name="type_fluide",
        value_name="conso",
    ).reset_index()

    # Remplacement des valeurs par le type de fluide
    replace_values = {
        "conso_elec": "électricité",
        "conso_gaz_pcs": "gaz",
        "conso_reseau_chaleur": "réseau de chaleur",
        "conso_reseau_froid": "réseau de froid",
    }
    df["type_fluide"] = df["type_fluide"].replace(to_replace=replace_values)

    #
    df = df.pivot(
        index=["code_bat_gestionnaire", "type_fluide"], columns="annee", values="conso"
    ).reset_index()
    df = df.fillna(np.nan).replace([np.nan], [None])

    return df


def transfo_pour_statut_global(df: pd.DataFrame) -> pd.DataFrame:
    df = df.pivot(
        index=["code_bat_gestionnaire"],
        columns="type_fluide",
        values="statut_du_fluide",
    ).reset_index()
    df = df.rename(
        columns={
            "électricité": "statut_elec",
            "gaz": "statut_gaz",
            "réseau de chaleur": "statut_reseau_chaleur",
            "réseau de froid": "statut_reseau_froid",
        }
    )
    df = df.fillna(np.nan).replace([np.nan], [None])

    return df


def determiner_statut_fluide_specifique(lst: list[Union[int, None]]) -> str:
    # print(f"Liste base: {lst}")

    if None not in lst:
        return Statuts.complet.value

    if all(element is None for element in lst):
        return None

    n = len(lst)

    # On cherche l'index du 1er élément non-nul de la liste
    start = 0
    while start < n and lst[start] is None:
        start += 1

    # On cherche l'index du dernier élément non-nul de la liste
    end = n - 1
    while end >= 0 and lst[end] is None:
        end -= 1

    # L'objectif est de déterminé s'il y a au moins une valeur None entre
    # le premier élément non nul et le dernier ou s'il y a que des valeurs non-nulles
    lst_entre = lst[start : end + 1]
    any_none_between_start_and_end = any(element is None for element in lst_entre)
    only_values_between_start_and_end = all(
        element is not None for element in lst_entre
    )
    # print(f"Any None: {any_none_between_start_and_end}")
    # print(f"Only values: {only_values_between_start_and_end}")

    if any_none_between_start_and_end:
        return Statuts.incomplet.value

    if start != 0 and end != n - 1 and only_values_between_start_and_end:
        return Statuts.incomplet.value

    if start != 0 and only_values_between_start_and_end:
        return Statuts.debut_exp.value

    if end != n - 1 and only_values_between_start_and_end:
        return Statuts.fin_exp.value

    return Statuts.incomplet.value


def determiner_statut_fluide_global(
    statut_elec: str,
    statut_gaz: str,
    statut_reseau_chaud: str,
    statut_reseau_froid: str,
) -> str:
    lst_statut_par_fluide = [statut_gaz, statut_reseau_chaud, statut_reseau_froid]

    if statut_elec == Statuts.complet.value and all(
        element is None or element == Statuts.complet.value
        for element in lst_statut_par_fluide
    ):
        return Statuts.complet.value

    if statut_elec == Statuts.debut_exp.value and all(
        element is None or element == Statuts.debut_exp.value
        for element in lst_statut_par_fluide
    ):
        return Statuts.debut_exp.value

    if statut_elec == Statuts.fin_exp.value and all(
        element is None or element == Statuts.fin_exp.value
        for element in lst_statut_par_fluide
    ):
        return Statuts.fin_exp.value

    return Statuts.incomplet.value


# conso_avant_2019: list[str | None]
def any_conso_avant_2019(conso_avant_2019) -> bool:
    if any(element is not None for element in conso_avant_2019):
        return True

    return False


def split_years(annees: list[Union[int, None]]) -> [list[int], list[int]]:
    annees = [annee for annee in annees if annee is not None]
    annees = list(np.sort(annees))
    for i in range(len(annees)):
        if int(annees[i]) == 2019:
            index_2019 = i

    annees_depuis_2019 = annees[index_2019:-1]
    annees_avant_2019 = annees[:index_2019]

    return annees_avant_2019, annees_depuis_2019


def check_conso_before_2019(
    df: pd.DataFrame, annees_avant_2019: list[int]
) -> pd.DataFrame:
    # List of raw consumption columns to check (add others if needed)
    conso_cols = [
        "conso_elec",
        "conso_gaz_pcs",
        "conso_reseau_chaleur",
        "conso_reseau_froid",
    ]

    # Filter only years < 2019
    df_before_2019 = df[df["annee"] < 2019].copy()

    # Replace NaN with 0 (so they don't count as consumption)
    df_before_2019[conso_cols] = df_before_2019[conso_cols].fillna(0)

    # Check if there is any non-zero consumption in any of the columns
    df_before_2019["any_conso"] = df_before_2019[conso_cols].sum(axis=1) > 0

    # Aggregate per building: True if any record had consumption
    result = (
        df_before_2019.groupby("code_bat_gestionnaire")["any_conso"]
        .any()
        .reset_index()
        .rename(columns={"any_conso": "statut_conso_avant_2019"})
    )

    return result


# statut_tous_les_fluides: str | None
def determiner_statut_batiment(
    conso_avant_2019: bool,
    statut_tous_les_fluides,
) -> str:

    if conso_avant_2019 and statut_tous_les_fluides == Statuts.debut_exp.value:
        return Statuts.incomplet.value

    return statut_tous_les_fluides


def statut_batiment(df: pd.DataFrame) -> list[str]:
    if "statut_conso_avant_2019" not in df.columns:
        batiment_statut = list(
            map(
                functools.partial(determiner_statut_batiment, False),
                df["statut_fluide_global"],
            )
        )
    else:
        batiment_statut = list(
            map(
                functools.partial(determiner_statut_batiment),
                df["statut_conso_avant_2019"],
                df["statut_fluide_global"],
            )
        )
    return batiment_statut


def get_perimetre_batiments(
    df_info_biens: pd.DataFrame,
    df_referentiel_gestionnaire: pd.DataFrame,
    df_perimetre_batiment: pd.DataFrame,
) -> pd.DataFrame:

    df_info_biens["batiment_soumis_deet"] = df_info_biens[
        "batiment_soumis_deet"
    ].replace({0: False, 1: True})
    df_info_biens = df_info_biens.fillna(np.nan).replace([np.nan], [None])

    # On check s'il y a de nouveaux bâtiments
    batiments_manquants = np.setdiff1d(
        df_info_biens["code_bat_gestionnaire"],
        df_perimetre_batiment["code_bat_gestionnaire"],
    )

    if len(batiments_manquants) > 0:
        df_batiments_manquants = df_info_biens[
            df_info_biens["code_bat_gestionnaire"].isin(batiments_manquants)
        ][["code_bat_gestionnaire", "id_gestionnaire"]]

        # Ajout du périmètre à ces nouveaux bâtiments
        df_batiments_manquants = pd.merge(
            df_batiments_manquants,
            df_referentiel_gestionnaire[["id_gestionnaire", "perimetre"]],
            how="left",
            left_on=["id_gestionnaire"],
            right_on=["id_gestionnaire"],
        )
        # Ajout des nouveaux batiments
        df_perimetre_batiment = pd.concat(
            [
                df_perimetre_batiment,
                df_batiments_manquants[["code_bat_gestionnaire", "perimetre"]],
            ]
        )

    # Ajout des perimetres aux batiments
    df_info_biens = pd.merge(
        df_info_biens,
        df_perimetre_batiment,
        how="left",
        left_on=["code_bat_gestionnaire"],
        right_on=["code_bat_gestionnaire"],
    )

    return df_info_biens


def rename_conso_mens(
    df_conso_mensuelles: pd.DataFrame, colonnes_conso_mens: list[str]
) -> pd.DataFrame:

    df_conso_mensuelles = rename_columns(
        df=df_conso_mensuelles, colonnes_correspondances=colonnes_conso_mens
    )
    print(f"\n Colonnes après rename: {df_conso_mensuelles.columns}")

    renamed_colonnes = df_conso_mensuelles.columns
    new_colonnes = [new_colonne[1] for new_colonne in colonnes_conso_mens]
    colonne_to_drop = [
        old_colonne
        for old_colonne in renamed_colonnes
        if old_colonne not in new_colonnes
    ]

    print(f"\n Nouvelles colonnes: {new_colonnes}")
    print(
        f"\n Anciennes colonnes encore présentes dans le dataframe: {colonne_to_drop}"
    )
    df_conso_mensuelles = df_conso_mensuelles.drop(columns=colonne_to_drop)
    print(
        f"\n Colonnes du dataframe après drop des colonnes inutiles: {df_conso_mensuelles.columns}"
    )

    return df_conso_mensuelles


def add_dju_moyen(df_conso_mens: pd.DataFrame) -> pd.DataFrame:

    df_dju_moyen = calculer_dju_moyen(
        df=df_conso_mens[
            ["code_bat_gestionnaire", "mois_conso", "degres_jours_de_chauffage"]
        ]
    )

    # Besoin de merge les DF pour récupérer la colonne DJU moyen
    df_conso_mens = pd.merge(
        df_conso_mens,
        df_dju_moyen,
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
