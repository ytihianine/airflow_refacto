import json
import uuid
from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd
from dags.cbcm.donnee_comptable.config import (
    DEFAULT_NULL_CC_CF,
)
from modules.utils.process.text import (
    convert_str_cols_to_date,
    normalize_whitespace_columns,
)

corr_mois = {
    "janvier": "01-janv",
    "février": "02-fev",
    "mars": "03-mars",
    "avril": "04-avr",
    "mai": "05-mai",
    "juin": "06-juin",
    "juillet": "07-juil",
    "août": "08-aout",
    "septembre": "09-sept",
    "octobre": "10-oct",
    "novembre": "11-nov",
    "décembre": "12-dec",
}

corr_num_mois = {
    1: "janvier",
    2: "février",
    3: "mars",
    4: "avril",
    5: "mai",
    6: "juin",
    7: "juillet",
    8: "août",
    9: "septembre",
    10: "octobre",
    11: "novembre",
    12: "décembre",
}


corr_type_ej = {
    "ZBAU": "ZBAU : Baux",
    "ZBC": "ZBC : Bon de commande",
    "ZCTR": "ZCTR : Autres contrats",
    "ZDEC": "ZDEC : Décisions diverses",
    "ZEJ4": "ZEJ4 : EJ Flux 4",
    "ZMBC": "ZMBC : Marché à BDC",
    "ZMPC": "ZMPC : MAPA à BDC",
    "ZMPT": "ZMPT : MAPA à Tranches",
    "ZMPU": "ZMPU : MAPA Unique",
    "ZMPX": "ZMPX : MAPA mixte",
    "ZMT": "ZMT : Marché à Tranches",
    "ZMU": "ZMU : Marché Unique",
    "ZMX": "ZMX : Marché Mixte",
    "ZSUB": "ZSUB : Subventions",
}

corr_nature_sous_nature = {
    2.1: "2.1 : Locations et charges immobilières",
    2.2: "2.2 : Télécommunications, énergie, eau",
    2.3: "2.3 : Marches publics",
    3.1: "3.1 : Frais de déplacement",
    3.2: "3.2 : Bourses",
    3.3: "3.3 : Autres Dépenses barêmées",
    3.4: "3.4 : Frais de changement de résidence",
    5.0: "5.0 : Paie / Capitaux décès",
    9.9: "9.9 : Autres",
    3.0: "3.0 : Code technique",
    4.1: "4.1 : Subventions aux ménages",
    4.2: "4.2 : Subventions aux entreprises",
    4.3: "4.3 : Subventions aux collectivités",
    4.4: "4.4 : Subventions européennes",
    6.0: "6.0 : Dépenses sans ordonnancement",
}


def create_row_id(name_seed: str, row: Mapping[str, Any]) -> str:
    """
    Créer un ID unique à partir des colonnes fournies.

    Args:
        name_seed: nom à ajouter aux valeurs des colonnes
        row: ligne du dataFrame qui contient les données pour générer l'ID

    Return:
        str: format uuid5

    Notes:
        Il est nécessire de garantir que les valeurs fournies pour chaque ligne seront les même.
        (i.e réaliser le processing avant de faire appel à cette fonction)
    """
    # Namespace pour générer des UUID déterministes
    NAMESPACE = uuid.uuid5(namespace=uuid.NAMESPACE_DNS, name=name_seed)

    # Serialize dict to str
    row_string = json.dumps(obj=row, sort_keys=True, ensure_ascii=False)

    return str(uuid.uuid5(namespace=NAMESPACE, name=row_string))


# ======================================================
# Demande d'achat (DA)
# ======================================================
def process_demande_achat(df: pd.DataFrame) -> pd.DataFrame:
    """fichier INFBUD57"""
    # Remplacer les valeurs nulles
    df[["centre_financier", "centre_cout"]] = df[["centre_financier", "centre_cout"]].fillna(DEFAULT_NULL_CC_CF)

    # Nettoyer les champs textuels
    txt_cols = ["centre_financier", "centre_cout"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Convertir les colonnes temporelles
    date_cols = ["date_creation_da", "date_replication"]
    df = convert_str_cols_to_date(df=df, columns=date_cols, str_date_format="%d/%m/%Y", errors="coerce")

    # Retirer les lignes sans date de réplication
    df = df.loc[df["date_replication"].notna()]

    # Ajouter les colonnes complémentaires
    df["delai_traitement_da"] = (df["date_replication"] - df["date_creation_da"]).dt.days
    df["cf_cc"] = df["centre_financier"] + "_" + df["centre_cout"]

    # Déterminer si la DA est unique ou multiple
    df_grouped = (  # pyright: ignore[reportCallIssue]
        df.groupby(by=["id_da"], as_index=False).size().rename(columns={"size": "nb_id_da"})
    )

    # Catégoriser les données
    df_grouped["unique_multi"] = np.where(
        df_grouped["nb_id_da"] == 1,
        "Unique",
        "Multiple",
    )
    df = pd.merge(
        left=df,
        right=df_grouped[["id_da", "unique_multi", "nb_id_da"]],
        how="left",
        on="id_da",
    )

    # Catégoriser les données
    df["mois"] = df["date_creation_da"].dt.month
    df["mois_nom"] = df.loc[:, "mois"].map(corr_num_mois).fillna("Non déterminé")
    df["mois_nombre_nom"] = df.loc[:, "mois_nom"].map(corr_mois).fillna(-1)

    palier = [0, 3, 6, 9, 12, 15, np.inf]
    labels = [
        "0-3 jours",
        "4-6 jours",
        "7-9 jours",
        "10-12 jours",
        "13-15 jours",
        "16 jours et +",
    ]
    df["delai_traitement_classe"] = pd.cut(
        x=df["delai_traitement_da"],
        bins=palier,
        labels=labels,
        right=True,
        include_lowest=True,
    )

    return df


# ======================================================
# Engagement juridique (EJ)
# ======================================================
def process_engagement_juridique(df: pd.DataFrame) -> pd.DataFrame:
    """fichier Z_LIST_EJ"""
    # Remplacer les valeurs nulles
    df[["centre_financier", "centre_cout"]] = df[["centre_financier", "centre_cout"]].fillna(DEFAULT_NULL_CC_CF)

    # Nettoyer les champs textuels
    txt_cols = ["centre_financier", "centre_cout", "type_ej"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Convertir les colonnes temporelles
    date_cols = ["date_creation_ej"]
    df = convert_str_cols_to_date(df=df, columns=date_cols, str_date_format="%d.%m.%Y %H:%M:%S", errors="coerce")

    # Ajouter les colonnes complémentaires
    df["mois"] = df["date_creation_ej"].dt.month
    df["mois_nom"] = df.loc[:, "mois"].map(corr_num_mois).fillna("Non déterminé")
    df["mois_nombre_nom"] = df.loc[:, "mois_nom"].map(corr_mois).fillna(-1)
    df["cf_cc"] = df["centre_financier"] + "_" + df["centre_cout"]
    df["ej_cf_cc"] = df["id_ej"].astype(str) + "_" + df["centre_financier"] + "_" + df["centre_cout"]
    df["annee_exercice"] = df.loc[:, "date_creation_ej"].dt.year

    # Suppression des doublons
    df = df.drop_duplicates(subset=["ej_cf_cc"])

    # Regroupement
    df_grouped = df.groupby(by=["id_ej"], as_index=False)["ej_cf_cc"].count()
    df_grouped = df_grouped.rename(columns={"ej_cf_cc": "nb_poste_ej"})  # type: ignore

    # Catégoriser les données
    df_grouped["unique_multi"] = np.where(
        df_grouped["nb_poste_ej"] == 1,
        "Unique",
        "Multiple",
    )

    # Ajout des colonnes calculées
    df = pd.merge(left=df, right=df_grouped, how="left", on="id_ej")

    # Catégoriser les données
    df["type_ej_nom"] = df.loc[:, "type_ej"].map(corr_type_ej).fillna("non determine")

    return df


# ======================================================
# Demande de paiement (DP)
# ======================================================
def process_demande_paiement(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZDEP53"""
    # Remplacer les valeurs nulles
    df[["centre_financier"]] = df[["centre_financier"]].fillna(DEFAULT_NULL_CC_CF)

    # Nettoyer les champs textuels
    txt_cols = ["centre_financier", "societe", "statut_piece"]
    df = normalize_whitespace_columns(df, columns=txt_cols)
    df["montant_dp"] = (
        df["montant_dp"]
        .astype(str)  # Convertir en string pour éviter les erreurs sur NaN
        .str.replace(" ", "", regex=False)  # Enlever tous les espaces
        .str.replace(",", ".", regex=False)  # Remplacer virgule par point
    )

    df["montant_dp"] = pd.to_numeric(arg=df["montant_dp"], errors="coerce")

    # Filtrer les lignes
    df = df.loc[df["statut_piece"] == "Comptabiliser"]

    # Ajouter les colonnes complémentaires
    df["id_dp"] = df["annee_exercice"].astype(str) + df["societe"] + df["num_dp"].astype(str)

    # Convertir les colonnes temporelles
    date_cols = ["date_comptable"]
    df = convert_str_cols_to_date(df=df, columns=date_cols, str_date_format="%d.%m.%Y", errors="coerce")

    # Catégoriser les données
    df["mois"] = df["date_comptable"].dt.month
    df["mois_nom"] = df.loc[:, "mois"].map(corr_num_mois).fillna("Non déterminé")
    df["mois_nombre_nom"] = df.loc[:, "mois_nom"].map(corr_mois).fillna(-1)
    df["nat_snat_nom"] = df.loc[:, "nature_sous_nature"].map(corr_nature_sous_nature).fillna("non determine")
    df["nat_snat_groupe"] = np.where(
        df["nature_sous_nature"].isin([2.1, 2.2, 2.3]),
        "Commande publique",
        "Hors Commande publique",
    )

    return df


def process_demande_paiement_flux(df: pd.DataFrame) -> pd.DataFrame:
    """fichier INFBUD55"""
    # Nettoyer les champs textuels
    txt_cols = ["societe", "type_flux"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Filtrer les lignes
    df = df.loc[df["type_flux"] == "Flux 3"]

    # Renommer les colonnes
    df = df.rename(columns={"type_flux": "dp_flux_3"})

    # Ajouter les colonnes complémentaires
    df["id_dp"] = df["annee_exercice"].astype(str) + df["societe"] + df["num_dp_flux"].astype(str)

    return df


def process_demande_paiement_sfp(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZSFP_SUIVI"""
    # Nettoyer les champs textuels
    txt_cols = ["societe", "statut_sfp", "type_flux_sfp", "automatisation_wf_cpt"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Ajouter les colonnes complémentaires
    df["id_dp"] = df["annee_exercice"].astype(str) + df["societe"] + df["num_piece_sfp"].astype(str)

    return df


def process_demande_paiement_carte_achat(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZDEP61"""
    # Nettoyer les champs textuels
    txt_cols = [
        "societe",
        "statut_dp_carte_achat",
        "automatisation_wf_cpt",
    ]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Filtrer les lignes
    df = df.loc[df["statut_dp_carte_achat"] != "Pré-enregistrée"]

    # Remplacer les valeurs nulles
    df["niveau_carte_achat"] = np.where(
        (df["niveau_carte_achat"].isna()) & (df["automatisation_wf_cpt"].isna()),
        "N1",
        df["niveau_carte_achat"],
    )

    # Ajouter les colonnes complémentaires
    df["id_dp"] = df["annee_exercice"].astype(str) + df["societe"] + df["num_piece_dp_carte_achat"].astype(str)

    # Suppression des doublons
    df = df.drop_duplicates(subset=["id_dp"])
    return df


def process_demande_paiement_journal_pieces(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZJDP"""
    # Remplacer les valeurs nulles
    df[["centre_financier", "centre_cout"]] = df[["centre_financier", "centre_cout"]].fillna(DEFAULT_NULL_CC_CF)

    # Nettoyer les champs textuels
    txt_cols = ["societe", "centre_cout", "centre_financier", "texte_de_poste"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Ajouter les colonnes complémentaires
    df["id_dp"] = df["annee_exercice_piece_fi"].astype(str) + df["societe"] + df["num_piece_reference"].astype(str)
    df["cf_cc"] = df["centre_financier"] + "_" + df["centre_cout"]
    df["id_dp_cf_cc"] = df["id_dp"] + df["cf_cc"]

    # Compter le nombre de postes
    df_grouped = df.groupby(by=["id_dp"], as_index=False).count()
    df_grouped = df_grouped.rename(columns={"id_dp_cf_cc": "nb_poste"})

    # Déterminer s'il y plusieurs CF_CC par ID_DP
    df_unique_cf_cc = df.groupby(by="id_dp")["cf_cc"].nunique().reset_index(name="nb_unique_cf_cc")  # type: ignore
    df_unique_cf_cc["unique_multi"] = np.where(
        df_unique_cf_cc["nb_unique_cf_cc"] == 1,
        "Unique",
        "Multiple",
    )
    df_unique_cf_cc = df_unique_cf_cc.drop_duplicates(subset=["id_dp"])

    # Ajout des colonnes calculées
    df = pd.merge(left=df, right=df_grouped[["id_dp", "nb_poste"]], how="left", on="id_dp")
    df = pd.merge(
        left=df,
        right=df_unique_cf_cc[["id_dp", "nb_unique_cf_cc", "unique_multi"]],
        how="left",
        on="id_dp",
    )

    # Suppression des doublons
    df = df.drop_duplicates(subset=["annee_exercice", "num_piece_reference", "societe", "poste_reference"])
    df = df.drop(columns=["poste_reference"])

    return df


def process_demande_paiement_complet(
    df_demande_paiement: pd.DataFrame,
    df_demande_paiement_carte_achat: pd.DataFrame,
    df_demande_paiement_flux: pd.DataFrame,
    df_demande_paiement_journal_pieces: pd.DataFrame,
    df_demande_paiement_sfp: pd.DataFrame,
) -> pd.DataFrame:
    # Suppression des doublons sur df_demande_paiement_flux & df_demande_paiement
    # Pour les mêmes id_dp, on garde la première ligne (données identiques)
    df_demande_paiement_flux = df_demande_paiement_flux.drop_duplicates(subset=["id_dp"], keep="first")

    # Pour les mêmes id_dp, on garde la ligne avec le montant_dp le plus élevé
    df_demande_paiement = df_demande_paiement.sort_values(by="montant_dp", ascending=False).drop_duplicates(
        subset=["id_dp"], keep="first"
    )

    # Fusionner les datasets
    df = pd.merge(
        left=df_demande_paiement,
        right=df_demande_paiement_journal_pieces,
        how="left",
        on=["id_dp", "societe", "annee_exercice"],
        suffixes=(None, "_a"),
    )
    df = pd.merge(
        left=df,
        right=df_demande_paiement_carte_achat[
            [
                "id_dp",
                "annee_exercice",
                "niveau_carte_achat",
                "num_piece_dp_carte_achat",
                "statut_dp_carte_achat",
            ]
        ],
        how="left",
        on=["id_dp", "annee_exercice"],
        suffixes=(None, "_b"),
    )
    df = pd.merge(
        left=df,
        right=df_demande_paiement_flux,
        how="left",
        on=["id_dp", "annee_exercice"],
        suffixes=(None, "_c"),
    )
    df = pd.merge(
        left=df,
        right=df_demande_paiement_sfp[
            [
                "id_dp",
                "annee_exercice",
                "type_flux_sfp",
                "statut_sfp",
                "automatisation_wf_cpt",
                "num_ej_sfp",
                "num_piece_sfp",
            ]
        ],
        how="left",
        on=["id_dp", "annee_exercice"],
        suffixes=(None, "_d"),
    )

    # Supprimer les colonnes en doublon
    duplicate_to_drop = [
        "id_row_dp_",
        "societe_",
        "automatisation_wf_cpt_",
        "import_timestamp_",
        "import_date_",
        "snapshot_id_",
    ]
    df = df.drop(df.filter(regex="|".join(duplicate_to_drop)).columns, axis=1)  # type: ignore

    # Rename columns
    df = df.rename(
        columns={
            "centre_financier": "centre_financier_demande_paiement",
            "centre_financier_a": "centre_financier_journal_pieces",
            "montant_poste": "montant_poste_journal_pieces",
        }
    )

    # Ajout des colonnes calculées
    df["montant_poste"] = df["montant_poste_journal_pieces"].combine_first(df["montant_dp"]).fillna(DEFAULT_NULL_CC_CF)
    df["centre_financier"] = (
        df["centre_financier_journal_pieces"]
        .combine_first(df["centre_financier_demande_paiement"])
        .fillna(DEFAULT_NULL_CC_CF)
    )
    df["centre_cout"] = df["centre_cout"].fillna(DEFAULT_NULL_CC_CF)

    conditions = [
        (df["dp_flux_3"] == "Flux 3") & (df["automatisation_wf_cpt"] == "X"),
        (df["dp_flux_3"] == "Flux 3"),
    ]
    choices = ["DP automatisées", "DP non automatisées"]
    df["flux_3_automatisation_compta"] = np.select(condlist=conditions, choicelist=choices, default="Indéterminé")

    return df


# ======================================================
# Délai global de paiement (DGP)
# ======================================================
def process_delai_global_paiement(df: pd.DataFrame) -> pd.DataFrame:
    """fichier INFDEP56"""

    # Nettoyer les champs textuels
    txt_cols = [
        "type_piece",
        "nature_sous_nature",
        "centre_cout",
        "centre_financier",
        "service_executant",
        "societe",
    ]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Remplacer les valeurs nulles
    df["centre_cout"] = df["centre_cout"].replace({"#": DEFAULT_NULL_CC_CF})
    df[["centre_financier", "centre_cout"]] = df[["centre_financier", "centre_cout"]].fillna(DEFAULT_NULL_CC_CF)

    # Filtrer les lignes
    df = df.loc[df["societe"].isin(["ADCE", "CSND"])]

    # Ajouter un ID unique à chaque ligne
    df["id_dgp"] = [
        create_row_id(name_seed="delai_global_paiement.INFDEP56", row=row) for row in df.to_dict("records")  # type: ignore
    ]

    # Ajouter les colonnes complémentaires
    df["mois_nom"] = df.loc[:, "mois"].map(corr_num_mois).fillna("Non déterminé")
    df["mois_nombre_nom"] = df.loc[:, "mois_nom"].map(corr_mois).fillna(-1)
    df["cf_cc"] = df["centre_financier"] + "_" + df["centre_cout"]

    # Arrondir les valeurs
    df["delai_global_paiement"] = df["delai_global_paiement"].round(2)

    return df
