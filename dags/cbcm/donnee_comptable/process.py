import pandas as pd
import numpy as np

from utils.control.text import convert_str_cols_to_date, normalize_whitespace_columns

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
    "novembre": "11-sept",
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
    "2.1": "2.1 : Locations et charges immobilières",
    "2.2": "2.2 : Télécommunications, énergie, eau",
    "2.3": "2.3 : Marches publics",
    "3.1": "3.1 : Frais de déplacement",
    "3.2": "3.2 : Bourses",
    "3.3": "3.3 : Autres Dépenses barêmées",
    "3.4": "3.4 : Frais de changement de résidence",
    "5.0": "5.0 : Paie / Capitaux décès",
    "9.9": "9.9 : Autres",
    "3.0": "3.0 : Code technique",
    "4.1": "4.1 : Subventions aux ménages",
    "4.2": "4.2 : Subventions aux entreprises",
    "4.3": "4.3 : Subventions aux collectivités",
    "4.4": "4.4 : Subventions européennes",
    "6.0": "6.0 : Dépenses sans ordonnancement",
}


# ======================================================
# Demande d'achat (DA)
# ======================================================
def process_demande_achat(df: pd.DataFrame) -> pd.DataFrame:
    """fichier INFBUD57"""
    # Remplacer les valeurs nulles
    df[["centre_financer", "centre_cout"]] = df[
        ["centre_financer", "centre_cout"]
    ].fillna("Ind")

    # Nettoyer les champs textuels
    txt_cols = ["centre_financer", "centre_cout"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Retirer les lignes sans date de réplication
    df = df.loc[df["date_replication"].notna()]

    # Convertir les colonnes temporelles
    date_cols = ["date_creation_da", "date_replication"]
    df = convert_str_cols_to_date(
        df=df, columns=date_cols, str_date_format="%d/%m/%Y", errors="coerce"
    )

    # Ajouter les colonnes complémentaires
    df["delai_traitement_da"] = (
        df["date_replication"] - df["date_creation_da"]
    ).dt.days
    df["cf_cc"] = df["centre_financer"] + "_" + df["centre_cout"]

    # Catégoriser les données
    df["mois"] = df["date_creation_da"].dt.month
    df["mois_nom"] = df.loc[:, "mois"].map(corr_num_mois).fillna("Non déterminé")
    df["mois_nombre"] = df.loc[:, "mois_nom"].map(corr_mois).fillna(-1)

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


def process_demande_achat_journal_pieces(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZJDP"""
    # Nettoyer les champs textuels
    txt_cols = ["societe", "centre_cout", "centre_financier"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Ajouter les colonnes complémentaires
    df["id_dp"] = (
        df["annee_exercice_piece_fi"].astype(str)
        + df["societe"]
        + df["num_piece_reference"].astype(str)
    )
    df["cf_cc"] = df["centre_financier"] + "_" + df["centre_cout"]
    df["id_dp_cf_cc"] = df["id_dp"] + df["cf_cc"]

    # Suppression des doublons
    df = df.drop_duplicates(subset=["id_dp_cf_cc"])

    # Regroupement
    df_grouped = df.groupby(by=["id_dp"], as_index=False)["id_dp_cf_cc"].count()
    df_grouped = df_grouped.rename(columns={"id_dp_cf_cc": "nb_poste"})

    # Catégoriser les données
    df_grouped["unique_multi"] = np.where(
        df_grouped["nb_poste"] == 1,
        "Unique",
        "Multiple",
    )

    # Ajout des colonnes calculées
    df = pd.merge(left=df, right=df_grouped, how="left", on="id_dp")

    return df


# ======================================================
# Engagement juridique (EJ)
# ======================================================
def process_engagement_juridique(df: pd.DataFrame) -> pd.DataFrame:
    """fichier Z_LIST_EJ"""
    # Remplacer les valeurs nulles
    df[["centre_financer", "centre_cout"]] = df[
        ["centre_financer", "centre_cout"]
    ].fillna("Ind")

    # Nettoyer les champs textuels
    txt_cols = ["centre_financer", "centre_cout", "type_ej"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Convertir les colonnes temporelles
    date_cols = ["date_creation_ej"]
    df = convert_str_cols_to_date(
        df=df, columns=date_cols, str_date_format="%d.%m.%Y %H:%M:%S", errors="coerce"
    )

    # Ajouter les colonnes complémentaires
    df["cf_cc"] = df["centre_financer"] + "_" + df["centre_cout"]
    df["ej_cf_cc"] = (
        df["id_ej"].astype(str) + "_" + df["centre_financer"] + "_" + df["centre_cout"]
    )
    df["annee_exercice"] = df.loc[:, "date_creation_ej"].dt.year
    df["mois_ej"] = df.loc[:, "date_creation_ej"].dt.month

    # Suppression des doublons
    df = df.drop_duplicates(subset=["ej_cf_cc"])

    # Regroupement
    df_grouped = df.groupby(by=["id_ej"], as_index=False)["ej_cf_cc"].count()
    df_grouped = df_grouped.rename(columns={"ej_cf_cc": "nb_poste_ej"})

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
    df[["centre_financer"]] = df[["centre_financer"]].fillna("Ind")

    # Nettoyer les champs textuels
    txt_cols = ["centre_financer", "societe", "statut_piece"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Filtrer les lignes
    df = df.loc[df["statut_piece"] == "Comptabiliser"]

    # Ajouter les colonnes complémentaires
    df["id_dp"] = (
        df["annee_exercice"].astype(str) + df["societe"] + df["num_dp"].astype(str)
    )

    # Convertir les colonnes temporelles
    date_cols = ["date_comptable"]
    df = convert_str_cols_to_date(
        df=df, columns=date_cols, str_date_format="%d.%m.%Y", errors="coerce"
    )

    # Catégoriser les données
    df["mois"] = df["date_comptable"].dt.month
    df["mois_nom"] = df.loc[:, "mois"].map(corr_num_mois).fillna("Non déterminé")
    df["nat_snat_nom"] = (
        df.loc[:, "nature_sous_nature"]
        .map(corr_nature_sous_nature)
        .fillna("non determine")
    )
    df["nat_snat_groupe"] = np.where(
        df["nature_sous_nature"].isin(["2.1", "2.2", "2.3"]),
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

    # Ajouter les colonnes complémentaires
    df["id_dp"] = (
        df["annee_exercice"].astype(str) + df["societe"] + df["num_dp_flux"].astype(str)
    )

    return df


def process_demande_paiement_sfp(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZSFP_SUIVI"""
    # Nettoyer les champs textuels
    txt_cols = ["societe", "statut_sfp", "type_flux_sfp", "automatisation_wf_cpt"]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Ajouter les colonnes complémentaires
    df["id_dp"] = (
        df["annee_exercice"].astype(str)
        + df["societe"]
        + df["num_piece_sfp"].astype(str)
    )

    return df


def process_demande_paiement_carte_achat(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZDEP61"""
    # Nettoyer les champs textuels
    txt_cols = [
        "societe",
        "statut_dp_carte_achat",
        "type_flux",
        "automatisation_wf_cpt",
    ]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Filtrer les lignes
    df = df.loc[df["statut_dp_carte_achat"] == "Pré-enregistrée"]

    # Remplacer les valeurs nulles
    df["niveau_carte_achat"] = np.where(
        (df["niveau_carte_achat"].isna()) & (df["automatisation_wf_cpt"].isna()),
        "N1",
        df["niveau_carte_achat"],
    )

    # Ajouter les colonnes complémentaires
    df["id_dp"] = (
        df["annee_exercice"].astype(str)
        + df["societe"]
        + df["num_piece_dp_carte_achat"].astype(str)
    )

    # Suppression des doublons
    df = df.drop_duplicates(subset=["id_dp"])
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
        "centre_financer",
        "service_executant",
        "societe",
    ]
    df = normalize_whitespace_columns(df, columns=txt_cols)

    # Remplacer les valeurs nulles
    df["centre_cout"] = df["centre_cout"].replace({"#": "Ind"})
    df[["centre_financer", "centre_cout"]] = df[
        ["centre_financer", "centre_cout"]
    ].fillna("Ind")

    # Filtrer les lignes
    df = df.loc[df["societe"].isin(["ADCE", "CSND"])]

    # Ajouter les colonnes complémentaires
    df["cf_cc"] = df["centre_financer"] + "_" + df["centre_cout"]
    df["mois_nom"] = df.loc[:, "periode_comptable"].map(corr_num_mois).fillna("inconnu")

    # Arrondir les valeurs
    df["delai_global_paiement"] = df["delai_global_paiement"].round(2)

    return df
