import pandas as pd
import numpy as np

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


def convert_str_cols_to_date(
    df: pd.DataFrame, cols: list[str], str_date_format: str, errors: str
) -> pd.DataFrame:
    if isinstance(cols, str):
        cols = [cols]

    for date_col in cols:
        df[date_col] = pd.to_datetime(
            df[date_col], format=str_date_format, errors=errors
        )

    return df


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

    # Retirer les lignes sans date de réplication
    df = df.loc[df["date_replication"].notna()]

    # Convertir les colonnes temporelles
    date_cols = ["date_creation_da", "date_replication"]
    df = convert_str_cols_to_date(
        df=df, cols=date_cols, str_date_format="%d/%m/%Y", errors="coerce"
    )

    # Ajouter les colonnes complémentaires
    df["delai_traitement_da"] = (
        df["date_replication"] - df["date_creation_da"]
    ).dt.days
    df["cf_cc"] = df["centre_financer"] + "_" + df["centre_cout"]

    # Catégoriser les données
    df["mois"] = df["date_creation_da"].df.month
    df["mois_nom"] = (
        df["date_creation_da"].df.month_name(locale="fr_FR.utf-8").str.lower()
    )
    df["mois_nombre"] = df["moi_nom"].map(corr_mois).fillna(-1)

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
        df["delai_de_traitement_da"],
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
    df[["centre_financer", "centre_cout"]] = df[
        ["centre_financer", "centre_cout"]
    ].fillna("Ind")

    # Nettoyer les champs textuels

    # Ajouter les colonnes complémentaires
    df["cf_cc"] = df["centre_financer"] + "_" + df["centre_cout"]

    # (stand-by) Détermine si multiple ou unique

    # Catégoriser les données
    df["type_ej_nom"] = df["type_ej"].map(corr_type_ej).fillna("non determine")

    return df


# ======================================================
# Demande de paiement (DP)
# ======================================================
def process_demande_paiement(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZDEP53"""
    # Remplacer les valeurs nulles
    df[["centre_financer"]] = df[["centre_financer"]].fillna("Ind")

    # Nettoyer les champs textuels

    # Filtrer les lignes
    df = df.loc[df["date_replication"] == "Comptabiliser"]

    # Ajouter les colonnes complémentaires
    df["id_dp"] = df["exercice"] + df["societe"] + df["num_dp"]

    # Convertir les colonnes temporelles
    date_cols = ["date_comptable"]
    df = convert_str_cols_to_date(
        df=df, cols=date_cols, str_date_format="%d/%m/%Y", errors="coerce"
    )

    # Catégoriser les données
    df["mois"] = df["date_comptable"].df.month
    df["nat_snat_nom"] = (
        df["nature_sous_nature"].map(corr_nature_sous_nature).fillna("non determine")
    )
    df["nat_snat_groupe"] = np.where(
        df["nature_sous_nature"].isin(
            ["2.1", "2.2", "2.3"], "Commande publique", "Hors Commande publique"
        )
    )

    return df


def process_demande_paiement_flux(df: pd.DataFrame) -> pd.DataFrame:
    """fichier INFBUD55"""
    # Filtrer les lignes
    # Ajouter les colonnes complémentaires

    return df


def process_demande_paiement_sfp(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZSFP_SUIVI"""
    # Ajouter les colonnes complémentaires
    return df


def process_demande_paiement_carte_achat(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZDEP61"""
    # Filtrer les lignes
    # Ajouter les colonnes complémentaires
    # Suppression des doublons
    return df


def process_demande_achat_journal_pieces(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZJDP"""
    # Ajouter les colonnes complémentaires
    # Suppression des doublons
    # En attente de confirmation
    return df


# ======================================================
# Délai global de paiement (DGP)
# ======================================================
def process_delai_global_paiement(df: pd.DataFrame) -> pd.DataFrame:
    """fichier INFDEP56"""
    # Filtrer les lignes
    # Ajouter les colonnes complémentaires
    # Arrondir les valeurs
    return df
