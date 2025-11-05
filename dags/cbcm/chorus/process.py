import pandas as pd


# ======================================================
# Demande d'achat (DA)
# ======================================================
def process_demande_achat(df: pd.DataFrame) -> pd.DataFrame:
    """fichier INFBUD57"""
    # Retirer les lignes sans date de réplication
    df = df[df["date_replication"].notna()]

    # Convertir les colonnes temporelles
    date_cols = ["date_creation_da", "date_replication"]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(df[date_col], format="%d/%m/%Y", errors="coerce")

    # Ajouter les colonnes complémentaires
    df["delai_traitement_da"] = (
        df["date_replication"] - df["date_creation_da"]
    ).dt.days
    df["cf_cc"] = df["centre_financer"] + "_" + df["centre_cout"]
    return df


# ======================================================
# Engagement juridique (EJ)
# ======================================================
def process_engagement_juridique(df: pd.DataFrame) -> pd.DataFrame:
    """fichier Z_LIST_EJ"""
    # Remplacer les valeurs nulles
    # Ajouter les colonnes complémentaires

    # (stand-by) Détermine si multiple ou unique
    return df


# ======================================================
# Demande de paiement (DP)
# ======================================================
def process_demande_paiement(df: pd.DataFrame) -> pd.DataFrame:
    """fichier ZDEP53"""
    # Filtrer les lignes
    # Ajouter les colonnes complémentaires

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
