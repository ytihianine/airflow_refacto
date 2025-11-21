import pandas as pd
import numpy as np

from utils.control.text import normalize_whitespace_columns


# ======================================================
# Référentiels
# ======================================================
def process_ref_prog(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des données textuelles
    txt_cols = ["prog"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    return df


def process_ref_bop(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des données textuelles
    txt_cols = ["bop"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Gestion des références vides
    df["prog"] = df.loc[:, "prog"].replace({0: np.nan})
    return df


def process_ref_uo(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des données textuelles
    txt_cols = ["uo"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Gestion des références vides
    rpl_cols = ["prog", "bop"]
    df[rpl_cols] = df.loc[:, rpl_cols].replace({0: np.nan})
    return df


def process_ref_cc(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des données textuelles
    txt_cols = ["cc"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Gestion des références vides
    rpl_cols = ["prog", "bop", "uo"]
    df[rpl_cols] = df.loc[:, rpl_cols].replace({0: np.nan})
    return df


def process_ref_sp_pilotage(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des données textuelles
    txt_cols = ["service_prescripteur"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    return df


def process_ref_sp_choisi(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des données textuelles
    txt_cols = ["service_prescripteur"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    return df


def process_ref_sdep(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des données textuelles
    txt_cols = ["service_depense"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    return df


# ======================================================
# Services prescripteurs
# ======================================================
def process_service_prescripteur(df: pd.DataFrame) -> pd.DataFrame:
    # Retirer les lignes sans valeurs
    df = df.dropna(subset=["centre_financier", "centre_de_cout"], how="any")

    # Nettoyage des données textuelles
    txt_cols = ["centre_financier", "centre_de_cout", "couple_cf_cc", "observation"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Gestion des références vides
    rpl_cols = [
        "service_prescripteur_pilotage_",
        "service_depense",
        "service_prescripteur_choisi_selon_cf_cc",
        "designation_prog",
        "designation_bop",
        "designation_uo",
        "designation_cc",
    ]
    df[rpl_cols] = df.loc[:, rpl_cols].replace({0: np.nan})

    return df
