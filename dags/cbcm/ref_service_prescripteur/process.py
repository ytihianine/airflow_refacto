import pandas as pd

from utils.control.text import normalize_whitespace_columns
from utils.control.number import convert_to_numeric


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
    num_cols = ["prog"]
    df = convert_to_numeric(df=df, columns=num_cols, errors="coerce")

    return df


def process_ref_uo(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des données textuelles
    txt_cols = ["uo"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Gestion des références vides
    num_cols = ["prog", "bop"]
    df = convert_to_numeric(df=df, columns=num_cols, errors="coerce")

    return df


def process_ref_cc(df: pd.DataFrame) -> pd.DataFrame:
    # Nettoyage des données textuelles
    txt_cols = ["cc"]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    # Gestion des références vides
    num_cols = ["prog", "bop", "uo"]
    df = convert_to_numeric(df=df, columns=num_cols, errors="coerce")
    df[num_cols] = df[num_cols].replace({0: pd.NA})

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
    num_cols = [
        "service_prescripteur_pilotage_",
        "service_depense",
        "service_prescripteur_choisi_selon_cf_cc",
        "designation_prog",
        "designation_bop",
        "designation_uo",
        "designation_cc",
    ]
    df = convert_to_numeric(df=df, columns=num_cols, errors="coerce")

    return df
