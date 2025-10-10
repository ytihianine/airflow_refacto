import pandas as pd
import numpy as np


def merge_old_df_to_new_df(
    new_df: pd.DataFrame,
    old_df: pd.DataFrame,
    id_keys: list[str],
    old_cols_to_keep: list[str] = None,
) -> pd.DataFrame:
    """
    Certaines données sont disponibles via d'autres chaines de traitement.
    Il faut donc les conserver.
    """
    if old_cols_to_keep is None:
        old_cols_to_keep = list(old_df.columns)

    df = pd.merge(
        left=new_df, right=old_df.loc[:, old_cols_to_keep], on=id_keys, how="left"
    )

    return df


def process_oad_file(df: pd.DataFrame) -> pd.DataFrame:
    df = df.loc[
        (df["presence_mef_bat"] == "Avec MEF")
        & (df["filtre_manuel_a_conserver"] == True)
    ]
    df = df.replace("NC", None)
    return df


def process_sites(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.dropna(subset=["code_site"])
        .assign(libelle_site=df["libelle_site"].str.upper())
        .convert_dtypes()
    )

    df = df.drop_duplicates(subset=["code_site", "libelle_site"], ignore_index=True)

    return df


def process_biens(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.dropna(subset=["code_bat_ter"])
        .assign(libelle_bat_ter=df["libelle_bat_ter"].str.upper())
        .convert_dtypes()
    )
    df = df.drop_duplicates(subset=["code_bat_ter"], keep="first", ignore_index=True)
    return df


def process_gestionnaires(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.dropna(subset=["code_gestionnaire"])
        .assign(
            libelle_gestionnaire=df["libelle_gestionnaire"]
            .str.upper()
            .str.split()
            .str.join(" ")
        )
        .convert_dtypes()
    )
    df = df.drop_duplicates(
        subset=["code_gestionnaire"], keep="first", ignore_index=True
    )

    return df


def process_biens_gestionnaires(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["code_bat_ter", "code_gestionnaire"]).convert_dtypes()
    df = df.drop_duplicates(
        subset=["code_bat_ter", "code_gestionnaire"],
        keep="first",
        ignore_index=True,
    )
    # Create calculated columns
    df["code_bat_gestionnaire"] = (
        df[["code_bat_ter", "code_gestionnaire"]].astype(str).agg("_".join, axis=1)
    )
    return df


def process_biens_occupants(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["code_bat_ter", "code_gestionnaire"]).convert_dtypes()

    # Create calculated columns
    df["code_bat_gestionnaire"] = (
        df[["code_bat_ter", "code_gestionnaire"]].astype(str).agg("_".join, axis=1)
    )

    # Reset index to ensure a clean row-based id (starting from 1)
    df = df.reset_index(drop=True)

    # Fill missing or empty occupant values with "NR - <row_number>"
    df["occupant"] = np.where(
        df["occupant"].isna() | (df["occupant"].astype(str).str.strip() == ""),
        "NR - " + (df.index + 1).astype(str),
        df["occupant"],
    )
    df["service_occupant"] = np.where(
        df["service_occupant"].isna()
        | (df["service_occupant"].astype(str).str.strip() == ""),
        "NR - " + (df.index + 1).astype(str),
        df["service_occupant"],
    )

    return df
