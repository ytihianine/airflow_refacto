import pandas as pd
import numpy as np
import datetime
from typing import Union

from utils.control.number import is_in_range, is_upper

from utils.df_utility import df_info, tag_last_value_rows


def generic_convert_to_float(value: str) -> float:
    if value:
        value = round(float(value) / 100, 4)
        return value
    return None


def clean_and_normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = map(str.lower, df.columns)
    # Drop des colonnes que si elles existent
    df = df.drop(columns=["id", "commentaire_archive"], errors="ignore")

    df = df.fillna(np.nan).replace([np.nan], [None])
    return df


def convert_bytes_to_string(df: pd.DataFrame) -> pd.DataFrame:
    for colname, dtype in df.dtypes.items():
        if dtype == object:  # Only process byte object columns.
            try:
                df[colname] = df[colname].str.decode("utf-8").fillna(df[colname])
            except Exception:
                df[colname] = df[colname].str.decode("latin-1").fillna(df[colname])
    return df


def generate_date(year: int, semester: str) -> Union[datetime.datetime, None]:
    semester_values = {"S1": 6, "Total": 12}

    if year is None or semester is None:
        print("Either year or semester value is None.")
        return None

    if semester not in semester_values.keys():
        print(
            f"Invalid semester value: {semester}. Must be one of {list(semester_values.keys())}"
        )
        return None

    try:
        year = int(year)
        month = semester_values[semester]
        date = datetime.datetime(year, month, 1)
        return date
    except ValueError:
        print(f"year cannot be converted to int. Current year value: {year}")
        return None
    except Exception as e:
        print(f"An exception as occured: {e}")
        return None

    return None


def has_any_data_differences(df_db: pd.DataFrame, df_grist: pd.DataFrame) -> bool:
    # Pour avoir les colonnes dans le même ordre
    df_grist = df_grist.sort_index(axis=1)
    df_db = df_db.sort_index(axis=1)

    for col in df_grist.columns:
        if col in df_db.columns:
            common_type = np.result_type(df_grist[col], df_db[col])
            df_grist[col] = df_grist[col].astype(common_type)
            df_db[col] = df_db[col].astype(common_type)

    df_info(df=df_grist, df_name="df grist - recherche de différence")
    df_info(df=df_db, df_name="df db - recherche de différence")

    df_are_equals = df_grist.equals(df_db)
    if df_are_equals:
        print("df_are_equals = True")
        return False
    else:
        print("df_are_equals = False")
        return True
    return df_are_equals


def process_reseaux_sociaux(df: pd.DataFrame) -> pd.DataFrame:
    # Processing des données
    df["date"] = pd.to_datetime(df["mois"], unit="s")
    df = pd.melt(
        df,
        id_vars=["date"],
        value_vars=[
            "twitter_x_",
            "facebook",
            "linkedin_mef",
            "instagram",
            "linkedin_sg",
        ],
        var_name="reseaux_sociaux",
        value_name="abonnes",
    )

    df.replace(
        {
            "twitter_x_": "Twitter (X)",
            "facebook": "Facebook",
            "linkedin_mef": "LinkedIn MEF",
            "linkedin_sg": "LinkedIn SG",
            "instagram": "Instagram",
        },
        inplace=True,
    )

    # Clean
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.replace("", None)
    df = df.dropna(subset=["abonnes"])

    # Data control
    if not is_upper(df=df, cols_to_check=["abonnes"], seuil=0, inclusive=False):
        raise ValueError("Certaines valeurs sont négatives !")
    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_abonnes_aux_lettres(df: pd.DataFrame) -> None:
    df["date"] = pd.to_datetime(df["mois"], unit="s")
    df = df.drop(columns=["mois"])

    # Clean
    cols_with_values = [
        "nouveaux_abonnes_bip",
        "desabonnes_bip",
        "cumul_total_abonnes_bip",
        "nouveaux_abonnes_bie",
        "desabonnes_bie",
        "cumul_total_abonnes_bie",
        "nouveaux_abonnes_totaux",
        "desabonnes_totaux",
        "cumul_total_abonnes_totaux",
    ]
    df = df.replace("", None)
    df = df.dropna(subset=cols_with_values)

    # Data control
    if not is_upper(df=df, cols_to_check=cols_with_values, seuil=0, inclusive=False):
        raise ValueError("Certaines valeurs sont négatives !")

    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_visites_portail(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["mois"], unit="s")
    df = df.drop(columns=["mois"])

    # Clean
    cols_with_values = ["visites"]
    df = df.replace("", None)
    df = df.dropna(subset=cols_with_values)

    # Data control
    if not is_upper(df=df, cols_to_check=cols_with_values, seuil=0, inclusive=False):
        raise ValueError("Certaines valeurs sont négatives !")
    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_visites_bercyinfo(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["mois"], unit="s")
    df = df.drop(columns=["mois"])

    # Clean
    cols_with_values = ["visites"]
    df = df.replace("", None)
    df = df.dropna(subset=cols_with_values)

    # Data control
    if not is_upper(df=df, cols_to_check=cols_with_values, seuil=0, inclusive=False):
        raise ValueError("Certaines valeurs sont négatives !")
    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_performances_lettres(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["mois"], unit="s")
    df = df.drop(columns=["mois"])

    # Pivot df
    colnames_mapping = {
        "bip_taux_d_ouverture_moyenne_": "bip_taux_d_ouverture",
        "bip_taux_de_visite_moyenne_": "bip_taux_de_visite",
        "bie_taux_d_ouverture_moyenne_": "bie_taux_d_ouverture",
        "bie_taux_de_visite_moyenne_": "bie_taux_de_visite",
    }

    df = pd.melt(
        df,
        id_vars=["date"],
        value_vars=colnames_mapping.keys(),
        var_name="indicateurs",
        value_name="taux",
    )

    # Cleaning
    df.replace(colnames_mapping, inplace=True)
    df = df.dropna(subset=["taux"])
    df = tag_last_value_rows(df=df, colname_max_value="date")

    # Data control
    if not is_in_range(df=df, cols_to_check=["taux"], seuil_inf=0, seuil_sup=1):
        raise ValueError("Certaines valeurs ne sont pas entre 0 et 1 !")

    df = df.convert_dtypes()

    return df


def process_visites_alize(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["mois"], unit="s")
    df = df.drop(columns=["mois"])

    # Clean
    cols_with_values = ["visites"]
    df = df.replace("", None)
    df = df.dropna(subset=cols_with_values)

    # Data control
    if not is_upper(df=df, cols_to_check=cols_with_values, seuil=0, inclusive=False):
        raise ValueError("Certaines valeurs sont négatives !")
    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_visites_intranet_sg(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = pd.to_datetime(df["mois"], unit="s")
    df = df.drop(columns=["mois"])

    # Clean
    cols_with_values = ["visites"]
    df = df.replace("", None)
    df = df.dropna(subset=cols_with_values)

    # Data control
    if not is_upper(df=df, cols_to_check=cols_with_values, seuil=0, inclusive=False):
        raise ValueError("Certaines valeurs sont négatives !")
    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_budget_depense(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["faits_marquants"])

    txt_colnames = ["semestre", "type_depense"]
    for colname in txt_colnames:
        df[colname] = df[colname].str.strip()

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_engagement_agents_mef(df: pd.DataFrame) -> pd.DataFrame:
    # Clean
    df["indicateurs"] = df["indicateurs"].str.strip()
    cols_with_values = ["taux_engagement"]
    df = df.replace("", None)
    df = df.dropna(subset=cols_with_values)

    # Data control
    if not is_in_range(df=df, cols_to_check=cols_with_values, seuil_inf=0, seuil_sup=1):
        raise ValueError("Certaines valeurs ne sont pas entre 0 et 1 !")

    df = tag_last_value_rows(df=df, colname_max_value="annee")

    return df


def process_qualite_vie_travail(df: pd.DataFrame) -> pd.DataFrame:
    # Clean
    df["indicateurs"] = df["indicateurs"].str.strip()
    cols_with_values = ["taux_satisfaction"]
    df = df.replace("", None)
    df = df.dropna(subset=cols_with_values)

    # Data control
    if not is_in_range(df=df, cols_to_check=cols_with_values, seuil_inf=0, seuil_sup=1):
        raise ValueError("Certaines valeurs ne sont pas entre 0 et 1 !")

    # Add info
    df = tag_last_value_rows(df=df, colname_max_value="annee")

    return df


def process_collab_inter_structure(df: pd.DataFrame) -> pd.DataFrame:
    txt_colnames = ["structure", "indicateurs"]
    for colname in txt_colnames:
        df[colname] = df[colname].str.strip()
    df = tag_last_value_rows(df=df, colname_max_value="annee")
    return df


def process_obs_interne(df: pd.DataFrame) -> pd.DataFrame:
    df["indicateurs"] = df["indicateurs"].str.strip()
    df["valeur"] = np.where(
        df["unite"] == "%", df["valeur"].apply(generic_convert_to_float), df["valeur"]
    )
    df = tag_last_value_rows(df=df, colname_max_value="annee")
    return df


def process_indicateurs_metiers(df: pd.DataFrame) -> pd.DataFrame:
    df["indicateurs"] = df["indicateurs"].str.strip()
    df["unite"] = df["unite"].str.strip()

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_enquete_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    df["indicateurs"] = df["indicateurs"].str.strip()
    df["unite"] = df["unite"].str.strip()

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_etudes(df: pd.DataFrame) -> pd.DataFrame:
    df["demandeurs"] = df["demandeurs"].str.strip()

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_communique_presse(df: pd.DataFrame) -> pd.DataFrame:
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_studio_graphique(df: pd.DataFrame) -> pd.DataFrame:
    df["demandeurs"] = df["demandeurs"].str.strip()
    df = df.dropna(subset=["semestre", "creation_graphique"])

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_rh_formation(df: pd.DataFrame) -> pd.DataFrame:
    df["indicateurs"] = df["indicateurs"].str.strip()
    df = df.drop(columns=["precision"])

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_rh_turnover(df: pd.DataFrame) -> pd.DataFrame:
    # Ajout de la date
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))

    # Clean
    cols_with_values = ["valeur"]
    df = df.replace("", None)
    df = df.dropna(subset=cols_with_values)

    # Data control
    if not is_in_range(df=df, cols_to_check=cols_with_values, seuil_inf=0, seuil_sup=1):
        raise ValueError("Certaines valeurs ne sont pas entre 0 et 1 !")

    # Add info
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_rh_contractuel(df: pd.DataFrame) -> pd.DataFrame:
    # Generate date
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))

    # Clean
    df = df.rename(columns={"taux_d_agents_contractuel": "taux_agents_contractuels"})
    df = df.replace("", None)
    df = df.dropna(subset=["taux_agents_contractuels"])

    # Data control
    if not is_in_range(
        df=df, cols_to_check=["taux_agents_contractuels"], seuil_inf=0, seuil_sup=1
    ):
        raise ValueError("Certaines valeurs ne sont pas comprise entre 0 et 1 !")

    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_obs_interne_participation(df: pd.DataFrame) -> pd.DataFrame:
    # Clean
    df = df.rename(columns={"taux_de_participation": "taux_participation"})
    df = df.replace("", None)
    df = df.dropna(subset=["taux_participation"])

    # Data control
    if not is_in_range(
        df=df, cols_to_check=["taux_participation"], seuil_inf=0, seuil_sup=1
    ):
        raise ValueError("Certaines valeurs ne sont pas comprise entre 0 et 1 !")

    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="annee")

    return df


def process_enquete_360(df: pd.DataFrame) -> pd.DataFrame:
    # Clean
    df = df.drop(columns=["unite"])
    df = df.replace("", None)
    df = df.dropna(subset=["valeur"])

    return df


def process_ouverture_lettre_alize(df: pd.DataFrame) -> pd.DataFrame:
    # Clean
    df = df.rename(columns={"taux_d_ouverture": "taux_ouverture"})
    df = df.replace("", None)
    df = df.dropna(subset=["taux_ouverture"])

    # Data control
    if not is_in_range(
        df=df, cols_to_check=["taux_ouverture"], seuil_inf=0, seuil_sup=1
    ):
        raise ValueError("Certaines valeurs ne sont pas comprise entre 0 et 1 !")

    return df


def process_engagement_environnement(df: pd.DataFrame) -> pd.DataFrame:
    # Processing des données
    values_to_replace = {
        "tout_a_fait_d_accord": "Tout à fait d'accord",
        "plutot_d_accord": "Plutôt d'accord",
        "plutot_pas_d_accord": "Plutôt pas d'accord",
        "pas_du_tout_d_accord": "Pas du tout d'accord",
        "je_ne_sais_pas": "Je ne sais pas",
        "sans_reponse": "Sans réponse",
    }
    df = pd.melt(
        df,
        id_vars=["annee", "niveau"],
        value_vars=list(values_to_replace.keys()),
        var_name="indicateurs",
        value_name="nombre_votants",
    )

    # Define conditions and choices
    conditions = [
        df["indicateurs"].isin(["tout_a_fait_d_accord", "plutot_d_accord"]),
        df["indicateurs"].isin(["plutot_pas_d_accord", "pas_du_tout_d_accord"]),
    ]
    choices = ["Sous-total d'accord", "Sous-total pas d'accord"]

    # Apply conditions to create new column
    df["indicateurs_regroupement"] = np.select(
        conditions, choices, default=df["indicateurs"]
    )

    df.replace(values_to_replace, inplace=True)
    # Clean
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.replace("", None)
    df = df.dropna(subset=["annee", "nombre_votants"])

    # Data control
    if not is_upper(df=df, cols_to_check=["nombre_votants"], seuil=0, inclusive=True):
        raise ValueError("Certaines valeurs sont négatives !")
    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="annee")

    return df
