import datetime
import logging

import numpy as np
import pandas as pd
from modules.utils.process.number import is_in_range, is_upper
from modules.utils.process.structures import tag_last_value_rows


def drop_additionals_columns(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    if columns is None:
        columns = ["commentaire_archive"]
    df = df.drop(columns=columns, errors="ignore")
    return df


def generic_convert_to_float(value: str | None) -> float | None:
    if value:
        result = round(number=float(value) / 100, ndigits=4)
        return result
    return None


def generate_date(year: int, semester: str) -> datetime.datetime | None:
    semester_values = {"S1": 6, "Total": 12}

    if year is None or semester is None:
        logging.info(msg="Either year or semester value is None.")
        return None

    if semester not in semester_values:
        logging.info(msg=f"Invalid semester value: {semester}. Must be one of {list(semester_values.keys())}")
        return None

    try:
        year = int(year)
        month = semester_values[semester]
        date = datetime.datetime(year, month, 1)
        return date
    except ValueError:
        logging.info(msg=f"year cannot be converted to int. Current year value: {year}")
        return None
    except Exception as e:
        logging.info(msg=f"An exception as occured: {e}")
        return None

    return None


def process_reseaux_sociaux(df: pd.DataFrame) -> pd.DataFrame:
    # Processing des données
    df = drop_additionals_columns(df=df)
    df["date"] = pd.to_datetime(df["mois"], unit="s").astype("datetime64[s]")
    df = pd.melt(
        df,
        id_vars=["date"],
        value_vars=[
            "twitter_x_",
            "facebook",
            "linkedin_mef",
            "instagram",
            "linkedin_sg",
            "tiktok_mef",
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
            "tiktok_mef": "Tiktok MEF",
        },
        inplace=True,
    )

    # Clean
    df = df.replace("", None)
    df = df.dropna(subset=["abonnes"])

    # Data control
    if not is_upper(df=df, cols_to_check=["abonnes"], seuil=0, inclusive=False):
        raise ValueError("Certaines valeurs sont négatives !")
    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")

    df = df.sort_values(by=["date", "reseaux_sociaux"]).reset_index(drop=True)
    df["id"] = df.index

    return df


def process_abonnes_lettres(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["date"] = pd.to_datetime(df["mois"], unit="s").astype("datetime64[s]")
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
    df = drop_additionals_columns(df=df)
    df["date"] = pd.to_datetime(df["mois"], unit="s").astype("datetime64[s]")
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
    df = drop_additionals_columns(df=df)
    df["date"] = pd.to_datetime(df["mois"], unit="s").astype("datetime64[s]")
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
    df = drop_additionals_columns(df=df)
    df["date"] = pd.to_datetime(df["mois"], unit="s").astype("datetime64[s]")
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
        value_vars=list(colnames_mapping.keys()),
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

    df = df.sort_values(by=["date", "indicateurs"]).reset_index(drop=True)
    df["id"] = df.index

    return df


def process_visites_alize(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["date"] = pd.to_datetime(df["mois"], unit="s").astype("datetime64[s]")
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
    df = drop_additionals_columns(df=df)
    df["date"] = pd.to_datetime(df["mois"], unit="s").astype("datetime64[s]")
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


def process_synthese_depenses(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df = df.drop(columns=["faits_marquants"])

    txt_colnames = ["semestre", "type_depense"]
    for colname in txt_colnames:
        df[colname] = df[colname].str.strip()

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_engagement_agents_mef(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
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
    df = drop_additionals_columns(df=df)
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
    df = drop_additionals_columns(df=df)
    txt_colnames = ["structure", "indicateurs"]
    for colname in txt_colnames:
        df[colname] = df[colname].str.strip()
    df = tag_last_value_rows(df=df, colname_max_value="annee")
    return df


def process_obs_interne(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["indicateurs"] = df["indicateurs"].str.strip()
    df["valeur"] = np.where(df["unite"] == "%", df["valeur"].apply(generic_convert_to_float), df["valeur"])
    df = tag_last_value_rows(df=df, colname_max_value="annee")
    return df


def process_indicateurs_metiers(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["indicateurs"] = df["indicateurs"].str.strip()
    df["unite"] = df["unite"].str.strip()

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_enquete_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["indicateurs"] = df["indicateurs"].str.strip()
    df["unite"] = df["unite"].str.strip()

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_etudes(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["demandeurs"] = df["demandeurs"].str.strip()

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_communique_presse(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_studio_graphique(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["demandeurs"] = df["demandeurs"].str.strip()
    df = df.dropna(subset=["semestre", "creation_graphique"])

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_rh_formation(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["indicateurs"] = df["indicateurs"].str.strip()
    df = df.drop(columns=["precision"])

    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_rh_turnover(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    # Ajout de la date
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")

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
    df = drop_additionals_columns(df=df)
    # Generate date
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")

    # Clean
    df = df.rename(columns={"taux_d_agents_contractuel": "taux_agents_contractuels"})
    df = df.replace("", None)
    df = df.dropna(subset=["taux_agents_contractuels"])

    # Data control
    if not is_in_range(df=df, cols_to_check=["taux_agents_contractuels"], seuil_inf=0, seuil_sup=1):
        raise ValueError("Certaines valeurs ne sont pas comprise entre 0 et 1 !")

    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")
    return df


def process_obs_interne_participation(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    # Clean
    df = df.rename(columns={"taux_de_participation": "taux_participation"})
    df = df.replace("", None)
    df = df.dropna(subset=["taux_participation"])

    # Data control
    if not is_in_range(df=df, cols_to_check=["taux_participation"], seuil_inf=0, seuil_sup=1):
        raise ValueError("Certaines valeurs ne sont pas comprise entre 0 et 1 !")

    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="annee")

    return df


def process_enquete_360(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    # Clean
    df = df.drop(columns=["unite"])
    df = df.replace("", None)
    df = df.dropna(subset=["valeur"])
    df = tag_last_value_rows(df=df, colname_max_value="annee")

    return df


def process_ouverture_lettre_alize(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    # Clean
    df = df.rename(
        columns={
            "taux_d_ouverture": "taux_ouverture",
            "nombre_d_agents": "nombre_agent",
        }
    )
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    df = tag_last_value_rows(df=df, colname_max_value="date")
    # Clean
    df = df.replace("", None)
    df = df.dropna(subset=["taux_ouverture"])

    # Data control
    if not is_in_range(df=df, cols_to_check=["taux_ouverture"], seuil_inf=0, seuil_sup=1):
        raise ValueError("Certaines valeurs ne sont pas comprise entre 0 et 1 !")

    return df


def process_engagement_environnement(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
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
    df["indicateurs_regroupement"] = np.select(condlist=conditions, choicelist=choices, default=df["indicateurs"])

    df.replace(values_to_replace, inplace=True)
    # Clean
    df = df.replace("", None)
    df = df.dropna(subset=["annee", "nombre_votants"])

    # Data control
    if not is_upper(df=df, cols_to_check=["nombre_votants"], seuil=0, inclusive=True):
        raise ValueError("Certaines valeurs sont négatives !")
    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="annee")

    df = df.sort_values(by=["annee", "niveau"]).reset_index(drop=True)
    df["id"] = df.index

    return df


def process_notes_veilles(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df = df.rename(
        columns={
            "nombre_de_notes": "nombre_note",
            "nombre_de_signalements": "nombre_signalements",
        }
    )
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")

    # Clean
    df = df.dropna(subset=["date"])
    cols_numeriques = ["nombre_note", "nombre_signalements"]
    for col in cols_numeriques:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(0).astype(int)

    if not is_upper(df=df, cols_to_check=cols_numeriques, seuil=0, inclusive=True):
        raise ValueError("Erreur : Des nbres de notes ou signalements sont négatifs !")
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_impressions_reseaux_sociaux(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df["date"] = pd.to_datetime(df["mois"], unit="s").astype("datetime64[s]")
    df = df.drop(columns=["mois"])
    df = df.rename(columns={"nombre_d_impression_globale": "impressions"})

    # Clean
    cols_with_values = ["impressions"]
    df = df.replace("", None)
    df = df.dropna(subset=cols_with_values)

    # Data control
    if not is_upper(df=df, cols_to_check=cols_with_values, seuil=0, inclusive=True):
        """Inclusive=True : si pas de post par mois on peut avoir 0 impressions"""
        raise ValueError("Certaines valeurs sont négatives !")
    # Add additionnal info
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_impact_actions_com(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df = df.rename(
        columns={
            "nombre_d_articles": "nombre_article",
            "nombre_de_vues": "nombre_vue",
        }
    )
    # Conv de l'annee en string
    df["annee"] = df["annee"].astype(str).str.strip()
    df["date"] = pd.to_datetime(df["annee"], format="%Y", errors="coerce").astype("datetime64[s]")
    df = df.drop(columns=["annee"])
    df = df.dropna(subset=["date"])  # lignes ou annee invalide
    cols_numeriques = ["nombre_article", "nombre_vue"]
    for col in cols_numeriques:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(0).astype(int)

    if not is_upper(df=df, cols_to_check=cols_numeriques, seuil=0, inclusive=True):
        raise ValueError("Erreur : Des nbres d'articles ou vues sont négatifs !")
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_recommandation_strat(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df = df.rename(
        columns={
            "nombre_de_recommandations": "nombre_recommandation",
        }
    )
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    # Clean
    df = df.dropna(subset=["nombre_recommandation"])
    cols_numeriques = ["nombre_recommandation"]
    for col in cols_numeriques:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(0).astype(int)
    if not is_upper(df=df, cols_to_check=cols_numeriques, seuil=0, inclusive=True):
        raise ValueError("Erreur : Le nombre de recommandations ne peut pas être négatif !")
    df = tag_last_value_rows(df=df, colname_max_value="date")

    return df


def process_projets_graphiques(df: pd.DataFrame) -> pd.DataFrame:
    df = drop_additionals_columns(df=df)
    df = df.rename(columns={"nombre_de_projets_graphiques_realises": "nombre_graphique_realise"})
    df["date"] = list(map(generate_date, df["annee"], df["semestre"]))
    df["date"] = df["date"].astype("datetime64[s]")
    # Definitions variables
    col_total = "nombre_graphique_realise"
    cols_directions = [
        "cabinets_ministeriels",
        "directions",
        "secretariat_general",
        "part_de_la_dge",
    ]
    cols_numeriques = [col_total, *cols_directions]

    # Data cleann
    for col in cols_numeriques:
        # On force en nombre, on remplace les vides par 0, et on force en intier
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].fillna(0).astype(int)

    df = pd.melt(
        frame=df,
        id_vars=["date", col_total],
        value_vars=cols_directions,
        var_name="commanditaire",
        value_name="nombre_projets_graphique",
    )
    # Data control
    cols_to_check = ["nombre_projets_graphique", col_total]
    if not is_upper(df=df, cols_to_check=cols_to_check, seuil=0, inclusive=True):
        raise ValueError("Erreur : Certaines valeurs de projet sont négatives !")
    df = tag_last_value_rows(df=df, colname_max_value="date")

    df = df.sort_values(by=["date", "commanditaire"]).reset_index(drop=True)
    df["id"] = df.index

    return df
