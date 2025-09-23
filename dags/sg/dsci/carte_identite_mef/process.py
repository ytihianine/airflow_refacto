import pandas as pd
from typing import Union
import datetime
import numpy as np


def generic_convert_to_float(value: str) -> float:
    if value:
        value = round(float(value) / 100, 4)
        return value
    return None


def clean_and_normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = map(str.lower, df.columns)
    # Drop des colonnes que si elles existent
    df = df.drop(columns=["id", "commentaire_archive"], errors="ignore")
    df = df.drop(df.filter(regex="^(grist|manual)").columns, axis=1)
    df = df.fillna(np.nan).replace([np.nan], [None])
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


# Traitement spécifique pour la table TELETRAVAIL
def process_teletravail(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    # Conversion en epoch time not working
    df["date"] = pd.to_datetime(df["date"], unit="s")
    return df


# Traitement spécifique pour la table teletravail_frequence
def process_teletravail_frequence(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_and_normalize_df(df)
    # Exemple : remplacer 'ND' par ''
    df.replace("ND", None, inplace=True)
    print(df.columns)
    df = df.fillna(np.nan).replace([np.nan], [None])
    # Conversion en epoch time not working
    print("Traitement de la table teletravail_frequence effectué.")
    return df


# Traitement spécifique pour la table teletravail_frequence
def process_teletravail_opinion(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_and_normalize_df(df)
    # Exemple : remplacer 'ND' par ''
    df.replace("ND", None, inplace=True)
    print(df.columns)
    df = df.fillna(np.nan).replace([np.nan], [None])
    # Conversion en epoch time not working
    print("Traitement de la table teletravail_opinion effectué.")
    return df


# Traitement spécifique pour Effectifs_MEFR_par_direction
def process_mef_par_direction(df: pd.DataFrame) -> pd.DataFrame:
    # Exemple : Suppression des colonnes vides et conversion de valeurs
    df = df.fillna(np.nan).replace([np.nan], [None])

    print("Traitement de la table Effectifs_MEFR_par_direction effectué.")
    return df


# Traitement spécifique pour Effectif_2022
def process_effectif_direction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(
        columns={
            "nombre_d_agent": "nombre_agents",
        }
    )
    print("Traitement de la table Effectif_2022 effectué.")
    return df


# Traitement spécifique pour Effectifs_par_perimetre
def process_effectifs_par_perimetre(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    # Renommer la colonne 'type' en 'type_budget'
    df = df.rename(columns={"type": "type_budget"})

    print("Traitement de la table Effectifs_par_perimetre effectué.")
    return df


# Traitement spécifique pour Effectifs_par_d partements
def process_effectif_par_departements(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])

    print("Traitement de la table Effectifs_par_departements effectué.")
    return df


# Traitement spécifique pour # Traitement spécifique pour Budget_total
def process_budget_total(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(
        columns={
            "etiquettes_de_lignes": "libelle",
            "somme_de_cp_t2": "somme_cp_t2",
            "somme_de_cp_hors_t2": "somme_cp_ht2",
            "somme_de_cp_t2_ht2_bt": "somme_cp_t2_ht2_bt",
            "part_du_total": "part_du_total",
            "annee": "annee",
            "type_budget": "type_budget",
        }
    )
    print("Traitement de la table Budget_Total effectué.")
    return df


def process_budget_pilotable(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    # Exemple : Remplacer les valeurs négatives par 0
    print("Traitement de la table Budget_pilotable effectué.")
    return df


# Fonction pour Budget_General
def process_budget_general(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(
        columns={
            "etiquettes_de_lignes": "libelle",
            "somme_de_cp_t2": "somme_cp_t2",
            "somme_de_cp_hors_t2": "somme_cp_ht2",
            "somme_de_cp_t2_ht2": "somme_cp_t2_ht2",
            "part_du_total": "part_du_total",
            "annee": "annee",
            "type_budget": "type_budget",
        }
    )

    return df


# Fonction pour Evolution_budget_mef
def process_evolution_budget_mef(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])

    print("Traitement de la table Evolution_budget_mef effectué.")
    return df


# Fonction pour Montant_intervention_invest
# def process_montant_invest(df: pd.DataFrame) -> pd.DataFrame:
#     df = df.fillna(np.nan).replace([np.nan], [None])

#     print("Traitement de la table Montant_intervention_invest effectué.")
#     return df


def process_montant_invest(df: pd.DataFrame) -> pd.DataFrame:
    # Remplacer les NaN par None
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(
        columns={
            "source": "source_montant",
        }
    )

    # Extraire l'année de la colonne 'source_montant' et la mettre dans 'annee'
    df["source_montant_split"] = df["source_montant"].str.split()
    df["annee"] = df["source_montant_split"].str.get(1)

    # Modifier la colonne 'source_montant' pour ne conserver que "LFI"
    df["source_montant"] = df["source_montant_split"].str.get(0)
    df = df.drop(columns=["source_montant_split"])

    print("Traitement de la table Montant_intervention_invest effectué.")
    return df


# Fonction pour Engagement_Agent
def process_engagement_agent(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])

    print("Traitement de la table Engagement_Agent effectué.")
    return df


# Fonction pour RESULTAT_ELECTIONS
def process_resultat_elections(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])

    print("Traitement de la table RESULTAT_ELECTIONS effectué.")
    return df


# Fonction pour Taux_participation
def process_taux_participation(df: pd.DataFrame) -> pd.DataFrame:
    # Remplacer les valeurs NaN par None
    df = df.fillna(np.nan).replace([np.nan], [None])

    # Unpivoter les colonnes d'années
    df_unpivoted = df.melt(
        id_vars=["secteur", "tri"],  # Colonnes à conserver telles quelles
        var_name="annee",  # Nom de la nouvelle colonne pour les années
        value_name="taux_participation",  # Nom de la nouvelle colonne pour les valeurs
    )

    # Filtrer les lignes où 'valeur' est NaN
    df_unpivoted = df_unpivoted.dropna(subset=["annee", "taux_participation"])
    df_unpivoted["annee"] = df_unpivoted["annee"].str.replace("c", "", regex=False)

    print("Traitement de la table Taux_participation effectué.")
    return df_unpivoted


# Fonction pour plafond_etpt
def process_plafond_etpt(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(
        columns={
            "Designation_ministere_ou_budget_annexe": "designation_ministere_ou_budget_annexe",
            "Plafond_en_ETPT": "plafond_en_etpt",
            "Part_du_total": "part_du_total",
        }
    )
    df["source"] = df["source"].str.split()
    df["annee"] = df["source"].str.get(1)
    df["source"] = df["source"].str.get(0)
    print("Traitement de la table plafond_etpt effectué.")
    return df


# Fonction pour db_plafond_etpt
def process_db_plafond_etpt(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(
        columns={
            "Source": "source",
            "Annee": "annee",
            "Type_budgetaire": "type_budgetaire",
            "Type_de_valeur": "type_de_valeur",
            "Type_de_budget": "type_de_budget",
            "Designation_ministere_ou_budget_annexe": "designation_ministere_ou_budget_annexe",
            "Valeur": "valeur",
            "Part_du_total": "part_du_total",
            "Unite": "unite",
        }
    )
    print("Traitement de la table db_plafond_etpt effectué.")
    return df


# Fonction pour masse_salariale
def process_masse_salariale(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(
        columns={
            "designation_du_ministere_ou_du_budget": "designation_ministere_ou_compte",
        }
    )
    return df


# Fonction pour masse_salariale
def process_budget_ministere(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.rename(
        columns={
            "budgets_annexes": "budget_annexe",
            "comptes_d_affectation_speciale": "compte_affection_speciale",
            "comptes_de_concours_financiers": "compte_concours_financiers",
            "total": "budget_total",
        }
    )
    num_cols = [
        "budget_general",
        "budget_annexe",
        "compte_affection_speciale",
        "compte_concours_financiers",
        "budget_total",
    ]
    # Conversion Mds euros en euros
    df[num_cols] = df[num_cols].fillna(0.0)
    df[num_cols] = df[num_cols].multiply(10**9).round(decimals=0)
    return df
