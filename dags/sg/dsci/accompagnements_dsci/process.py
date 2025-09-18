import pandas as pd
import ast

from utils.common.vars import NO_PROCESS_MSG


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    print("Normalizing dataframe")
    df.columns = map(str.lower, df.columns)
    return df


def convert_str_of_list_to_list(df: pd.DataFrame, col_to_convert: str) -> pd.DataFrame:
    df[col_to_convert] = df[col_to_convert].apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else x
    )
    return df


"""
    Fonction de processing des référentiels
"""


def process_ref_certification(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_competence_particuliere(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_bureau(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_direction(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_pole(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_profil_correspondant(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = ["id", "profil_correspondant", "intitule_long"]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    return df


def process_ref_promotion_fac(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_semainier(df: pd.DataFrame) -> pd.DataFrame:
    df["date_semaine"] = pd.to_datetime(df["date_semaine"], unit="s")
    df = df.drop(columns=["annee", "trimestre", "mois", "is_duplicate"])
    return df


def process_ref_qualite_service(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_date_debut_inactivite(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_region(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_type_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"pole": "id_pole"})
    return df


def process_ref_typologie_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    df["typologie_accompagnement"] = (
        df["typologie_accompagnement"].str.strip().str.split().str.join(" ")
    )
    return df


"""
    Fonction de processing des données Mission Innovation
"""


def process_struc_accompagnement_mi(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = [
        "id",
        "intitule",
        "direction",
        "date_de_realisation",
        "statut",
        "pole",
        "type_d_accompagnement",
        "est_certifiant",
        "places_max",
        "nb_inscrits",
        "places_restantes",
        "est_ouvert_notation",
        "informations_complementaires",
    ]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    df = df.rename(
        columns={
            "direction": "id_direction",
            "pole": "id_pole",
            "type_d_accompagnement": "id_type_accompagnement",
        }
    )
    df["date_de_realisation"] = pd.to_datetime(df["date_de_realisation"], unit="s")
    df["informations_complementaires"] = (
        df["informations_complementaires"].str.strip().str.split().str.join(" ")
    )
    df[["id_direction", "id_pole", "id_type_accompagnement"]] = df[
        ["id_direction", "id_pole", "id_type_accompagnement"]
    ].replace(0, None)

    return df


def process_struc_accompagnement_mi_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "accompagnement": "id_accompagnement",
            "type_d_accompagnement": "id_type_accompagnement",
        }
    )
    df[["id_accompagnement", "id_type_accompagnement"]] = df[
        ["id_accompagnement", "id_type_accompagnement"]
    ].replace(0, None)
    return df


"""
    Fonction de processing des données
"""


def process_struc_accompagnement_dsci(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_struc_accompagnement_opportunite_cci(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_struc_animateur_externe(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_struc_animateur_fac(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_struc_animateur_interne(df: pd.DataFrame) -> pd.DataFrame:
    return df


"""
    Fonction de processing des bilatérales
"""


def process_struc_bilaterales(df: pd.DataFrame) -> pd.DataFrame:
    df["date_de_rencontre"] = pd.to_datetime(df["date_de_rencontre"], unit="s")
    df = df.rename(columns={"direction": "id_direction"})
    return df


def process_struc_bilaterale_remontee(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "bilaterale": "id_bilaterale",
            "bureau": "id_bureau",
            "int_direction": "id_direction",
        }
    )
    df["information_a_remonter"] = (
        df["information_a_remonter"].str.strip().str.split().str.join(" ")
    )
    return df


"""
    Fonction de processing des correspondants
"""


def process_struc_correspondant(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(
        columns=[
            "type_de_correspondant",
            "type_correspondant_text",
            "competence_particuliere",
            "created_by",
            "created_at",
            "updated_at",
            "updated_by",
            "is_duplicate",
            "check_mail",
            "fac_certifications_realisees",
        ]
    )
    df = df.rename(
        columns={
            "direction": "id_direction",
            "region": "id_region",
            "promotion_fac": "id_promotion_fac",
        }
    )
    df = df.dropna(subset=["mail"])
    df["mail"] = df["mail"].str.strip()
    df = df.loc[df["mail"] != ""]
    df = df.drop_duplicates(subset=["mail"], keep="last")
    df["date_debut_inactivite"] = pd.to_datetime(df["date_debut_inactivite"], unit="s")
    df["id_region"] = df["id_region"].replace(0.0, None)
    df["id_region"] = df["id_region"].replace(0, None)
    df["id_direction"] = df["id_direction"].replace(0.0, None)
    df["id_direction"] = df["id_direction"].replace(0, None)
    return df


def process_struc_correspondant_profil(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = ["mail", "type_de_correspondant"]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    df = df.rename(columns={"type_de_correspondant": "id_type_correspondant"})
    df = df.drop_duplicates(subset=["mail"], keep="last")
    df = df.dropna(subset=["mail"])
    df["mail"] = df["mail"].str.strip()
    df = df.loc[df["mail"] != ""]
    # Convert str of list to python list
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_type_correspondant")
    df = df.explode("id_type_correspondant")
    df = df.reset_index(drop=True)
    df["id"] = df.index
    return df


def process_struc_correspondant_certification(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = ["mail", "type_de_correspondant"]
    cols_to_drop = list(set(df.columns) - set(cols_to_keep))
    df = df.drop(columns=cols_to_drop)
    df = df.rename(
        columns={
            "direction": "id_direction",
            "region": "id_region",
            "promotion_fac": "id_promotion_fac",
        }
    )
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_struc_effectif_dsci(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_struc_laboratoires_territoriaux(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_struc_quest_accompagnement_fac_hors_bercylab(
    df: pd.DataFrame,
) -> pd.DataFrame:
    return df


def process_struc_quest_inscription_passinnov(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_struc_quest_satisfaction_accompagnement_cci(
    df: pd.DataFrame,
) -> pd.DataFrame:
    return df


def process_struc_quest_satisfaction_formation_fac(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_struc_quest_satisfaction_passinnov(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_struc_charge_agent_cci(df: pd.DataFrame) -> pd.DataFrame:
    return df
