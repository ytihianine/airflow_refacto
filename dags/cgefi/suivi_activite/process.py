import pandas as pd

from utils.control.structures import convert_str_of_list_to_list
from utils.config.vars import NO_PROCESS_MSG


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    print("Normalizing dataframe")
    df.columns = map(str.lower, df.columns)
    return df


"""
    Fonction de processing des référentiels
"""


def process_ref_type_organisme(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_ref_secteur_professionnel(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_ref_actions(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_ref_type_de_frais(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_formation_specialite(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_ref_sous_type_de_frais(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "type_de_frais": "id_type_de_frais",
        }
    )
    return df


def process_ref_actif_type(df: pd.DataFrame) -> pd.DataFrame:
    print(NO_PROCESS_MSG)
    return df


def process_ref_actif_sous_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "type_d_actif": "id_type_actif",
            "sous_type": "actif_sous_type",
        }
    )
    return df


def process_ref_passif_type(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_ref_passif_sous_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "type_passif": "id_type_passif",
        }
    )
    return df


"""
    Fonction de processing des informations générales
"""


def process_region_atpro(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["tri"])
    return df


def process_organisme(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["type_d_organisme"])
    return df


def process_organisme_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["id", "libelle_court", "libelle_long"])
    df = df.rename(columns={"type_d_organisme": "id_type_organisme"})
    df["id_type_organisme"] = convert_str_of_list_to_list(
        df=df, col_to_convert="id_type_organisme"
    )
    df["id_type_organisme"] = df["id_type_organisme"].explode(ignore_index=True)
    df["id"] = df.index
    return df


def process_controleur(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"organisme": "id_organisme"})
    df["mail"] = df["mail"].str.strip()
    return df


"""
    Fonction de processing du processus 6
"""


def process_process_6_a01(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "organisme": "id_organisme",
        }
    )
    return df


def process_process_6_b01(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "organisme": "id_organisme",
        }
    )
    return df


def process_process_6_d01(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "organisme": "id_organisme",
        }
    )
    return df


def process_process_6_e01(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "organisme": "id_organisme",
        }
    )
    return df


def process_process_6_g02(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={"organisme": "id_organisme", "c500h_et_plus": "500h_et_plus"}
    )
    df = df.melt(
        id_vars=["annee", "id_organisme", "type_d_actions"],
        value_vars=[
            "moins_de_5h",
            "de_5_a_10h",
            "de_11_a_59h",
            "de_60_a_199h",
            "de_200_a_499h",
            "500h_et_plus",
            "non_repartis",
        ],
        var_name="categorie_duree",
        value_name="nb_formation",
    )
    df["id"] = df.index
    return df


def process_process_6_g03(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "organisme": "id_organisme",
        }
    )
    df = df.melt(
        id_vars=["annee", "id_organisme", "type_d_actions"],
        value_vars=[
            "niveau_7_ou_8",
            "niveau_6",
            "niveau_5",
            "niveau_4",
            "niveau_3",
            "sans_niveau",
            "non_repartis",
        ],
        var_name="niveau_formation",
        value_name="nb_formation",
    )
    df["id"] = df.index
    return df


def process_process_6_h01(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "organisme": "id_organisme",
        }
    )
    return df


"""
    Fonction de processing du processus 4
"""


def process_process_4_a01(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_a02(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_h02(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_b01(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_c01(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_b02(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_e01(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_e02(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_g01(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_e11(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_g02(df: pd.DataFrame) -> pd.DataFrame:
    return df


def process_process_4_h01(df: pd.DataFrame) -> pd.DataFrame:
    return df


"""
    Fonction de processing du processus ATPro
"""


def process_process_atpro_a01(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["region_tri"])
    df = df.rename(
        columns={
            "region": "id_region",
            "montant_net_des_frais_engages_avec_prise_en_compte_des_annulations_probables": "montant_net_des_frais_engages_avec_annulations_probables",
        }
    )
    return df


def process_process_atpro_f01(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "region": "id_region",
            "type_d_actif": "id_type_actif",
            "sous_type_d_actif": "id_sous_type_actif",
        }
    )
    return df


def process_process_atpro_f02(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(
        columns={
            "region": "id_region",
            "type_passif": "id_type_passif",
            "sous_type_passif": "id_sous_type_passif",
        }
    )
    return df


def process_process_atpro_g02(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"region": "id_region"})
    print(NO_PROCESS_MSG)
    return df


def process_process_atpro_h01(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"region": "id_region"})
    return df


def process_process_atpro_j01(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"region": "id_region"})
    return df
