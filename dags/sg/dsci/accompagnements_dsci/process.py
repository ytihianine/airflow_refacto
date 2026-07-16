import logging

import pandas as pd
from modules.constants import NO_PROCESS_MSG
from modules.utils.process.structures import (
    convert_str_of_list_to_list,
    handle_grist_null_references,
)

"""
    Fonction de processing des référentiels
"""


def process_ref_certification(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_competence_particuliere(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_bureau(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_direction(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_pole(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_profil_correspondant(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_promotion_fac(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_semainier(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["is_duplicate"])
    return df


def process_ref_qualite_service(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_date_debut_inactivite(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_region(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_type_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_ref_typologie_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


# =================================
# Fonctions de processing dsci
# =================================
def process_effectif_dsci(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [
        "created_at",
        "updated_at",
        "created_by",
        "updated_by",
        "bureau_texte",
    ]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
    # Gestion doublon mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_accompagnement_dsci(df: pd.DataFrame) -> pd.DataFrame:
    df["annee"] = pd.to_numeric(arg=df["annee"], errors="coerce").astype("Int64")  # type: ignore
    return df


def process_accompagnement_dsci_typologie(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_typologie")
    df = df.explode(column="id_typologie")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_typologie"])
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_accompagnement", "id_typologie"]], index=False) % (2**63)
    return df


def process_accompagnement_dsci_equipe(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_equipe_s_dsci")
    df = df.explode(column="id_equipe_s_dsci")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_equipe_s_dsci"])

    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_accompagnement", "id_equipe_s_dsci"]], index=False) % (2**63)
    return df


def process_accompagnement_dsci_porteur(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_porteur_dsci")
    df = df.explode(column="id_porteur_dsci")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_porteur_dsci"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_accompagnement", "id_porteur_dsci"]], index=False) % (2**63)
    return df


# ========================================================
# Fonction de processing des données Mission Innovation
# ========================================================
def process_accompagnement_mi(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_accompagnement_mi_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    num_cols = [
        "nombre_de_participants",
        "nombre_de_reponses",
        "taux_de_reponse",
        "note_moyenne_de_satisfaction",
    ]
    cols_presentes = [col for col in num_cols if col in df.columns]
    if cols_presentes:
        df[cols_presentes] = df[cols_presentes].apply(pd.to_numeric, errors="coerce")

    return df


def process_animateur_interne(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_animateur_externe(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_animateur_fac(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_animateur_fac_certification(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_certifications_souhaitees")
    df = df.explode(column="id_certifications_souhaitees")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_certifications_souhaitees"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_animateur_fac", "id_certifications_souhaitees"]], index=False) % (
        2**63
    )
    return df


def process_animateur_fac_certification_valide(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_certifications_validees")
    df = df.explode(column="id_certifications_validees")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["id_certifications_validees"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_animateur_fac", "id_certifications_validees"]], index=False) % (
        2**63
    )
    return df


def process_laboratoires_territoriaux(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_pleniere_quest_inscription(df: pd.DataFrame) -> pd.DataFrame:
    # Dedoublonnage sur mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_pleniere_quest_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    df["note_globale"] = pd.to_numeric(df["note_globale"], errors="coerce").astype("Int64")  # type: ignore
    # Gestion de doublons
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_passinnov_quest_inscription(df: pd.DataFrame) -> pd.DataFrame:
    # Dedoublonnage sur mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_passinnov_quest_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    # Dedoublonnage sur mail
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_formation_codev_quest_inscription(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["mail"], keep="last")
    return df


def process_formation_fac_quest_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    cols_notes = ["note_module_1", "note_module_2", "note_module_3", "nps"]
    for col in cols_notes:
        df[col] = pd.to_numeric(arg=df[col], errors="coerce").astype("Int64")  # type: ignore

    # Dédoublonnage sur le mail
    df = df.drop_duplicates(subset=["mail"], keep="last")

    return df


def process_formation_fac_envie_suite_quest_satisfaction(
    df: pd.DataFrame,
) -> pd.DataFrame:
    # Gestion des colonnes
    cols_to_keep = ["id", "envies_pour_la_suite"]
    df = df.loc[:, cols_to_keep]
    df = df.rename(columns={"id": "id_formation_fac"})

    # Gestion des refs
    ref_cols = ["id_formation_fac"]
    df = handle_grist_null_references(df=df, columns=ref_cols)

    # Convertion et Explode
    df = convert_str_of_list_to_list(df=df, col_to_convert="envies_pour_la_suite")
    df = df.explode(column="envies_pour_la_suite")
    # Nettoyage des lignes vides
    df = df.dropna(subset=["envies_pour_la_suite"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_formation_fac", "envies_pour_la_suite"]], index=False) % (2**63)

    return df


def process_fac_hors_bercylab_quest_accompagnement(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_fac_hors_bercylab_quest_type_accompagnement(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df = convert_str_of_list_to_list(df=df, col_to_convert="type_d_accompagnement")
    df = df.explode(column="type_d_accompagnement")
    #  Nettoyage
    df = df.dropna(subset=["type_d_accompagnement"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_formation_fac_hors_bercylab", "type_d_accompagnement"]], index=False
    ) % (2**63)

    return df


def process_fac_hors_bercylab_quest_accompagnement_partiicipants(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df = convert_str_of_list_to_list(df=df, col_to_convert="participants")
    df = df.explode(column="participants")
    #  Nettoyage
    df = df.dropna(subset=["participants"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_formation_fac_hors_bercylab", "participants"]], index=False) % (
        2**63
    )

    return df


def process_fac_hors_bercylab_quest_accompagnement_facilitateurs(
    df: pd.DataFrame,
) -> pd.DataFrame:
    # Converstion  str en list
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_facilitateurs")
    df = df.explode(column="id_facilitateurs")
    #  Nettoyage
    df = df.dropna(subset=["id_facilitateurs"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(
        obj=df[["id_formation_fac_hors_bercylab", "id_facilitateurs"]], index=False
    ) % (2**63)

    return df


# =======================================================
# Fonction de processing des données Conseil Interne cci
# =======================================================
def process_accompagnement_cci_opportunite(df: pd.DataFrame) -> pd.DataFrame:
    # securité pour les saisies manuelles sans passer par la reference
    df["id_accompagnement"] = pd.to_numeric(arg=df["id_accompagnement"], errors="coerce")

    return df


def process_charge_agent_cci(df: pd.DataFrame) -> pd.DataFrame:
    df["annee"] = pd.to_numeric(arg=df["annee"], errors="coerce").astype("Int64")  # type: ignore
    num_cols = ["temps_passe", "taux_de_charge"]
    for col in num_cols:
        df[col] = pd.to_numeric(arg=df[col], errors="coerce")

    return df


def process_accompagnement_cci_quest_satisfaction(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


# =================================================
#   Fonction de processing des données bilatérales
# =================================================
def process_bilaterale(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(msg=NO_PROCESS_MSG)
    return df


def process_bilaterale_remontee(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["int_direction"])
    return df


# =================================================
#   Fonction de processing des correspondants
# =================================================
def process_correspondant(df: pd.DataFrame) -> pd.DataFrame:
    # Cleaning
    df["entite"] = df["entite"].replace('"', "", regex=False)

    # Filtrer les lignes
    # rows_to_drop = df.loc[(df["mail"] != "") | (df["mail"].isna())].index()
    df = df.loc[(df["mail"] != "") & (~df["mail"].isna())]
    return df


def process_correspondant_profil(df: pd.DataFrame) -> pd.DataFrame:
    # Convert str of list to python list
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_type_de_correspondant")
    df = df.explode(column="id_type_de_correspondant")
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_correspondant", "id_type_de_correspondant"]], index=False) % (
        2**63
    )

    return df


def process_correspondant_competence_particuliere(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion str en liste
    df = convert_str_of_list_to_list(df=df, col_to_convert="id_competence_particuliere")
    df = df.explode(column="id_competence_particuliere")
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_correspondant", "id_competence_particuliere"]], index=False) % (
        2**63
    )

    return df


def process_correspondant_connaissance_communaute(df: pd.DataFrame) -> pd.DataFrame:
    # Convertion
    df = convert_str_of_list_to_list(df=df, col_to_convert="connaissance_communaute")
    df = df.explode(column="connaissance_communaute")
    # Nettoyage de ligne vide
    df = df.dropna(subset=["connaissance_communaute"])
    # Ajout colonne ID unique
    df["id"] = pd.util.hash_pandas_object(obj=df[["id_correspondant", "connaissance_communaute"]], index=False) % (
        2**63
    )

    return df
