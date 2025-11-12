import pandas as pd

from utils.control.text import normalize_whitespace_columns


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    print("Normalizing dataframe")
    df = df.set_axis(labels=map(str.lower, df.columns), axis="columns")
    df = df.convert_dtypes()
    return df


"""
    Functions de processing des référentiels
"""


def process_ref_base_remuneration(df: pd.DataFrame) -> pd.DataFrame:
    df["base_remuneration"] = (
        df["base_remuneration"].str.strip().str.split().str.join(" ")
    )
    return df


def process_ref_base_revalorisation(df: pd.DataFrame) -> pd.DataFrame:
    df["base_revalorisation"] = (
        df["base_revalorisation"].str.strip().str.split().str.join(" ")
    )
    return df


def process_ref_experience_pro(df: pd.DataFrame) -> pd.DataFrame:
    df["experience_pro"] = df["experience_pro"].str.strip().str.split().str.join(" ")
    return df


def process_ref_niveau_diplome(df: pd.DataFrame) -> pd.DataFrame:
    df["niveau_diplome"] = df["niveau_diplome"].str.strip().str.split().str.join(" ")
    return df


def process_ref_valeur_point_indice(df: pd.DataFrame) -> pd.DataFrame:
    df["date_d_application"] = pd.to_datetime(df["date_d_application"], unit="s")
    return df


"""
    Functions de processing des fichiers sources
"""


def normalize_df_source(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_axis(
        labels=[" ".join(colname.strip().split()) for colname in df.columns],
        axis="columns",
    )
    return df


def process_agent_carto_rem(df: pd.DataFrame, cols_mapping: dict) -> pd.DataFrame:
    df = normalize_df_source(df=df)
    df = df.rename(columns=cols_mapping, errors="raise")
    cols_to_keep = list(cols_mapping.values())
    df = df.loc[cols_to_keep]
    df["matricule_agent"] = (
        df["matricule_nom_prenom"].str.split("_").str.get(0).astype("int64")
    )
    df["date_de_naissance"] = pd.to_datetime(df["date_de_naissance"], format="%d/%m/%Y")
    txt_cols = [
        "categorie",
        "genre",
        "qualite_statutaire",
        "corps",
        "grade",
        "type_de_contrat",
        "region_indemnitaire",
        "type_indemnitaire",
        "groupe_ifse",
    ]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    return df


def process_agent_info_carriere(df: pd.DataFrame, cols_mapping: dict) -> pd.DataFrame:
    df = normalize_df_source(df=df)
    df = df.rename(columns=cols_mapping, errors="raise")
    cols_to_keep = list(cols_mapping.values())
    df = df.loc[cols_to_keep]
    txt_cols = [
        "dge_perimetre",
        "nom_usuel",
        "prenom",
    ]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)
    return df


def process_agent_r4(df: pd.DataFrame, cols_mapping: dict) -> pd.DataFrame:
    df = normalize_df_source(df=df)
    df = df.rename(columns=cols_mapping, errors="raise")
    cols_to_keep = list(cols_mapping.values())
    df = df.loc[cols_to_keep]
    txt_cols = [
        "numero_poste",
        "type_de_fonction_libelle_court",
        "type_de_fonction_libelle_long",
        "type_poste",
    ]
    df = normalize_whitespace_columns(df=df, columns=txt_cols)

    df["date_acces_corps"] = df["date_acces_corps"].replace("A COMPLETER", None)
    df["date_acces_corps"] = pd.to_datetime(df["date_acces_corps"], format="%d/%m/%Y")

    return df


"""
    Functions de processing des Grist source
"""


def process_agent_diplome(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["agent"])
    df = df.rename(columns={"niveau_diplome": "id_niveau_diplome"})
    df["libelle_diplome"] = df["libelle_diplome"].str.strip().str.split().str.join(" ")
    df["id_niveau_diplome"] = df["id_niveau_diplome"].replace({0: None})
    df = df.reset_index(drop=True)
    df["id"] = df.index
    return df


def process_agent_revalorisation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["agent"])
    df = df.rename(columns={"base_revalorisation": "id_base_revalorisation"})
    df["historique"] = df["historique"].str.strip().str.split().str.join(" ")
    date_cols = ["date_dernier_renouvellement", "date_derniere_revalorisation"]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(df[date_col], unit="s", errors="coerce")
    df["id_base_revalorisation"] = df["id_base_revalorisation"].replace({0: None})
    df = df.loc[df["matricule_agent"] != 0]
    df = df.reset_index(drop=True)
    df["id"] = df.index
    return df


def process_agent_contrat(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["agent"])
    # df["duree_cumulee_contrats_tout_contrat_mef"] = (
    #     df["duree_cumulee_contrats_tout_contrat_mef"]
    #     .apply(lambda x: x.decode("utf-8") if isinstance(x, bytes) else x)
    #     .astype(str)
    #     .str.strip()
    #     .str.split()
    #     .str.join(" ")
    #     .fillna("")
    # )
    date_cols = [
        "date_premier_contrat_mef",
        "date_debut_contrat_actuel_dge",
        "date_fin_contrat_cdd_en_cours_au_soir",
        "date_de_cdisation",
    ]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(df[date_col], unit="s")
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).str.replace("\x00", "", regex=False)
    df = df.reset_index(drop=True)
    df["id"] = df.index
    return df


def process_agent_rem_variable(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["agent"])
    df = df.rename(
        columns={"part_variable_collective": "plafond_part_variable_collective"}
    )
    df["observations"] = df["observations"].str.strip().str.split().str.join(" ")
    return df


"""
    Functions de processing des fichiers finaux
"""


def process_agent(
    df_rem_carto: pd.DataFrame, df_info_car: pd.DataFrame, required_cols: list[str]
) -> pd.DataFrame:
    df = pd.merge(
        left=df_rem_carto, right=df_info_car, how="outer", on=["matricule_agent"]
    )
    df = df.loc[required_cols]
    df = df.reset_index(drop=True)
    df["id"] = df.index

    return df


def process_agent_poste(
    df_agent: pd.DataFrame,
    df_carto_rem: pd.DataFrame,
    df_r4: pd.DataFrame,
    required_cols: list[str],
) -> pd.DataFrame:
    df = pd.merge(left=df_agent, right=df_carto_rem, how="left", on=["matricule_agent"])
    df = pd.merge(left=df, right=df_r4, how="left", on=["matricule_agent"])
    df = df.loc[required_cols]
    df["date_recrutement_structure"] = None
    df = df.reset_index(drop=True)
    df["id"] = df.index
    return df


def process_agent_remuneration(
    df_rem_carto: pd.DataFrame, df_agent_rem_variable: pd.DataFrame
) -> pd.DataFrame:
    cols_to_keep = [
        "matricule_agent",
        "indice_majore",
        "type_indemnitaire",
        "region_indemnitaire",
        "total_indemnitaire_annuel",
        "total_annuel_ifse",
        "totale_brute_annuel",
        "plafond_part_variable",
        "plafond_part_variable_collective",
        "present_cartographie",
        "observations",
    ]
    df = pd.merge(
        left=df_rem_carto,
        right=df_agent_rem_variable,
        how="left",
        on=["matricule_agent"],
    )
    df = df.loc[cols_to_keep]
    map_region_indem = {
        "Pas d'indemnité de résidence": 0,
        "Zone Corse et certaines communes de l'Ain et de la Haute-Savoie": 0,
        "Zone à 0%": 0,
        "Zone à 1%": 0.001,
        "Zone à 1% com. minières Moselle": 0.001,
        "Zone à 3%": 0.003,
    }
    df["region_indemnitaire_valeur"] = df.loc["region_indemnitaire"].map(
        map_region_indem
    )
    cols = [
        "total_indemnitaire_annuel",
        "totale_brute_annuel",
        "plafond_part_variable",
        "total_annuel_ifse",
        "plafond_part_variable_collective",
    ]

    for c in cols:
        df[c] = df.loc[c].astype(str).str.replace(",", ".")
        df[c] = pd.to_numeric(arg=df.loc[c], errors="coerce")

    df["present_cartographie"] = (
        df.loc["present_cartographie"]
        .apply(lambda x: x.decode(encoding="utf-8") if isinstance(x, bytes) else x)
        .map({"T": True, "F": False})
    )
    df = df.reset_index(drop=True)
    df["id"] = df.index
    return df
