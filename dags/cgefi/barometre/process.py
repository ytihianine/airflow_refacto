import pandas as pd
import numpy as np


def normalize_str(value: str) -> str:
    try:
        value = value.strip()
        value = value.upper()
        value = value.replace("’", "'")
        return value
    except Exception as e:
        print(f"Valeur problématique: {value}")
        raise ValueError(e) from e


def concat_df(list_df: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(list_df)


def split_df_organisme(df_orga: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    col_id = ["denomination", "sigle", "siren"]
    cols_df_orga_merge = [
        "cartographie_attendue",
        "efc_attendu",
        "type_rapport",
        "rapport_annuel_attendu",
    ]
    cols_df_orga = list(set(df_orga.columns) - set(cols_df_orga_merge))

    return df_orga[cols_df_orga], df_orga[col_id + cols_df_orga_merge]


def process_organisme_hors_corpus(
    df: pd.DataFrame, cols_to_rename: dict[str, str]
) -> pd.DataFrame:
    df = df.rename(columns=cols_to_rename, errors="raise")
    type_rapport = ["Principal", "Rattaché"]
    df["rapport_annuel_attendu"] = df["type_rapport"].isin(type_rapport)
    df["hors_corpus"] = True
    # Convertir toutes les dates

    return df


def process_organisme(df: pd.DataFrame, cols_to_rename: dict[str, str]) -> pd.DataFrame:
    # Process columns
    df = df.rename(columns=cols_to_rename, errors="raise")
    df = df.drop(columns=list(set(df.columns) - set(cols_to_rename.values())))

    type_rapport = ["Principal", "Rattaché"]
    df["rapport_annuel_attendu"] = df["type_rapport"].isin(type_rapport)
    df["hors_corpus"] = False

    # Convertir toutes les dates
    date_cols = [
        "date_creation_organisme",
        "date_modification_organisme",
        "date_archivage",
        "date_de_fin",
    ]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(df[date_col], format="%d/%m/%Y", errors="coerce")

    # Remplacer tous les Oui/Non en True/False
    cols_repl_values = [
        "publication_afs",
        "afs_fiche_rh_detaillee",
        "cartographie_attendue",
        "efc_attendu",
        "operateur_etat",
        "organisme_sensible",
    ]
    df[cols_repl_values] = df[cols_repl_values].replace({"Oui": True, "Non": False})
    return df


def process_cartographie(
    df: pd.DataFrame, df_orga_merge: pd.DataFrame, cols_to_rename: dict[str, str]
) -> pd.DataFrame:
    # Processing columns
    df = df.rename(columns=cols_to_rename, errors="raise")
    df = df.drop(columns=list(set(df.columns) - set(cols_to_rename.values())))
    df["siren"] = df["siren"].astype("float64")

    # Merge
    df = pd.merge(
        left=df_orga_merge[["denomination", "sigle", "cartographie_attendue"]],
        right=df,
        how="left",
        on=["denomination", "sigle"],
    )

    df["cartographie_attendue"] = np.where(
        df["cartographie_attendue"] == 1, True, False
    )
    df["cartographie_recue"] = np.where(df["cartographie_recue"] == 1, True, False)

    return df


def process_efc(
    df: pd.DataFrame, df_orga_merge: pd.DataFrame, cols_to_rename: dict[str, str]
) -> pd.DataFrame:
    # Processing columns
    df = df.rename(columns=cols_to_rename, errors="raise")
    df = df.drop(columns=list(set(df.columns) - set(cols_to_rename.values())))
    df["siren"] = df["siren"].astype("float64")

    # Merge
    df = pd.merge(
        left=df_orga_merge[["denomination", "sigle", "efc_attendu"]],
        right=df,
        how="left",
        on=["denomination", "sigle"],
    )

    df["efc_attendu"] = np.where(df["efc_attendu"] == 1, True, False)
    df["efc_recu"] = df["date_efc_recu"].notna()
    return df


def process_fiches_signaletiques(
    df: pd.DataFrame, cols_to_rename: dict[str, str]
) -> pd.DataFrame:
    # Process columns
    df = df.rename(columns=cols_to_rename, errors="raise")
    df = df.drop(columns=list(set(df.columns) - set(cols_to_rename.values())))

    # Fill all numeric values null values with 0 to allow comparaison
    cols_to_fill = list(set(df.columns) - set(["siren"]))
    df[cols_to_fill] = df[cols_to_fill].fillna(0)
    df = calculer_fiches_signaletiques(df=df)
    return df


def process_rapport_annuel(
    df: pd.DataFrame,
    df_orga_merge: pd.DataFrame,
    df_date_retour_attendu: pd.DataFrame,
    cols_to_rename: dict[str, str],
) -> pd.DataFrame:
    # Process columns
    df = df.rename(columns=cols_to_rename, errors="raise")
    df = df.drop(columns=list(set(df.columns) - set(cols_to_rename.values())))
    # Convertir columns
    df["siren"] = df["siren"].astype("float64")
    date_cols = ["date_du_retour"]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(df[date_col], format="%d/%m/%Y", errors="coerce")
    df["rapport_annuel_recu"] = df["date_du_retour"].notna()

    # Merge
    df = pd.merge(
        left=df_orga_merge[
            ["denomination", "sigle", "type_rapport", "rapport_annuel_attendu"]
        ],
        right=df,
        how="left",
        on=["denomination", "sigle"],
    )
    df = pd.merge(
        left=df,
        right=df_date_retour_attendu[
            ["denomination", "sigle", "date_du_retour_attendue"]
        ],
        how="left",
        on=["denomination", "sigle"],
    )

    return df


def process_date_retour_attendue(
    df: pd.DataFrame, cols_to_rename: dict[str, str]
) -> pd.DataFrame:
    # Process columns
    df = df.rename(columns=cols_to_rename, errors="raise")
    df = df.drop(columns=list(set(df.columns) - set(cols_to_rename.values())))
    # Convertir columns
    df["siren"] = df["siren"].astype("float64")
    date_cols = ["date_du_retour_attendue"]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(df[date_col], format="%d/%m/%Y", errors="coerce")
    return df


def process_recommandation(
    df: pd.DataFrame, cols_to_rename: dict[str, str]
) -> pd.DataFrame:
    # Process colnames
    df = df.rename(columns=cols_to_rename, errors="raise")
    df = df.drop(columns=list(set(df.columns) - set(cols_to_rename.values())))

    # Convertir date
    date_cols = [
        "date_creation",
        "date_cible",
        "date_de_modification",
        "date_de_changement_etat",
    ]
    for date_col in date_cols:
        df[date_col] = pd.to_datetime(df[date_col], format="%d/%m/%Y")

    # Convertir taux_avancement en float entre 0 et 1
    df["taux_avancement"] = (
        df["taux_avancement"]
        .astype(str)
        .str.strip()
        .str.replace("%", "", regex=False)
        .replace({"": None, "nan": None, "N/A": None})
        .astype(float)
        / 100
    )

    # Gestion des colonnes textuelles - cellule empty string
    str_cols = [
        "rubrique_de_rattachement",
        "objet_recommandation",
        "support_recommandation",
        "destinataire",
        "suite_et_commentaire",
        "finalite",
        "type_de_recommandation",
        "importance_recommandation",
    ]
    for str_col in str_cols:
        df[str_col] = df[str_col].str.split().str.join(" ")
    return df


def calculer_fiches_signaletiques(df: pd.DataFrame) -> pd.DataFrame:
    # Traitement colonne "Fiche RH" du barometre
    df["fiche_rh"] = np.where(df["effectifs_totaux"] > 0, True, False)

    # Traitement colonne "Bilan" du barometre
    df["bilan"] = np.where(
        (df["immobilise_net"] > 0)
        | (df["circulant_hors_tresorerie"] > 0)
        | (df["tresorerie"] > 0),
        True,
        False,
    )

    # Traitement colonne "Compte de résultat" du barometre
    df["compte_de_resultat"] = np.where(
        (df["charges_de_personnels_cpte_64"] > 0)
        | (df["charges_de_fonctionnement_externe"] > 0)
        | (df["impots_et_taxes"] > 0)
        | (df["interventions"] > 0)
        | (df["dotations_amortissement_provisions"] > 0),
        True,
        False,
    )

    # Traitement colonne "Fiche financière" du barometre
    df["fiche_financiere"] = np.where(
        (df["fonds_de_roulement"] > 0)
        | (df["en_nb_de_jours_charges_brutes_exploitation"] > 0)
        | (df["besoin_en_fonds_de_roulement"] > 0)
        | (df["duree_expression_tresorerie_nette_en_jours"] > 0)
        | (df["variation_des_emplois_stables"] > 0)
        | (df["variation_des_ressources_stables"] > 0),
        True,
        False,
    )

    # Traitement colonne "Fiche signalétique" du barometre
    df["fiche_signaletique"] = np.where(
        (df["fiche_rh"])
        | (df["bilan"])
        | (df["compte_de_resultat"])
        | (df["fiche_financiere"]),
        True,
        False,
    )

    return df


def add_rapport_hors_corpus(
    df_barometre: pd.DataFrame, df_hors_corpus: pd.DataFrame
) -> pd.DataFrame:
    df_barometre["hors_corpus"] = False
    df_hors_corpus = df_hors_corpus.reindex(columns=df_barometre.columns)
    tmp_df = pd.concat([df_barometre, df_hors_corpus], axis=0, ignore_index=True)
    cols_to_fill = [
        "carto",
        "efc",
        "recos",
        "fiche_rh",
        "bilan",
        "compte_de_resultat",
        "fiche_financiere",
        "fiche_signaletique",
    ]
    tmp_df[cols_to_fill] = tmp_df[cols_to_fill].fillna(0)

    return tmp_df
