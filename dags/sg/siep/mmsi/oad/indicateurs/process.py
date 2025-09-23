from datetime import datetime
from typing import cast
import pandas as pd

from infra.database.postgres import PostgresDBHandler
from utils.config.vars import DEFAULT_PG_DATA_CONN_ID
from infra.database.factory import create_db_handler


def filter_bien(df: pd.DataFrame, df_bien: pd.DataFrame) -> pd.DataFrame:
    # Conserver uniquement les biens présents dans la table bien
    df = pd.merge(
        left=df_bien["code_bat_ter"], right=df, on="code_bat_ter", how="inner"
    )

    return df


def process_oad_indic(df: pd.DataFrame) -> pd.DataFrame:
    db_handler = cast(PostgresDBHandler, create_db_handler(DEFAULT_PG_DATA_CONN_ID))
    df = df.dropna(subset=["code_bat_ter"])
    df = df.drop_duplicates(subset=["code_bat_ter"], ignore_index=True)

    # Removing biens which are not presents in table bien
    biens = db_handler.fetch_df(query="SELECT code_bat_ter FROM siep.bien;")
    biens = biens.loc[:, "code_bat_ter"].to_list()
    df = df[df["code_bat_ter"].isin(biens)]
    return df


def process_accessibilite(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.assign(
            date_mise_en_accessibilite=pd.to_datetime(
                df["date_mise_en_accessibilite"], dayfirst=True
            ),
            motif_derogation=df["motif_derogation"].str.split().str.join(" "),
        )
        .replace({"Oui": True, "Non": False})
        .convert_dtypes()
    )

    return df


def process_accessibilite_detail(df: pd.DataFrame) -> pd.DataFrame:
    def get_niveau_fonctionnel(niveau: str) -> str:
        if niveau is None:
            return None

        correspondance = {
            "NR.F": "Oui",
            "Ne sait pas": "Ne sait pas",
            "R.F": "Oui",
            "R.NF": "Non",
            "Sans objet": "Sans objet",
        }
        niveau = niveau.strip()

        return correspondance.get(niveau, "NC")

    def get_niveau_reglementaire(niveau: str) -> str:
        if niveau is None:
            return None

        correspondance = {
            "NR.F": "Non",
            "Ne sait pas": "Ne sait pas",
            "R.F": "Oui",
            "R.NF": "Oui",
            "Sans objet": "Sans objet",
        }
        niveau = niveau.strip()

        return correspondance.get(niveau, "NC")

    df = df.melt(
        id_vars=["code_bat_ter"],
        value_vars=[
            "accessibilite_acces",
            "accessibilite_accueil",
            "accessibilite_general",
            "accessibilite_sanitaire",
            "accessibilite_services",
        ],
        var_name="composant_bien",
        value_name="niveau",
    ).assign(
        composant_bien=lambda df: df["composant_bien"]
        .str.replace("accessibilite_", "")
        .str.title()
    )

    # Create calculated columns
    df["niveau_fonctionnel"] = list(map(get_niveau_fonctionnel, df["niveau"]))
    df["niveau_reglementaire"] = list(map(get_niveau_reglementaire, df["niveau"]))

    return df


def process_bacs(df: pd.DataFrame) -> pd.DataFrame:
    df = (
        df.assign(
            date_installation_gtb=pd.to_datetime(
                df["date_installation_gtb"], dayfirst=True
            ),
            date_derniere_inspection=pd.to_datetime(
                df["date_derniere_inspection"], dayfirst=True
            ),
            presence_gtb=df["presence_gtb"]
            .str.lower()
            .replace({"oui": True, "non": False}),
            split_decret_bacs=df["soumis_decret_bacs"].str.split(pat=" ", n=1),
            classe_gtb=df["classe_gtb"].str.upper(),
            commentaire_soumission=df["commentaire_soumission"]
            .str.split()
            .str.join(" "),
            commentaire_general=df["commentaire_general"].str.split().str.join(" "),
        )
        .assign(
            soumis_decret_bacs=lambda df: (
                df["split_decret_bacs"]
                .str[0]
                .str.lower()
                .replace({"oui": True, "non": False})
            ),
            raison_soumis_decret_bacs=lambda df: (
                df["split_decret_bacs"].str[1].str.replace("(", "").str.replace(")", "")
            ),
        )
        .drop(columns=["split_decret_bacs"])
    )

    return df


def process_bails(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        date_debut_bail=df["date_debut_bail"].apply(
            lambda x: datetime.strptime(x, "%d/%m/%Y") if isinstance(x, str) else pd.NaT
        ),
        date_fin_bail=df["date_fin_bail"].apply(
            lambda x: datetime.strptime(x, "%d/%m/%Y") if isinstance(x, str) else pd.NaT
        ),
        type_contrat=df["type_contrat"].str.split().str.join(" "),
    )

    return df


def process_couts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        annee_charges_copropriete=df["annee_charges_copropriete"].replace("-", None),
        annee_charges_locatives=df["annee_charges_locatives"].replace("-", None),
        annee_loyers_budgetaires=df["annee_loyers_budgetaires"].replace("-", None),
        annee1_charges_fonct=df["annee1_charges_fonct"].replace("-", None),
        annee1_loyer_ht_hc_ttc=df["annee1_loyer_ht_hc_ttc"].replace("-", None),
        annee2_charges_fonc=df["annee2_charges_fonc"].replace("-", None),
        annee2_loyer_ht_hc_ttc=df["annee2_loyer_ht_hc_ttc"].replace("-", None),
    )

    return df


def process_deet_energie(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        annee1_conso_ef_et_ges=df["annee1_conso_ef_et_ges"].replace("-", None),
        annee2_conso_ef_et_ges=df["annee2_conso_ef_et_ges"].replace("-", None),
        annee1_conso_eau=df["annee1_conso_eau"].replace("-", None),
        annee2_conso_eau=df["annee2_conso_eau"].replace("-", None),
        bat_assujettis_deet=df["bat_assujettis_deet"]
        .str.lower()
        .replace({"oui": True, "non": False}),
        deet_commentaire=df["bat_assujettis_deet"].str.split().str.join(" "),
    )

    return df


def process_eds(df: pd.DataFrame) -> pd.DataFrame:
    common_col = ["code_bat_ter"]

    # Traitement partie "théorique"
    theorique_cols = [
        colname for colname in df.columns if colname.endswith("theorique")
    ]
    df_theorique = df.loc[:, common_col + theorique_cols]
    df_theorique = df_theorique.melt(
        id_vars=common_col, var_name="composant_bien", value_name="eds_theorique"
    )
    df_theorique = df_theorique.assign(
        composant_bien=lambda df: (
            df["composant_bien"]
            .str.replace("_theorique", "")
            .str.replace("eds_", "")
            .str.replace("_", " ")
            .str.title()
        )
    )

    # Traitement partie "constaté"
    constate_cols = [
        colname
        for colname in df.columns
        if colname.endswith("constate") or colname.endswith("general")
    ]
    df_constate = df.loc[:, common_col + constate_cols]
    df_constate = df_constate.melt(
        id_vars=common_col, var_name="composant_bien", value_name="eds_constate"
    )
    df_constate = df_constate.assign(
        composant_bien=lambda df: (
            df["composant_bien"]
            .str.replace("_constate", "")
            .str.replace("eds_", "")
            .str.replace("_", " ")
            .str.title()
        )
    )

    # Join
    df = pd.merge(
        df_theorique, df_constate, on=["code_bat_ter", "composant_bien"], how="outer"
    )

    df = df.assign(
        composant_bien=lambda df: (
            df["composant_bien"]
            .str.replace("_constate", "")
            .str.replace("_theorique", "")
            .str.replace("eds_", "")
            .str.replace("_", " ")
            .str.title()
        )
    )

    return df


def process_exploitation(df: pd.DataFrame) -> pd.DataFrame:
    common_col = ["code_bat_ter"]

    # Traitement partie "théorique"
    commentaire_cols = [
        colname for colname in df.columns if colname.endswith("commentaire")
    ]
    df_commentaire = df.loc[:, common_col + commentaire_cols]
    df_commentaire = df_commentaire.melt(
        id_vars=common_col, var_name="composant_bien", value_name="commentaire"
    )
    df_commentaire = df_commentaire.assign(
        composant_bien=lambda df: (df["composant_bien"].str.replace("_commentaire", ""))
    )

    # Traitement partie "constaté"
    etat_cols = [colname for colname in df.columns if colname.endswith("etat")]
    df_etat = df.loc[:, common_col + etat_cols]
    df_etat = df_etat.melt(
        id_vars=common_col, var_name="composant_bien", value_name="etat_exploitation"
    )
    df_etat = df_etat.assign(
        composant_bien=lambda df: (df["composant_bien"].str.replace("_etat", ""))
    )

    # Join
    df = pd.merge(
        df_commentaire, df_etat, on=["code_bat_ter", "composant_bien"], how="outer"
    )
    df = df.assign(
        composant_bien=lambda df: (
            df["composant_bien"]
            .str.replace("_commentaire", "")
            .str.replace("_etat", "")
            .str.replace("exploitation_", "")
            .str.replace("_", "")
            .str.title()
        ),
        commentaire=df["commentaire"].str.split().str.join(" "),
    )

    return df


def process_localisation(
    df_oad_carac: pd.DataFrame, df_oad_indic: pd.DataFrame, df_biens: pd.DataFrame
) -> pd.DataFrame:
    # Merging data from both sources
    df = pd.merge(left=df_oad_carac, right=df_oad_indic, on="code_bat_ter", how="left")
    cols_to_keep = [
        "code_bat_ter",
        "latitude",
        "longitude",
        "adresse_normalisee",
        "adresse_source",
        "code_insee_normalise",
        "commune_mef_hmef",
        "commune_normalisee",
        "commune_source",
        "france_etranger",
        "num_departement_normalisee",
        "num_departement_source",
    ]
    df = df[cols_to_keep]

    # Cleaning data
    df = df.assign(
        adresse_source=df["adresse_source"].str.split().str.join(" "),
        commune_source=df["commune_source"].str.split().str.join(" "),
        num_departement_source=df["num_departement_source"].str.split().str.join(" "),
    )
    df = df.drop_duplicates(
        subset=["code_bat_ter"],
        keep="first",
        ignore_index=True,
    )

    df = filter_bien(df=df, df_bien=df_biens)
    return df


def process_notes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.assign(
        modernisation=df["modernisation"].str.replace(",", "."),
        optimisation=df["optimisation"].str.replace(",", "."),
        preservation=df["preservation"].str.replace(",", "."),
    )
    return df


def process_effectif(df: pd.DataFrame) -> pd.DataFrame:
    # Aucune actions nécessaire
    return df


def process_proprietaire(df: pd.DataFrame) -> pd.DataFrame:
    def get_locatif_domanial(statut_occupation: str) -> str:
        correspondance = {
            "L'Etat ne possède pas et n'occupe pas en tant que services de l'Etat": "Locatif",
            "L'Etat ne possède pas et occupe en tant que services de l'Etat": "Locatif",
            "L'Etat possède et n'occupe pas en tant que services de l'Etat": "Domanial",
            "L'Etat possède et occupe en tant que services de l'Etat": "Domanial",
        }
        statut_occupation = statut_occupation.strip()

        return correspondance.get(statut_occupation, "NC")

    # Create calculated column
    df["locatif_domanial"] = list(map(get_locatif_domanial, df["statut_occupation"]))

    return df


def process_reglementation(df: pd.DataFrame) -> pd.DataFrame:
    def get_reglementation_corrigee(reglementation: str) -> str:
        if reglementation is None:
            return None
        correspondance = {
            "ERP": "ERP",
            "Code du Travail (non ERP)": "Code du travail (non ERP)",
            "Code du travail (non ERP)": "Code du travail (non ERP)",
            "Autre": "Autre",
            "Bâtiment d'habitation": "Bâtiment d'habitation",
        }
        reglementation = reglementation.strip()

        return correspondance.get(reglementation, "NC")

    df = df.assign(
        igh=(df["igh"].str.lower().replace({"oui": True, "non": False})),
    )

    # Create calculated column
    df["reglementation_corrigee"] = list(
        map(get_reglementation_corrigee, df["reglementation"])
    )

    return df


def process_strategie(
    df_oad_carac: pd.DataFrame, df_oad_indic: pd.DataFrame, df_biens: pd.DataFrame
) -> pd.DataFrame:
    df = pd.merge(left=df_oad_carac, right=df_oad_indic, on="code_bat_ter", how="left")
    df = df.drop_duplicates(
        subset=["code_bat_ter"],
        keep="first",
        ignore_index=True,
    )
    cols_to_keep = [
        "code_bat_ter",
        "perimetre_spsi_initial",
        "perimetre_spsi_maj",
        "segmentation_sdir_spsi",
        "segmentation_theorique_sdir_spsi",
        "statut_osc",
    ]
    df = df[cols_to_keep]
    df = filter_bien(df=df, df_bien=df_biens)

    return df


def process_surface(df: pd.DataFrame) -> pd.DataFrame:
    # Aucune actions nécessaire
    return df


def process_valeur(df: pd.DataFrame) -> pd.DataFrame:
    # Aucune actions nécessaire
    return df


def process_typologie(df: pd.DataFrame) -> pd.DataFrame:
    # Aucune actions nécessaire
    return df
