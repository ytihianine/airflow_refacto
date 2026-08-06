import pandas as pd
from airflow.sdk import Variable
from modules.constants import (
    AGENT,
    DEFAULT_GRIST_HOST,
    DEFAULT_PG_DATA_CONN_ID,
    PROXY,
)
from modules.infra.database.factory import create_db_handler
from modules.infra.grist.client import GristClient
from modules.infra.http_client.adapters import RequestsClient
from modules.infra.http_client.config import ClientConfig


def get_all_cf_cc() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(query="""
            select
                distinct
                centre_financier,
                centre_cout,
                import_timestamp
            from
                donnee_comptable.demande_achat
            where
                centre_financier <> 'Ind'
                and centre_cout <> 'Ind'
            union
            select
                distinct
                centre_financier,
                centre_cout,
                import_timestamp
            from
                donnee_comptable.engagement_juridique
            where
                centre_financier <> 'Ind'
                and centre_cout <> 'Ind'
            union
            select
                distinct
                centre_financier,
                centre_cout,
                import_timestamp
            from
                donnee_comptable.delai_global_paiement
            where
                centre_financier <> 'Ind'
                and centre_cout <> 'Ind'
            union
            select
                distinct
                centre_financier,
                centre_cout,
                import_timestamp
            from
                donnee_comptable.demande_paiement_complet
            where
                centre_financier <> 'Ind'
                and centre_cout <> 'Ind'
            ;
        """)

    return df


def get_demande_achat() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(query="""
            SELECT
                id_da,
                centre_financier,
                centre_cout,
                unique_multi,
                import_timestamp
            FROM donnee_comptable.demande_achat
            GROUP BY
                id_da, centre_financier, centre_cout, unique_multi, import_timestamp
            ;
        """)

    return df


def get_demande_paiement_complet() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(query="""
            select
                id_dp,
                centre_financier,
                centre_cout,
                unique_multi,
                texte_de_poste,
                import_timestamp
            from
                donnee_comptable.demande_paiement_complet
            group by
                id_dp,
                centre_financier,
                centre_cout,
                unique_multi,
                texte_de_poste,
                import_timestamp
            ;
        """)

    return df


def get_delai_global_paiement() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(query="""
            select
                id_dgp::text,
                centre_financier,
                centre_cout,
                annee_exercice,
                societe,
                type_piece,
                import_timestamp
            from
                donnee_comptable.delai_global_paiement
            group by
                id_dgp,
                centre_financier,
                centre_cout,
                annee_exercice,
                societe,
                type_piece,
                import_timestamp
            ;
        """)

    return df


def get_engagement_juridique() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)
    df = db_handler.fetch_df(query="""
            select
                id_ej,
                centre_financier,
                centre_cout,
                orga,
                gac,
                import_timestamp
            from
                donnee_comptable.engagement_juridique
            group by
                id_ej,
                centre_financier,
                centre_cout,
                orga,
                gac,
                import_timestamp
            ;
        """)

    return df


# ========================================
# Envoyer les nouvelles données sur Grist
# ========================================
def load_new_cf_cc(df_get_all_cf_cc: pd.DataFrame, df_sp: pd.DataFrame) -> None:
    # Filtrer les lignes
    df_get_all_cf_cc = df_get_all_cf_cc.loc[
        (df_get_all_cf_cc["centre_financier"] != "Ind")
        & (df_get_all_cf_cc["centre_cout"] != "Ind")
        & (df_get_all_cf_cc["centre_cout"].str.upper() != "TECHNIQUE")
    ]

    # Merge pour comparer
    on_cols = ["centre_financier", "centre_cout"]
    df = pd.merge(
        left=df_get_all_cf_cc,
        right=df_sp[on_cols],
        how="left",
        on=on_cols,
        indicator=True,
    )
    df["import_timestamp"] = df["import_timestamp"].astype("string")

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only", [*on_cols, "import_timestamp"]]

    # Intégrer ces lignes dans Grist
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    request_client = RequestsClient(config=http_config)
    grist_client = GristClient(
        http_client=request_client,
        grist_host=DEFAULT_GRIST_HOST,
        api_token=Variable.get(key="grist_secret_key"),
    )
    doc_id = Variable.get(key="grist_doc_id_cbcm")
    if doc_id is None:
        raise ValueError("doc_id is None. Please check the configuration.")
    grist_client.send_dataframe_to_grist(
        df=df,
        doc_id=doc_id,
        table_id="Service_prescripteur",
        rename_columns={
            "centre_cout": "Centre_de_cout",
            "centre_financier": "Centre_financier",
        },
    )


def load_demande_achat(df_get_demande_achat: pd.DataFrame, df_demande_achat_sp_manuel: pd.DataFrame) -> None:
    # Filtrer les lignes
    df_get_demande_achat = df_get_demande_achat.loc[
        (df_get_demande_achat["unique_multi"] == "Multiple")
        | (df_get_demande_achat["centre_financier"] == "Ind")
        | (df_get_demande_achat["centre_cout"].isin(["Ind", "TECHNIQUE"]))
    ]

    # Merge pour comparer
    df = pd.merge(
        left=df_get_demande_achat,
        right=df_demande_achat_sp_manuel["id_da"],
        how="left",
        on=["id_da"],
        indicator=True,
    )
    df["import_timestamp"] = df["import_timestamp"].astype("string")

    # Suppression des doublons
    df = df.drop_duplicates(subset=["id_da"])
    # CF & CC = None si Multiple
    mask = df["unique_multi"] == "Multiple"
    df.loc[mask, ["centre_financier", "centre_cout"]] = None

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]
    df = df.drop(columns=["_merge"])

    # Les envoyers dans Grist
    print(df.columns)
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    request_client = RequestsClient(config=http_config)
    grist_client = GristClient(
        http_client=request_client,
        grist_host=DEFAULT_GRIST_HOST,
        api_token=Variable.get(key="grist_secret_key"),
    )
    doc_id = Variable.get(key="grist_doc_id_cbcm")
    if doc_id is None:
        raise ValueError("doc_id is None. Please check the configuration.")
    grist_client.send_dataframe_to_grist(
        df=df,
        doc_id=doc_id,
        table_id="Demande_achat_sp_manuel",
        rename_columns={
            "id_da": "id_da",
            "centre_financier": "Centre_financier",
            "centre_cout": "Centre_de_cout",
        },
    )


def load_demande_paiement_complet(
    df_get_demande_paiement_complet: pd.DataFrame,
    df_demande_paiement_sp_manuel: pd.DataFrame,
) -> None:
    # Filtrer les lignes
    df_get_demande_paiement_complet = df_get_demande_paiement_complet.loc[
        (df_get_demande_paiement_complet["centre_financier"] == "Ind")
        | (df_get_demande_paiement_complet["centre_cout"].isin(["Ind", "TECHNIQUE"]))
        | (df_get_demande_paiement_complet["unique_multi"] == "Multiple")
    ]

    # Merge pour comparer
    df = pd.merge(
        left=df_get_demande_paiement_complet,
        right=df_demande_paiement_sp_manuel["id_dp"],
        how="left",
        on=["id_dp"],
        indicator=True,
    )
    df["import_timestamp"] = df["import_timestamp"].astype("string")
    df = df.drop_duplicates(subset=["id_dp"])

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]
    df = df.drop(columns=["_merge"])
    # CF & CC = None si Multiple
    mask = df["unique_multi"] == "Multiple"
    df.loc[mask, ["centre_financier", "centre_cout"]] = None

    # Intégrer ces lignes dans Grist
    print(df.columns)
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    request_client = RequestsClient(config=http_config)
    grist_client = GristClient(
        http_client=request_client,
        grist_host=DEFAULT_GRIST_HOST,
        api_token=Variable.get(key="grist_secret_key"),
    )
    doc_id = Variable.get(key="grist_doc_id_cbcm")
    if doc_id is None:
        raise ValueError("doc_id is None. Please check the configuration.")
    grist_client.send_dataframe_to_grist(
        df=df,
        doc_id=doc_id,
        table_id="Demande_paiement_sp_manuel",
        rename_columns={
            "id_da": "id_da",
            "centre_financier": "Centre_financier",
            "centre_cout": "Centre_de_cout",
            "unique_multi": "Unique_multiple",
            "texte_de_poste": "Texte_de_poste",
        },
    )


def load_delai_global_paiement(
    df_get_delai_global_paiement: pd.DataFrame,
    df_delai_global_paiement_sp_manuel: pd.DataFrame,
) -> None:
    # Filtrer les lignes
    df_get_delai_global_paiement = df_get_delai_global_paiement.loc[
        (df_get_delai_global_paiement["centre_financier"] == "Ind")
        | (df_get_delai_global_paiement["centre_cout"].isin(["Ind", "TECHNIQUE"]))
    ]

    # Merge pour comparer
    df = pd.merge(
        left=df_get_delai_global_paiement,
        right=df_delai_global_paiement_sp_manuel["id_dgp"],
        how="left",
        on=["id_dgp"],
        indicator=True,
    )
    df["import_timestamp"] = df["import_timestamp"].astype("string")

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]
    df = df.drop(columns=["_merge"])

    # Intégrer ces lignes dans Grist
    print(df.columns)
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    request_client = RequestsClient(config=http_config)
    grist_client = GristClient(
        http_client=request_client,
        grist_host=DEFAULT_GRIST_HOST,
        api_token=Variable.get(key="grist_secret_key"),
    )
    doc_id = Variable.get(key="grist_doc_id_cbcm")
    if doc_id is None:
        raise ValueError("doc_id is None. Please check the configuration.")
    grist_client.send_dataframe_to_grist(
        df=df,
        doc_id=doc_id,
        table_id="Delai_global_paiement_sp_manuel",
        rename_columns={
            "id_da": "id_da",
            "centre_financier": "Centre_financier",
            "centre_cout": "Centre_de_cout",
            "annee_exercice": "Annee_exercice",
            "societe": "Societe",
            "type_piece": "Type_piece",
        },
    )


def load_engagement_juridique(
    df_get_engagement_juridique: pd.DataFrame,
    df_engagement_juridique_sp_manuel: pd.DataFrame,
) -> None:
    # Filtrer les lignes
    df_get_engagement_juridique = df_get_engagement_juridique.loc[
        (df_get_engagement_juridique["centre_financier"] == "Ind")
        | (df_get_engagement_juridique["centre_cout"].isin(["Ind", "TECHNIQUE"]))
    ]

    # Merge pour comparer
    df = pd.merge(
        left=df_get_engagement_juridique,
        right=df_engagement_juridique_sp_manuel["id_ej"],
        how="left",
        on=["id_ej"],
        indicator=True,
    )
    df["import_timestamp"] = df["import_timestamp"].astype("string")

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]
    df = df.drop(columns=["_merge"])

    # Intégrer ces lignes dans Grist
    print(df.columns)
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    request_client = RequestsClient(config=http_config)
    grist_client = GristClient(
        http_client=request_client,
        grist_host=DEFAULT_GRIST_HOST,
        api_token=Variable.get(key="grist_secret_key"),
    )
    doc_id = Variable.get(key="grist_doc_id_cbcm")
    if doc_id is None:
        raise ValueError("doc_id is None. Please check the configuration.")
    grist_client.send_dataframe_to_grist(
        df=df,
        doc_id=doc_id,
        table_id="Engagement_juridique_sp_manuel",
        rename_columns={
            "id_da": "id_da",
            "centre_financier": "Centre_financier",
            "centre_cout": "Centre_de_cout",
            "annee_exercice": "Annee_exercice",
            "orga": "Orga",
            "gac": "gac",
        },
    )
