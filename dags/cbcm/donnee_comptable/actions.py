import logging

import pandas as pd
from airflow.sdk import Variable
from modules.constants import (
    AGENT,
    DEFAULT_GRIST_HOST,
    PROXY,
)
from modules.enums.database import DatabaseType
from modules.infra.database.factory import DbConfig, create_db_handler
from modules.infra.grist.client import GristClient
from modules.infra.http_client.adapters import RequestsClient
from modules.infra.http_client.config import ClientConfig


def load_new_sp(
    df_demande_achat: pd.DataFrame,
    df_engagement_juridique: pd.DataFrame,
    df_demande_paiement_journal_pieces: pd.DataFrame,
    df_delai_global_paiement: pd.DataFrame,
) -> None:
    # Concaténer tous les CF et CC
    cols_to_keep = ["centre_financier", "centre_cout"]
    df_source = pd.concat(
        objs=[
            df_demande_achat,
            df_engagement_juridique,
            df_demande_paiement_journal_pieces,
            df_delai_global_paiement,
        ]
    )

    # Supprimer les doublons
    df_source = df_source.loc[:, cols_to_keep]
    df_source = df_source.drop_duplicates(subset=cols_to_keep)  # type: ignore

    # Récupérer les SP déjà connus
    db = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(),
    )
    df_sp = db.fetch_df(query="""SELECT DISTINCT centre_financier, centre_cout
            FROM donnee_comptable.service_prescripteur;""")
    logging.info(msg=f"Nombre de SP connus: {len(df_sp)}")

    # Réaliser une jointure
    df = pd.merge(left=df_sp, right=df_source, on=cols_to_keep, how="outer", indicator=True)

    # Conserver uniquement les lignes sans SP
    df = df.loc[
        (df["_merge"] == "right_only"),
        cols_to_keep,
    ]

    # Intégrer ces lignes dans Grist
    new_cf_cc = df.rename(
        columns={
            "centre_cout": "Centre_de_cout",
            "centre_financier": "Centre_financier",
        }
    ).to_dict(orient="records")
    logging.info(msg=f"Nouveau couple CF-CC sans SP: {len(new_cf_cc)}")

    if len(new_cf_cc) > 0:
        logging.info(msg="Ajout des nouveaux couples CF-CC dans Grist")
        data = {"records": [{"fields": record} for record in new_cf_cc]}

        logging.info(msg=f"Exemple: {data['records'][0]}")

        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
        request_client = RequestsClient(config=http_config)
        grist_client = GristClient(
            http_client=request_client,
            grist_host=DEFAULT_GRIST_HOST,
            api_token=Variable.get(key="grist_secret_key"),
        )
        try:
            doc_id = Variable.get(key="grist_doc_id_cbcm")
            if doc_id is None:
                raise ValueError("doc_id is None. Please check the configuration.")
            grist_client.update_records(doc_id=doc_id, table_id="Service_prescripteur", json=data)
        except Exception:
            logging.info(msg="Les nouveaux couples à ajouter existent déjà dans Grist !!")
    else:
        logging.info(msg="Aucun nouveau couple CF-CC ... Skipping")


def get_sp() -> pd.DataFrame:
    # Récupérer les SP déjà connus
    db_handler = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(),
    )
    df = db_handler.fetch_df(query="""SELECT dcsp.couple_cf_cc as cf_cc, dcrspc.service_prescripteur
            FROM donnee_comptable.service_prescripteur dcsp
            LEFT JOIN donnee_comptable.ref_service_prescripteur_choisi dcrspc
                ON dcsp.service_prescripteur_choisi_selon_cf_cc = dcrspc.id;""")

    return df
