from collections.abc import Mapping
from typing import Any

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
from modules.utils.config.dag_params import get_db_info, get_project_name
from modules.utils.config.tasks import get_selecteur_storage_info


def get_agent_db(context: Mapping[str, Any]) -> pd.DataFrame:
    schema = get_db_info(context=context).prod_schema

    # Hook
    db_handler = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(),
    )

    # Retrieve data
    df = db_handler.fetch_df(query=f"""
            select
                cra.matricule_agent,
                cra.nom_usuel,
                cra.prenom,
                cra.genre,
                cra.age,
                cracr.qualite_statutaire,
                cracr.dge_perimetre
            from
                {schema}.agent cra
            inner join {schema}.agent_carriere cracr
            on cra.matricule_agent = cracr.matricule_agent
            ;
        """)

    return df


def load_agent(
    df_get_agent_db: pd.DataFrame,
    df_agent: pd.DataFrame,
    grist_doc_selecteur: str,
    context: Mapping[str, Any],
) -> None:
    # Get Grist doc_id
    nom_projet = get_project_name(context=context)
    grist_doc_info = get_selecteur_storage_info(nom_projet=nom_projet, selecteur=grist_doc_selecteur)

    # Merge pour comparer
    df = pd.merge(
        left=df_get_agent_db,
        right=df_agent["matricule_agent"],
        how="left",
        on=["matricule_agent"],
        indicator=True,
    )

    # Conserver uniquement les nouvelles
    df = df.loc[df["_merge"] == "left_only"]
    df = df.drop(columns=["_merge", "genre", "age"])

    # Intégrer ces lignes dans Grist
    print(df.columns)
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    request_client = RequestsClient(config=http_config)
    grist_client = GristClient(
        http_client=request_client,
        grist_host=DEFAULT_GRIST_HOST,
        api_token=Variable.get(key="grist_secret_key"),
    )
    doc_id = grist_doc_info.id_source
    if doc_id is None:
        raise ValueError(
            f"doc_id is None for selecteur {grist_doc_selecteur} in project {nom_projet}. Please check the configuration."
        )
    grist_client.send_dataframe_to_grist(
        df=df,
        doc_id=doc_id,
        table_id="Agent",
        rename_columns={
            "matricule_agent": "Matricule_agent",
            "nom_usuel": "Nom_usuel",
            "prenom": "Prenom",
            "qualite_statutaire": "Qualite_statutaire",
        },
    )
