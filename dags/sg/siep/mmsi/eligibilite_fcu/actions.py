import logging
from typing import Any

import pandas as pd
from dags.sg.siep.mmsi.eligibilite_fcu.process import (
    get_eligibilite_fcu,
)
from dags.sg.siep.mmsi.oad.config import nom_projet_oad
from modules.constants import AGENT, PROXY
from modules.enums.database import DatabaseType
from modules.enums.http import HttpHandlerType
from modules.infra.database.factory import DbConfig, create_db_handler
from modules.infra.http_client.adapters import ClientConfig
from modules.infra.http_client.factory import create_http_client
from modules.utils.config.tasks import get_projet_metadata


def eligibilite_fcu(context: dict[str, Any]) -> pd.DataFrame:
    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    http_internet_client = create_http_client(client_type=HttpHandlerType.REQUEST, config=client_config)

    # Hooks
    db_hook = create_db_handler(
        db_type=DatabaseType.POSTGRES,
        db_config=DbConfig(),
    )

    metadata = get_projet_metadata(nom_projet=nom_projet_oad, dag_completed=True)
    logging.info(msg=f"Snapshot ID récupéré : {metadata}")

    # Storage paths
    df_oad = db_hook.fetch_df(
        query="""
        SELECT
            sbl.code_bat_ter,
            sbl.latitude,
            sbl.longitude
        FROM siep.bien_localisation sbl
        WHERE sbl.snapshot_id = %s;
        """,
        parameters=(metadata.snapshot_id,),
    )

    if df_oad.empty:
        logging.warning(msg="Le DataFrame df_oad est vide. Fin du processus.")
        raise ValueError("Aucune données disponible dans df_oad.")

    api_host = "https://france-chaleur-urbaine.beta.gouv.fr"
    api_endpoint = "api/v1/eligibility"
    url = "/".join([api_host, api_endpoint])

    api_results = []
    nb_rows = len(df_oad)
    logging.info(msg=f"Nombre de bâtiments à traiter : {nb_rows}")
    for i, (code_bat_ter, latitude, longitude) in enumerate(
        df_oad[["code_bat_ter", "latitude", "longitude"]].itertuples(
            index=False,
            name=None,
        ),
        start=1,
    ):
        logging.info(msg=f"{i}/{nb_rows} - Appel API FCU")

        api_result = get_eligibilite_fcu(
            api_client=http_internet_client,
            url=url,
            latitude=latitude,
            longitude=longitude,
        )

        api_result["code_bat_ter"] = code_bat_ter
        api_results.append(api_result)

        logging.info(msg=api_result)

    df_result = pd.DataFrame(data=api_results)
    return df_result
