from typing import cast
import pandas as pd

from infra.http_client.adapters import HttpxClient, ClientConfig
from utils.config.vars import AGENT, PROXY
from infra.database.factory import create_db_handler
from infra.database.postgres import PostgresDBHandler

from dags.sg.siep.mmsi.eligibilite_fcu.process import (
    get_eligibilite_fcu,
    process_result,
)
from utils.dataframe import df_info


def eligibilite_fcu():
    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    httpx_internet_client = HttpxClient(client_config)

    # Hooks
    db_hook = cast(
        PostgresDBHandler, create_db_handler(connection_id="db_depose_fichier")
    )

    # Storage paths

    df_oad = db_hook.fetch_df(
        query="""SELECT sp.code_bat_ter, sbl.latitude, sbl.longitude
            FROM siep.bien sp
            JOIN siep.bien_localisation sbl
                ON sp.code_bat_ter = sbl.code_bat_ter;
        """
    )

    api_host = "https://france-chaleur-urbaine.beta.gouv.fr"
    api_endpoint = "api/v1/eligibility"
    url = "/".join([api_host, api_endpoint])

    api_results = []
    nb_rows = len(df_oad)
    for row in df_oad.itertuples():
        print(f"{row.Index}/{nb_rows}")
        api_result = get_eligibilite_fcu(
            api_client=httpx_internet_client,
            url=url,
            latitude=row.latitude,
            longitude=row.longitude,
        )
        api_result["code_bat_ter"] = row.code_bat_ter
        api_results.append(api_result)
        print(api_result)

    df_result = pd.DataFrame(api_results)
    df_result = process_result(df_result)
    df_info(df=df_result, df_name="Result API - After processing")

    return df_result
