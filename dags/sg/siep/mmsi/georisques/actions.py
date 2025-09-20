from typing import cast
import pandas as pd

from infra.database.factory import create_db_handler
from infra.database.postgres import PostgresDBHandler
from infra.file_handling.s3 import S3FileHandler
from infra.http_client.adapters import HttpxClient
from infra.http_client.config import ClientConfig
from utils.config.vars import AGENT, PROXY
from utils.config.tasks import get_selecteur_config
from utils.dataframe import df_info

from dags.sg.siep.mmsi.georisques.process import (
    get_risque,
    format_query_param,
    format_risque_results,
)


def get_georisques(
    nom_projet: str, selecteur_risques: str, selecteur_risques_info: str
) -> None:
    # Http client
    http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
    httpx_internet_client = HttpxClient(config=http_config)

    # Hooks
    db_handler = cast(PostgresDBHandler, create_db_handler("db_data_store"))
    s3_handler = S3FileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

    # Storage paths
    config_risques = get_selecteur_config(
        nom_projet=nom_projet, selecteur=selecteur_risques
    )
    config_risques_info = get_selecteur_config(
        nom_projet=nom_projet, selecteur=selecteur_risques_info
    )

    # Get data from AOD
    df_oad = db_handler.fetch_df(
        query="""SELECT sp.code_bat_ter, sbl.latitude, sbl.longitude, sbl.adresse
            FROM siep.bien sp
            JOIN siep.bien_localisation sbl
                ON sp.code_bat_ter = sbl.code_bat_ter
            ;
        """
    )

    # Get result from API
    api_host = "https://www.georisques.gouv.fr"
    api_endpoint = "api/v1/resultats_rapport_risque"
    url = "/".join([api_host, api_endpoint])

    risques_api_info = []
    risques_results = []
    nb_rows = len(df_oad)
    for row in df_oad.itertuples():
        print(f"{row.Index + 1}/{nb_rows}")
        query_param = format_query_param(
            adresse=row.adresse, latitude=row.latitude, longitude=row.longitude
        )
        risque_api_result = get_risque(
            api_client=httpx_internet_client, url=url, query_param=query_param
        )
        risque_api_result["code_bat_ter"] = row.code_bat_ter
        formated_risques = format_risque_results(risques=risque_api_result)
        # print(risque_api_result)
        # print(formated_risques)
        risques_api_info.append(
            {
                "code_bat_ter": risque_api_result["code_bat_ter"],
                "statut": risque_api_result["statut"],
                "statut_code": risque_api_result["statut_code"],
                "raison": risque_api_result["raison"],
            }
        )
        risques_results.extend(formated_risques)

    df_risques = pd.DataFrame(risques_results)
    df_risques_info = pd.DataFrame(risques_api_info)
    # Logs
    df_info(df=df_risques, df_name="Risque results - After processing")
    df_info(df=df_risques_info, df_name="Risques API Info - After processing")

    # Process results - Pas besoin
    # Export results - Risques
    s3_handler.write(
        content=df_risques.to_parquet(path=None, index=False),
        file_path=config_risques.filepath_tmp_s3,
    )
    # Export results - API Info
    s3_handler.write(
        content=df_risques_info.to_parquet(path=None, index=False),
        file_path=config_risques_info.filepath_tmp_s3,
    )
