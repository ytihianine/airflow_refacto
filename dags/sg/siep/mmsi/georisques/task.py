from airflow.decorators import task

from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.file_handler import MinioFileHandler


from utils.common.config_func import get_storage_rows

from utils.df_utility import df_info
from dags.sg.siep.mmsi.georisques.process import (
    get_risque,
    format_query_param,
    format_risque_results,
)


@task(task_id="georisques")
def georisques(nom_projet: str):
    import pandas as pd
    from utils.common.vars import PROXY, AGENT
    from utils.api_client.adapters import HttpxAPIClient

    # Http client
    httpx_internet_client = HttpxAPIClient(proxy=PROXY, user_agent=AGENT)

    # Hooks
    db_hook = PostgresHook(postgres_conn_id="db_data_store")
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Storage paths
    storage_paths = get_storage_rows(nom_projet=nom_projet)

    # Get data from AOD
    df_oad = db_hook.get_pandas_df(
        sql="""SELECT sp.code_bat_ter, sbl.latitude, sbl.longitude, sbl.adresse
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
    s3_hook.load_bytes(
        bytes_data=df_risques.to_parquet(path=None, index=False),
        key=storage_paths.loc[
            storage_paths["selecteur"] == "georisques", "filepath_tmp_s3"
        ]
        + ".parquet",
        replace=True,
    )
    # Export results - API Info
    s3_hook.load_bytes(
        bytes_data=df_risques_info.to_parquet(path=None, index=False),
        key=storage_paths.loc[
            storage_paths["selecteur"] == "georisques_api", "filepath_tmp_s3"
        ]
        + ".parquet",
        replace=True,
    )
