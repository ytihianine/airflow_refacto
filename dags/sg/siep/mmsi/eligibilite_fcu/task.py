from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


from utils.config.tasks import get_storage_rows
from utils.dataframe import df_info

from dags.sg.siep.mmsi.eligibilite_fcu.process import (
    get_eligibilite_fcu,
    process_result,
)


@task(task_id="eligibilite_fcu")
def eligibilite_fcu(nom_projet: str, selecteur: str = "fcu"):
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

    df_oad = db_hook.get_pandas_df(
        sql="""SELECT sp.code_bat_ter, sbl.latitude, sbl.longitude
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

    # Export
    s3_hook.load_bytes(
        bytes_data=df_result.to_parquet(path=None, index=False),
        key=storage_paths.loc[
            storage_paths["selecteur"] == selecteur, "filepath_tmp_s3"
        ]
        + ".parquet",
        replace=True,
    )
