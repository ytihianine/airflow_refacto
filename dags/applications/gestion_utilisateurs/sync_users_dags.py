# from datetime import timedelta
# from airflow.decorators import dag, task_group
# from airflow.providers.common.sql.hooks.sql import DbApiHook
# from airflow.models.baseoperator import chain
# from airflow.utils.dates import days_ago

# from utils.api_client.base import AbstractApiClient

# from dags.applications.gestion_utilisateurs.tasks import (
#     get_user_infos,
#     create_user_on_chartsgouv,
#     create_user_on_depot_fichier,
#     update_user_info_grist,
#     get_chartsgouv_access_token,
#     get_depot_fichier_access_token,
# )

# from dags.applications.gestion_utilisateurs.config import (
#     DEPOT_FICHIER_DB_HOOK,
#     CHARTSGOUV_DB_HOOK,
#     httpx_rie_client,
#     httpx_internet_client,
#     chartsgouv_admin,
#     chartsgouv_admin_pwd,
#     depot_fichier_admin,
#     depot_fichier_admin_pwd,
# )


# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": days_ago(1),
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 0,
#     "retry_delay": timedelta(seconds=20),
#     "pool": "grist_api",
# }


# @dag(
#     default_args=default_args,
#     schedule_interval="*/15 9-19 * * 1-5",
#     catchup=False,
#     dag_id="gestion_utilisateurs",
#     description="Permet de créer automatiquement des utilisateurs à partir de la liste définie dans Grist",
# )
# def gestion_utilisateurs():
#     @task_group()
#     def utilisateurs(
#         apiclient: AbstractApiClient, db_hook: DbApiHook, user_info: dict[str, any]
#     ) -> None:
#         chain(
#             [
#                 create_user_on_chartsgouv(api_client=apiclient, user_info=user_info),
#                 create_user_on_depot_fichier(
#                     api_client=apiclient, db_hook=db_hook, user_info=user_info
#                 ),
#             ]
#         )

#     users_info = get_user_infos(api_client=httpx_internet_client)

#     chain(
#         [
#             get_chartsgouv_access_token(
#                 api_client=httpx_rie_client,
#                 username=chartsgouv_admin,
#                 password=chartsgouv_admin_pwd,
#             ),
#             get_depot_fichier_access_token(
#                 api_client=httpx_rie_client,
#                 username=depot_fichier_admin,
#                 password=depot_fichier_admin_pwd,
#             ),
#         ],
#         utilisateurs.partial(
#             apiclient=httpx_rie_client, db_hook=DEPOT_FICHIER_DB_HOOK
#         ).expand(user_info=users_info),
#         update_user_info_grist.partial(
#             api_client=httpx_internet_client,
#             db_hook_chartsgouv=CHARTSGOUV_DB_HOOK,
#             db_hook_depot_fichier=DEPOT_FICHIER_DB_HOOK,
#         ).expand(user_info=users_info),
#     )


# gestion_utilisateurs()
