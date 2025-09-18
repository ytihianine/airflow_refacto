# from datetime import datetime
# from airflow.decorators import task

# from utils.api_client.base import AbstractApiClient
# from airflow.providers.common.sql.hooks.sql import DbApiHook


# from utils.dataframe import df_info

# from dags.applications.gestion_utilisateurs.process import process_user_info

# from dags.applications.gestion_utilisateurs.config import grist_api, TBL_USER


# def get_token(
#     api_client: AbstractApiClient, url: str, username: str, password: str
# ) -> str:
#     response_login = api_client.post(
#         endpoint=url,
#         json={"username": username, "password": password, "provider": "db"},
#     )

#     if response_login.status_code == 200:
#         access_token = response_login.json()["access_token"]
#         return access_token

#     raise ValueError(
#         f"La requête n'est pas passée. Statut code: {response_login.status_code}"
#     )


# @task(task_id="get_user_infos")
# def get_user_infos(api_client: AbstractApiClient):
#     """
#     Cette tâche récupérer les infos des utilisateurs
#     dans Grist.
#     """
#     # Get data from Grist
#     df = grist_api.get_df_from_records(tbl_name=TBL_USER)

#     df = process_user_info(df=df)

#     df_info(df=df, df_name="DF User Grist - après traitement")

#     return df.to_dict("records")


# @task
# def get_chartsgouv_access_token(
#     api_client: AbstractApiClient, username: str, password: str
# ) -> str:
#     url_chartsgouv = "https://chartsgouv-mef-sg.lab.incubateur.finances.rie.gouv.fr"
#     endpoint_token = "/api/v1/security/login"

#     access_token = get_token(
#         api_client=api_client,
#         url=url_chartsgouv + endpoint_token,
#         username=username,
#         password=password,
#     )
#     return access_token


# @task
# def get_depot_fichier_access_token(
#     api_client: AbstractApiClient, username: str, password: str
# ) -> str:
#     url_depot_fichier = (
#         "https://depot-fichier-sg.lab.incubateur.finances.rie.gouv.fr/api"
#     )
#     endpoint_token = "/api/v1/security/login"

#     access_token = get_token(
#         api_client=api_client,
#         url=url_depot_fichier + endpoint_token,
#         username=username,
#         password=password,
#     )
#     return access_token


# @task(task_id="create_user_on_chartsgouv")
# def create_user_on_chartsgouv(
#     api_client: AbstractApiClient, user_info: dict[str, any], **context
# ):
#     """
#     Cette tâche permet de créer les utilisateurs
#     dans Superset et l'interface de dépôt de fichier.
#     """
#     url_chartsgouv = "https://chartsgouv-mef-sg.lab.incubateur.finances.rie.gouv.fr"
#     endpoint_users = "/api/v1/security/users/"

#     username = user_info.get("username")
#     access_token = context["ti"].xcom_pull(
#         key="return_value", task_ids="get_chartsgouv_access_token"
#     )

#     response = api_client.post(
#         endpoint=url_chartsgouv + endpoint_users,
#         json={
#             "active": True,
#             "email": user_info.get(
#                 "mail", "".join([username, "_default@finances.gouv.fr"])
#             ),
#             "first_name": user_info.get("prenom"),
#             "last_name": user_info.get("nom"),
#             "password": user_info.get("password"),
#             "roles": [0],
#             "username": user_info.get("username"),
#         },
#         headers={"Authorization": f"Bearer {access_token}"},
#     )

#     if response.status_code == 422:
#         print(f"User {username} already exists in Chartsgouv user list ! :) ")
#     else:
#         response.raise_for_status()


# @task(task_id="create_user_on_depot_fichier")
# def create_user_on_depot_fichier(
#     api_client: AbstractApiClient,
#     db_hook: DbApiHook,
#     user_info: dict[str, any],
#     **context,
# ):
#     """
#     Cette tâche permet de créer les utilisateurs
#     dans Superset et l'interface de dépôt de fichier.
#     """
#     url_depot_fichier = (
#         "https://depot-fichier-sg.lab.incubateur.finances.rie.gouv.fr/api"
#     )

#     endpoint_users = "/api/v1/security/users/"
#     user_has_access = user_info.get("Acces_depose_fichier", False)

#     if user_has_access:
#         username = user_info.get("username")
#         access_token = context["ti"].xcom_pull(
#             key="return_value", task_ids="get_depot_fichier_access_token"
#         )

#         response = api_client.post(
#             endpoint=url_depot_fichier + endpoint_users,
#             json={
#                 "active": True,
#                 "email": user_info.get("mail", "mail_default@finances.gouv.fr"),
#                 "first_name": user_info.get("prenom"),
#                 "last_name": user_info.get("nom"),
#                 "password": user_info.get("password"),
#                 "roles": [9],
#                 "username": username,
#             },
#             headers={"Authorization": f"Bearer {access_token}"},
#         )

#         if response.status_code == 422:
#             print(f"User {username} already exists in Depot Fichier user list ! :) ")
#         else:
#             response.raise_for_status()

#         db_hook.run(
#             sql="UPDATE public.ab_user SET id_direction=%(id_direction)s, id_service=%(id_service)s WHERE username=%(username)s",
#             parameters={
#                 "id_direction": user_info.get("direction", "Non-défini"),
#                 "id_service": user_info.get("service", "Non-défini"),
#                 "username": user_info.get("username"),
#             },
#         )
#     else:
#         print(f"User {user_info.get('username')} has no access to depot fichier app")


# @task(task_id="update_user_info_grist")
# def update_user_info_grist(
#     api_client: AbstractApiClient,
#     db_hook_chartsgouv: DbApiHook,
#     db_hook_depot_fichier: DbApiHook,
#     user_info: dict[str, any],
# ) -> None:
#     """
#     Cette tâche permet de créer les utilisateurs
#     dans Superset et l'interface de dépôt de fichier.
#     """
#     username = user_info.get("username")

#     user_chartsgouv = db_hook_chartsgouv.get_records(
#         sql="SELECT username, created_on, password, email from public.ab_user WHERE username=%(username)s",
#         parameters={"username": username},
#     )
#     user_depot_fichier = db_hook_depot_fichier.get_records(
#         sql="SELECT username, created_on, password from public.ab_user WHERE username=%(username)s",
#         parameters={"username": username},
#     )

#     if user_chartsgouv is None and user_depot_fichier is None:
#         raise ValueError(
#             f"No user found in both db chartsgouv config and depot fichier with username '{username}'"
#         )

#     user_info = user_chartsgouv[0]
#     print(user_info)

#     json_data = {
#         "records": [
#             {
#                 "require": {"username": user_info[0]},
#                 "fields": {
#                     "user_created": True,
#                     "date_creation": user_info[1].timestamp(),
#                     "password": user_info[2],
#                     "mail": user_info[3],
#                 },
#             }
#         ]
#     }

#     response = grist_api.put_records(
#         tbl_name=TBL_USER, json=json_data, query_params=["noadd=true"]
#     )
#     response.raise_for_status()
