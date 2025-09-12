# from airflow.models import Variable
# from airflow.providers.postgres.hooks.postgres import PostgresHook

# from utils.api_client.adapters import HttpxAPIClient
# from utils.grist import GristAPI

# from utils.common.vars import PROXY, AGENT


# # Variables
# chartsgouv_admin = Variable.get("chartsgouv_admin_username")
# chartsgouv_admin_pwd = Variable.get("chartsgouv_admin_password")
# depot_fichier_admin = Variable.get("depot_fichier_admin_username")
# depot_fichier_admin_pwd = Variable.get("depot_fichier_admin_password")

# # API Info
# httpx_rie_client = HttpxAPIClient(verify=False)
# httpx_internet_client = HttpxAPIClient(proxy=PROXY, user_agent=AGENT)
# grist_api = GristAPI(
#     api_client=httpx_internet_client,
#     base_url="https://grist.numerique.gouv.fr",
#     workspace_id="dsci",
#     doc_id=Variable.get("grist_doc_id_gestion_interne"),
#     api_token=Variable.get("grist_secret_key"),
# )
# TBL_USER = "Utilisateurs"

# # Liens
# link_documentation_pipeline = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/dsci/depot_fichier?ref_type=heads"
# link_documentation_donnees = (
#     "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"
# )

# # Hooks
# DEPOT_FICHIER_DB_HOOK = PostgresHook(postgres_conn_id="db_depose_fichier")
# CHARTSGOUV_DB_HOOK = PostgresHook(postgres_conn_id="db_chartsgouv_config")
