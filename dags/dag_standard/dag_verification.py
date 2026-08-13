from collections.abc import Mapping
from pprint import pprint
from typing import Any

from airflow.sdk import Variable, dag, get_current_context, task
from airflow.sdk.bases.operator import chain
from dags.dag_standard.config import storage_options
from modules.common_tasks.projet import config_projet_group, get_selecteur_config
from modules.common_tasks.s3 import del_iceberg_staging_table
from modules.constants import (
    DEFAULT_POLARIS_CATALOG,
    DEFAULT_POLARIS_HOST,
    DEFAULT_S3_CONN_ID,
    DEFAULT_TRINO_HOST,
)
from modules.enums.dags import DagStatus
from modules.enums.filesystem import IcebergTableStatus
from modules.infra.catalog.iceberg import IcebergCatalog, generate_catalog_properties
from modules.infra.file_system.factory import create_default_s3_handler
from modules.infra.mails.default_smtp import (
    MailMessage,
    MailStatus,
    _callback,
    send_mail,
)
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.types.projet import SelecteurConfig
from modules.utils.config.dag_params import (
    create_dag_params,
    create_default_args,
    get_db_info,
)
from modules.utils.config.tasks import get_list_source_fichier

nom_projet = "Configuration des projets"


# Définition du DAG
@dag(
    dag_id="dag_verification",
    schedule=None,
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "Vérification"],
    description="Dag de vérification.",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="iceberg"),
        feature_flags=FeatureFlagsEnable(
            db=True,
            mail=True,
            s3=True,
            convert_files=True,
            download_grist_doc=True,
        ),
    ),
)
def dag_verification() -> None:
    @task
    def print_context(**context) -> None:
        pprint(object=context)
        pprint(object=context["dag"].__dict__)
        pprint(object=context["ti"].__dict__)
        pprint(object=context["ti"].xcom_pull(key="snapshot_id", task_ids="get_projet_snapshot"))

    @task
    def send_simple_mail(**context) -> None:
        mail_message = MailMessage(
            to=["yanis.tihianine@finances.gouv.fr"],
            cc=["yanis.tihianine@finances.gouv.fr"],
            subject="Simple test depuis Airflow",
            html_content="Test réussi !",
        )
        send_mail(mail_message=mail_message)

    @task
    def send_error_mail(**context) -> None:
        _callback(context=context, mail_status=MailStatus.ERROR)

    @task
    def send_success_mail(**context) -> None:
        _callback(context=context, mail_status=MailStatus.SUCCESS)

    @task
    def check_s3_hook() -> None:
        s3_hook = create_default_s3_handler(connection_id=DEFAULT_S3_CONN_ID)
        keys = s3_hook.list_files(directory="data_store/test_namespace/")
        print(keys)
        print(len(keys))
        keys_with_pattern = s3_hook.list_files(directory="data_store/test_namespace/", pattern="*_staging*")
        print(keys_with_pattern)
        print(len(keys_with_pattern))

        s3_hook.delete_single(file_path="data_store/test_namespace/test_table_staging/")

    @task
    def check_trino_hook() -> None:
        from modules.infra.database.trino import TrinoAdapter

        trino_user = Variable.get(key="TRINO_USER")

        trino_handler = TrinoAdapter(
            host=DEFAULT_TRINO_HOST,
            user=trino_user,
            catalog=DEFAULT_POLARIS_CATALOG,
            port=443,
            http_scheme="https",
            verify=False,
        )
        df = trino_handler.fetch_df(query='SELECT * FROM "infrastructure.configuration.projet".direction')
        print(df.head())

    @task
    def iceberg_task(**context) -> None:
        import pandas as pd

        properties = generate_catalog_properties(
            uri=DEFAULT_POLARIS_HOST,
        )
        catalog = IcebergCatalog(name="data_store", properties=properties)

        df = pd.DataFrame(data={"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        db_schema = get_db_info(context=context).prod_schema
        key = "key/sub_key/my_table_test"
        key_split = key.split("/")
        tbl_name = key_split.pop(-1)
        namespace = ".".join([db_schema, *key_split])
        catalog.write_table_and_namespace(
            df=df,
            table_status=IcebergTableStatus.PROD,
            namespace=namespace,
            table_name=tbl_name,
        )

    @task
    def check_liste_source_fichier() -> None:
        lst_fichiers = get_list_source_fichier(nom_projet="Cartographie rémunération")
        print(lst_fichiers)

    selecteur_configs = get_selecteur_config(nom_projet=nom_projet, storage_options=storage_options)

    @task(map_index_template="{{ import_task_name }}")
    def print_selecteur_config(
        selecteur_config: Mapping[str, Any],
        **context,
    ) -> None:
        # Init selecteur_config to SelecteurConfig if it's a dict
        print(f"Received selecteur_config: {selecteur_config}")
        print(type(selecteur_config))
        config = SelecteurConfig.from_dict(data=selecteur_config)
        print(f"Selecteur config: {config.storage_info.selecteur}")

        context = get_current_context()
        context["import_task_name"] = config.storage_info.selecteur  # type: ignore

    # Ordre des tâches
    chain(
        selecteur_configs,
        [
            print_context(),
            send_simple_mail(),
            send_error_mail(),
            send_success_mail(),
            config_projet_group(nom_projet=nom_projet, storage_options=storage_options),
            # import_files_to_db(
            #     nom_projet=nom_projet, storage_options=storage_options
            # ),
            iceberg_task(),
            check_liste_source_fichier(),
            check_s3_hook(),
            check_trino_hook(),
            del_iceberg_staging_table(),
            print_selecteur_config.expand(selecteur_config=selecteur_configs),
        ],
    )


dag_verification()
