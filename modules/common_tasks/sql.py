"""SQL task utilities using infrastructure handlers."""

import logging
import textwrap
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from airflow.sdk import get_current_context, task

from modules.constants import (
    DEFAULT_PG_DATA_CONN_ID,
    DEFAULT_S3_CONN_ID,
    DEFAULT_TMP_SCHEMA,
)
from modules.enums.dags import FeatureFlags
from modules.enums.database import (
    LoadStrategy,
    PartitionTimePeriod,
)
from modules.enums.filesystem import FileHandlerType
from modules.infra.database.base import DBInterface
from modules.infra.database.factory import create_db_handler
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.factory import (
    FSConfig,
    create_file_handler,
)
from modules.types.projet import (
    SelecteurConfig,
    SelecteurStorageOptions,
)
from modules.utils.config.dag_params import (
    get_db_info,
    get_execution_date,
    get_project_name,
    should_skip_task,
)
from modules.utils.config.tasks import (
    get_list_selecteur_storage_info,
    get_projet_metadata,
    merge_selecteur_config,
)
from modules.utils.process.structures import are_lists_egal

# ------------------------------------------------------------------------------
# Internal functions
# ------------------------------------------------------------------------------


def _get_primary_keys(schema: str, table: str, db_handler: DBInterface) -> list[str]:
    """Get primary key columns of a table."""
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
                AND tc.constraint_schema = kcu.constraint_schema
        WHERE tc.table_schema = %s
            AND tc.table_name = %s
            AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position;
    """
    df = db_handler.fetch_df(query, parameters=(schema, table))
    return df.loc[:, "column_name"].tolist()


def _get_table_columns(schema: str, table: str, db_handler: DBInterface) -> list[str]:
    df = db_handler.fetch_df(
        query="""
            SELECT isc.table_catalog, isc.table_schema, isc.table_name, isc.column_name
            FROM information_schema.columns isc
            WHERE
                isc.table_schema = %s
                AND isc.table_name = %s
                AND isc.column_default IS NULL
                AND isc.is_identity = 'NO'
            ORDER BY table_schema ASC, table_name ASC, column_name ASC;
        """,
        parameters=(schema, table),
    )
    return df.loc[:, "column_name"].tolist()


def _create_snapshot_id(nom_projet: str, execution_date: datetime, nom_projet_parent: str | None = None) -> None:
    """
    Créer un snapshot_id pour un projet donné et l'insérer dans la table conf_projets.projet_snapshot.
    Une insertion est également faite dans la table versioning.snapshot_id.
    Actuellement dans une phase de migration. L'insertion dans conf_projets.projet_snapshot sera supprimée à terme
    """
    # Init vars
    snapshot_id = uuid4()
    snapshot_id_parent = None
    import_timestamp = execution_date.replace(tzinfo=None)
    import_date = execution_date.date()

    # Init hook
    db_client = create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)

    # Get project id
    id_projet_result = db_client.fetch_one(
        query="SELECT id_projet FROM conf_projets.projet WHERE projet = %(nom_projet)s;",
        parameters={"nom_projet": nom_projet},
    )
    if id_projet_result is None:
        raise ValueError(f"No project found with name {nom_projet}")

    id_projet = id_projet_result.get("id_projet")
    if id_projet is None:
        raise ValueError(f"No id_projet found for project {nom_projet}")

    # Get parent snapshot_id
    if nom_projet_parent is not None:
        snapshot_id_parent = _get_snapshot_id(nom_projet=nom_projet_parent, db_handler=db_client, dag_completed=True)

    query = """
        INSERT INTO versioning.snapshot (id_projet, snapshot_id, snapshot_id_parent, import_timestamp, import_date)
        VALUES (%(id_projet)s, %(snapshot_id)s, %(snapshot_id_parent)s, %(import_timestamp)s, %(import_date)s);
    """
    params = {
        "id_projet": id_projet,
        "snapshot_id": snapshot_id,
        "snapshot_id_parent": snapshot_id_parent,
        "import_timestamp": import_timestamp,
        "import_date": import_date,
    }
    # Exécution de la requête
    db_client.execute(query, parameters=params)


def _get_snapshot_id(nom_projet: str, db_handler: DBInterface, dag_completed: bool = False) -> UUID:
    """
    Get the latest completed snapshot for a project.
    """

    query = """
        SELECT s.snapshot_id
        FROM versioning.snapshot s
        JOIN conf_projets.projet p
            ON p.id_projet = s.id_projet
        WHERE p.projet = %(nom_projet)s
          AND s.is_dag_completed = %(is_dag_completed)s
        ORDER BY s.import_timestamp DESC
        LIMIT 1;
    """

    params = {"nom_projet": nom_projet, "is_dag_completed": dag_completed}

    db_result = db_handler.fetch_one(
        query,
        parameters=params,
    )

    if db_result is None:
        raise ValueError(f"No completed snapshot found for project {nom_projet}")

    snapshot_id = db_result.get("snapshot_id")

    if snapshot_id is None:
        raise ValueError(f"No snapshot_id found for project {nom_projet}")

    return snapshot_id


def determine_partition_period(time_period: PartitionTimePeriod, execution_date: datetime) -> tuple[datetime, datetime]:
    """Determine the start and end dates for a partition based on the time period."""
    if time_period == PartitionTimePeriod.YEAR:
        from_date_period = execution_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        to_date_period = from_date_period.replace(year=from_date_period.year + 1)
    elif time_period == PartitionTimePeriod.MONTH:
        from_date_period = execution_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if from_date_period.month == 12:
            to_date_period = from_date_period.replace(year=from_date_period.year + 1, month=1)
        else:
            to_date_period = from_date_period.replace(month=from_date_period.month + 1)
    elif time_period == PartitionTimePeriod.WEEK:
        from_date_period = execution_date - timedelta(days=execution_date.weekday())
        from_date_period = from_date_period.replace(hour=0, minute=0, second=0, microsecond=0)
        to_date_period = from_date_period + timedelta(weeks=1)
    elif time_period == PartitionTimePeriod.DAY:
        from_date_period = execution_date.replace(hour=0, minute=0, second=0, microsecond=0)
        to_date_period = from_date_period + timedelta(days=1)
    else:
        raise ValueError(f"Unsupported time period: {time_period}")
    return (from_date_period, to_date_period)


# ------------------------------------------------------------------------------
# SQL tasks
# ------------------------------------------------------------------------------
@task
def create_projet_snapshot(
    nom_projet_parent: str | None = None, pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID, **context
) -> None:
    """ """
    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    nom_projet = get_project_name(context=context)
    execution_date = get_execution_date(context=context)

    # Hook
    # db_client = create_db_handler(connection_id=pg_conn_id)

    _create_snapshot_id(nom_projet=nom_projet, execution_date=execution_date, nom_projet_parent=nom_projet_parent)


@task
def update_projet_snapshot_status(
    nom_projet: str | None = None,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    **context,
) -> None:
    """
    Lorsque le DAG est complété, mettre à jour le statut du snapshot_id du projet à TRUE
    dans la table versioning.snapshot_id.

    Args:
        nom_projet (optionnel): Le nom du projet. A spécifier lorsque le nom du projet
            dans le DAG est différent de celui qui génère le snapshot_id,
        pg_conn_id: Connexion Postgres. Valeur par défaut

    Returns:
        None.
    """
    if nom_projet is None:
        nom_projet = get_project_name(context=context)

    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    # Hook
    db_client = create_db_handler(connection_id=pg_conn_id)

    projet_metadata = get_projet_metadata(nom_projet=nom_projet)

    # Update is_dag_completed to True for the snapshot_id
    query = """
        UPDATE versioning.snapshot
        SET is_dag_completed = TRUE
        WHERE id_projet = %(id_projet)s
        AND snapshot_id = %(snapshot_id)s;
    """
    params = {
        "id_projet": projet_metadata.id_projet,
        "snapshot_id": projet_metadata.snapshot_id,
    }

    db_client.execute(query, parameters=params)
    logging.info(msg="Updated latest snapshot_id. Set is_dag_completed to True")


@task(map_index_template="{{ import_task_name }}")
def ensure_partition(
    selecteur_config: Mapping[str, Any],
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    **context,
) -> None:
    """
    Vérifie si une partition mensuelle existe pour une table partitionnée par date.
    Si elle n'existe pas, la créer.

    Args:
        pg_conn_id: Connexion Postgres
        partition_column: Colonne de partition (par défaut 'import_date')

    Returns:
        Le nom de la partition (créée ou existante)
    """
    # Init selecteur_config to SelecteurConfig if it's a dict
    config = SelecteurConfig.from_dict(data=selecteur_config)

    context = get_current_context()
    context["import_task_name"] = config.storage_info.selecteur  # type: ignore

    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    tbl_name = config.storage_info.tbl_name
    is_partitioned = config.storage_options.is_partitioned
    partition_period = config.storage_options.partition_period

    if config.storage_options.write_to_db is False:
        logging.info(msg=f"write_to_db is set to False for selecteur {config.storage_info.selecteur} ... skipping")
        return

    if tbl_name is None or tbl_name == "":
        logging.warning(
            msg=f"No table name specified for selecteur {config.storage_info.selecteur} ... skipping partition creation"
        )
        return

    if not is_partitioned:
        logging.info(msg=f"{tbl_name} is not partitioned ... skipping")
        return

    execution_date = get_execution_date(context=context)
    db_info = get_db_info(context=context)
    prod_schema = db_info.prod_schema

    # Hook
    db = create_db_handler(connection_id=pg_conn_id)

    # Get partition period range
    from_date, to_date = determine_partition_period(
        time_period=partition_period,
        execution_date=execution_date,
    )

    # Nom de la partition : parenttable_YYYY_MM
    partition_name = f"{tbl_name}_{from_date.strftime(format='%Y%m%d')}_{to_date.strftime(format='%Y%m%d')}"

    try:
        logging.info(msg=f"Creating partition {partition_name} for {tbl_name}.")
        # Créer la partition
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {prod_schema}.{partition_name}
            PARTITION OF {prod_schema}.{tbl_name}
            FOR VALUES FROM
                ('{from_date.strftime(format="%Y-%m-%d")}') TO ('{to_date.strftime(format="%Y-%m-%d")}');
        """
        db.execute(query=create_sql)
        logging.info(msg=f"Partition {partition_name} created successfully.")
    except Exception as e:
        logging.error(msg=f"Error creating partition {partition_name}: {e!s}")
        raise


@task(task_id="create_tmp_tables")
def create_tmp_tables(
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    storage_options: Mapping[str, SelecteurStorageOptions] | None = None,
    reset_id_seq: bool = False,
    **context,
) -> None:
    """
    Used to create temporary tables in the database.
    """
    nom_projet = get_project_name(context=context)

    if should_skip_task(context=context):
        return

    db_info = get_db_info(context=context)
    prod_schema = db_info.prod_schema
    tmp_schema = db_info.tmp_schema
    print(prod_schema, tmp_schema)

    # Hook
    db = create_db_handler(connection_id=pg_conn_id)

    # Get selecteur config
    storage_info = get_list_selecteur_storage_info(nom_projet=nom_projet)
    selecteur_config = merge_selecteur_config(storage_info=storage_info, storage_options=storage_options)

    drop_queries = []
    create_queries = []
    alter_queries = []

    for config in selecteur_config:
        if not config.should_write_to_db():
            logging.info(msg=f"Skipping DB tmp table creation for selecteur <{config.storage_info.selecteur}>")
            continue

        tbl_name = config.storage_info.tbl_name

        drop_queries.append(f"DROP TABLE IF EXISTS {tmp_schema}.tmp_{tbl_name};")
        create_queries.append(f"""CREATE TABLE
                IF NOT EXISTS {tmp_schema}.tmp_{tbl_name}
                ( LIKE {prod_schema}.{tbl_name} INCLUDING ALL);
            """)
        alter_queries.append(f"ALTER SEQUENCE {prod_schema}.{tbl_name}_id_seq RESTART WITH 1;")

    for drop_query in drop_queries:
        db.execute(query=drop_query)

    for create_query in create_queries:
        db.execute(query=create_query)
    if reset_id_seq:
        for alter_query in alter_queries:
            db.execute(query=alter_query)


@task(task_id="delete_tmp_tables")
def delete_tmp_tables(
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    storage_options: Mapping[str, SelecteurStorageOptions] | None = None,
    **context,
) -> None:
    """
    Used to delete temporary tables in the database.
    """
    nom_projet = get_project_name(context=context)

    db_info = get_db_info(context=context)
    tmp_schema = db_info.tmp_schema

    # Get selecteur config
    storage_info = get_list_selecteur_storage_info(nom_projet=nom_projet)
    selecteur_config = merge_selecteur_config(storage_info=storage_info, storage_options=storage_options)

    # Hook
    db = create_db_handler(connection_id=pg_conn_id)

    for config in selecteur_config:
        if not config.should_write_to_db():
            logging.info(msg=f"Skipping DB tmp table deletion for selecteur <{config.storage_info.selecteur}>")
            continue

        tbl_name = config.storage_info.tbl_name

        db.execute(query=f"DROP TABLE IF EXISTS {tmp_schema}.tmp_{tbl_name};")


def _generate_copy_query(
    selecteur_config: list[SelecteurConfig],
    db_handler: DBInterface,
    prod_schema: str = DEFAULT_TMP_SCHEMA,
    tmp_schema: str = DEFAULT_TMP_SCHEMA,
    merge_delete: bool = False,
) -> list[str]:
    """
    Generate SQL query to copy data from temporary table to production table based on the specified strategy.

    Args:
        tbl_name: Name of the table.
        primary_keys: List of primary key columns.
        strategy: Load strategy (FULL_LOAD, INCREMENTAL, APPEND).
        prod_schema: Production schema name.
        tmp_schema: Temporary schema name.
    """
    queries = []
    for config in selecteur_config:
        if not config.should_write_to_db():
            logging.info(msg=f"Skipping DB copy to real table for selecteur <{config.storage_info.selecteur}>")
            continue

        load_strategy = config.storage_options.load_strategy
        tbl_name = config.storage_info.tbl_name
        assert tbl_name is not None  # guaranteed by should_write_to_db()
        prod_table = f"{prod_schema}.{tbl_name}"
        tmp_table = f"{tmp_schema}.tmp_{tbl_name}"

        col_list = sort_db_colnames(
            db_handler=db_handler,
            selecteur_config=config,
            schema=prod_schema,
        )
        cols = ", ".join(col_list)

        if load_strategy == LoadStrategy.APPEND:
            insert_query = f"INSERT INTO {prod_table} ({cols}) SELECT {cols} FROM {tmp_table};"
            queries.append(insert_query)

        if load_strategy == LoadStrategy.FULL_LOAD:
            del_query = f"DELETE FROM {prod_table};"
            insert_query = f"INSERT INTO {prod_table} ({cols}) SELECT {cols} FROM {tmp_table};"
            queries.append(del_query)
            queries.append(insert_query)

        if load_strategy == LoadStrategy.INCREMENTAL:
            pk_cols = _get_primary_keys(schema=prod_schema, table=tbl_name, db_handler=db_handler)
            logging.info(msg=f"Table <{tbl_name}> primary key: {pk_cols}")

            merge_query = f"""
                MERGE INTO {prod_table} tbl_target
                USING {tmp_table} tbl_source ON ({' AND '.join([f'tbl_source.{col} = tbl_target.{col}' for col in pk_cols])})
                WHEN MATCHED THEN
                    UPDATE SET {", ".join([f"{col}=tbl_source.{col}" for col in col_list if col not in pk_cols])}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(col_list)})
                        VALUES ({', '.join([f'tbl_source.{col}' for col in col_list])})
            """

            if merge_delete:
                merge_query += """
                    WHEN NOT MATCHED BY SOURCE THEN
                        DELETE
                    ;
                """

            queries.append(merge_query)

    return queries


@task(task_id="copy_tmp_table_to_real_table")
def copy_tmp_table_to_real_table(
    storage_options: Mapping[str, SelecteurStorageOptions] | None = None,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    merge_delete: bool = False,
    **context,
) -> None:
    """
    Permet de copier les tables temporaires dans les tables réelles.

    strategy:
        FULL_LOAD      -> delete all prod rows, insert everything from tmp
        INCREMENTAL    -> UPSERT + delete missing rows based on primary key
        APPEND    -> ADD all rows from tmp to prod
    """
    nom_projet = get_project_name(context=context)

    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    db_info = get_db_info(context=context)
    prod_schema = db_info.prod_schema
    tmp_schema = db_info.tmp_schema

    # Hook
    db_handler = create_db_handler(connection_id=pg_conn_id)

    # Get selecteur config
    storage_info = get_list_selecteur_storage_info(nom_projet=nom_projet)
    selecteur_config = merge_selecteur_config(storage_info=storage_info, storage_options=storage_options)
    # Sort by tbl_order to handle foreign key dependencies
    selecteur_config = sorted(selecteur_config, key=lambda x: x.storage_options.tbl_order)
    logging.info(msg=f"Nombre de tables à copier: {len(selecteur_config)}")

    queries = _generate_copy_query(
        selecteur_config=selecteur_config,
        db_handler=db_handler,
        prod_schema=prod_schema,
        tmp_schema=tmp_schema,
        merge_delete=merge_delete,
    )

    if len(queries) == 0:
        logging.info(msg="No query to execute")
        return

    for q in queries:
        try:
            db_handler.execute(query=q)
        except Exception as e:
            logging.error(msg=f"Failed to execute query: {q}")
            raise e


def sort_db_colnames(
    db_handler: DBInterface,
    selecteur_config: SelecteurConfig,
    schema: str = DEFAULT_TMP_SCHEMA,
) -> list[str]:
    """Get sorted column names from a table.

    Args:
        selecteur_config: Selecteur configuration
        schema: Schema name

    Returns:
        Sorted list of column names
    """
    tbl_name = selecteur_config.storage_info.tbl_name
    pg_conn_id = selecteur_config.storage_options.db_conn_id

    if tbl_name is None or tbl_name == "":
        return []

    tbl_cols = _get_table_columns(schema=schema, table=tbl_name, db_handler=db_handler)

    sorted_cols = sorted(tbl_cols)
    logging.info(msg=f"Sorted columns for > {pg_conn_id} - {schema}.{tbl_name}: {sorted_cols}")
    return sorted_cols


def bulk_load_local_tsv_file_to_db(
    local_filepath: str,
    tbl_name: str,
    column_names: list[str],
    db_handler: DBInterface,
    schema: str = DEFAULT_TMP_SCHEMA,
) -> None:
    """Bulk load TSV file into database using COPY.

    Args:
        local_filepath: Path to local TSV file
        tbl_name: Target table name
        column_names: List of column names in order
        schema: Target schema
    """
    logging.info(msg=f"Bulk importing {local_filepath} to {schema}.tmp_{tbl_name}")

    copy_sql = f"""
        COPY {schema}.tmp_{tbl_name} ({", ".join(column_names)})
        FROM STDIN WITH (
            FORMAT TEXT,
            DELIMITER E'\t',
            HEADER TRUE,
            NULL 'NULL'
        )
    """

    db_handler.copy_expert(
        sql=copy_sql,
        filepath=local_filepath,
    )
    logging.info(msg=f"Successfully loaded {local_filepath} into {schema}.tmp_{tbl_name}")


@task(map_index_template="{{ import_task_name }}")
def import_file_to_db(
    selecteur_config: Mapping[str, Any],
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    **context,
) -> None:
    # Init selecteur_config to SelecteurConfig if it's a dict
    config = SelecteurConfig.from_dict(data=selecteur_config)
    selecteur = config.storage_info.selecteur

    db_info = get_db_info(context=context)
    context = get_current_context()
    context["import_task_name"] = config.storage_info.selecteur  # type: ignore

    if should_skip_task(context=context, feature_flag=FeatureFlags.DB):
        return

    if config.storage_options.write_to_db is False:
        logging.info(msg=f"write_to_db option is set to False for selecteur <{selecteur}>. Skipping import to db ...")
        return

    schema = db_info.prod_schema if config.storage_options.use_prod_schema else db_info.tmp_schema

    # Define hooks
    db_handler = create_db_handler(connection_id=pg_conn_id)
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(connection_id=s3_conn_id),
    )
    local_handler = create_file_handler(
        handler_type=FileHandlerType.LOCAL,
        config=FSConfig(base_path=None),
    )

    # Variables
    tbl_name = config.storage_info.tbl_name

    if tbl_name is None or tbl_name == "":
        logging.info(msg=f"tbl_name is None for selecteur <{selecteur}>. Nothing to import to db")
    else:
        # Variables
        local_filepath = config.storage_info.get_local_path()
        s3_filepath = config.storage_info.get_full_s3_key(with_tmp_segment=True)

        # Check if old file exists
        local_handler.delete(file_path=local_filepath)

        # Read data from s3, sort its columns and save it locally
        logging.info(msg=f"Reading file from remote < {s3_filepath} >")
        df = read_dataframe(file_handler=s3_handler, file_path=s3_filepath)

        sorted_df_cols = sorted(df.columns)
        df = df.reindex(labels=sorted_df_cols, axis=1).convert_dtypes()
        logging.info(msg=f"DF : {sorted_df_cols}")
        logging.info(msg=f"Saving file to local < {local_filepath} >")
        local_handler.write(
            file_path=local_filepath,
            content=df.to_csv(index=False, sep="\t", na_rep="NULL"),
        )

        # Check if columns are the same between df and db table
        sorted_db_colnames = sort_db_colnames(
            db_handler=db_handler,
            selecteur_config=config,
            schema=schema,
        )
        if not are_lists_egal(list_A=sorted_df_cols, list_B=sorted_db_colnames):
            raise ValueError(textwrap.dedent(text="""
                Il y a des différences entre les colonnes du DataFrame et de la Table.
                Impossible d'importer les données.
            """))

        # Bulk load file to db
        bulk_load_local_tsv_file_to_db(
            local_filepath=local_filepath,
            tbl_name=tbl_name,
            column_names=sorted_db_colnames,
            db_handler=db_handler,
        )

        # Clean up local file
        local_handler.delete(file_path=local_filepath)


@task
def refresh_views(pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID, **context) -> None:
    """Tâche pour actualiser les vues matérialisées"""
    if should_skip_task(context=context):
        return

    db_info = get_db_info(context=context)
    prod_schema = db_info.prod_schema

    db = create_db_handler(connection_id=pg_conn_id)

    get_mview_query = """
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = %s;
    """

    views = db.fetch_df(query=get_mview_query, parameters=(prod_schema,))["matviewname"].tolist()

    if len(views) == 0:
        logging.info(msg=f"No materialized views found for schema {prod_schema}. Skipping ...")
    else:
        sql_queries = [f"REFRESH MATERIALIZED VIEW {prod_schema}.{view_name};" for view_name in views]
        for query in sql_queries:
            db.execute(query=query)
