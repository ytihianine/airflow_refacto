"""SQL task utilities using infrastructure handlers."""

import logging
from typing import cast
from datetime import datetime, timedelta

from enum import Enum, auto
import psycopg2
from airflow.decorators import task
from airflow.operators.python import get_current_context

from infra.database.factory import create_db_handler
from infra.database.postgres import PostgresDBHandler

from infra.file_handling.dataframe import read_dataframe
from infra.file_handling.factory import FileHandlerFactory

from infra.file_handling.s3 import S3FileHandler
from utils.config.tasks import get_projet_config, get_tbl_names
from utils.config.types import SelecteurConfig
from utils.control.structures import are_lists_egal
from utils.config.vars import (
    DEFAULT_TMP_SCHEMA,
    DEFAULT_PG_DATA_CONN_ID,
    DEFAULT_PG_CONFIG_CONN_ID,
    DEFAULT_S3_CONN_ID,
    DEFAULT_S3_BUCKET,
)


CONF_SCHEMA = "conf_projets"


class PartitionTimePeriod(str, Enum):
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"


class LoadStrategy(Enum):
    FULL_LOAD = auto()
    INCREMENTAL = auto()
    APPEND = auto()


def get_primary_keys(
    schema: str, table: str, pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID
) -> list[str]:
    """Get primary key columns of a table."""
    db = cast(PostgresDBHandler, create_db_handler(pg_conn_id))
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
    df = db.fetch_df(query, (schema, table))
    return df["column_name"].tolist()


@task(task_id="get_tbl_names_from_postgresql")
def get_tbl_names_from_postgresql(**context) -> list[str]:
    params = context.get("params", {})
    nom_projet = params.get("nom_projet")
    if not nom_projet:
        raise ValueError("Project name must be provided in DAG parameters!")

    tbl_names = get_tbl_names(nom_projet=nom_projet)
    return tbl_names


def create_snapshot_id(
    nom_projet: str, execution_date: datetime, pg_conn_id: str
) -> None:
    import uuid

    snapshot_id = uuid.uuid4()
    query = """
        INSERT INTO conf_projets.projet_snapshot (id_projet, snapshot_id, creation_timestamp)
        SELECT
            p.id,
            %(snapshot_id)s,
            %(creation_timestamp)s
        FROM conf_projets.projet p
        WHERE p.nom_projet = %(nom_projet)s
        AND EXISTS (SELECT 1 FROM conf_projets.projet WHERE nom_projet = %(nom_projet)s);
    """

    # Paramètres pour la requête
    params = {
        "nom_projet": nom_projet,
        "snapshot_id": snapshot_id,
        "creation_timestamp": execution_date.naive(),
    }

    # Exécution de la requête
    db = create_db_handler(pg_conn_id)
    db.execute(query, params)


def get_snapshot_id(nom_projet: str, pg_conn_id: str) -> str:
    query = """
        SELECT psi.snapshot_id
        FROM conf_projets.projet_snapshot psi
        WHERE
            id_projet = (
                SELECT id
                FROM conf_projets.projet p
                WHERE p.nom_projet = %(nom_projet)s
            )
        AND psi.creation_timestamp = (
            SELECT MAX(creation_timestamp)
            FROM conf_projets.projet_snapshot
        );
    """

    # Paramètres pour la requête
    params = {"nom_projet": nom_projet}

    # Exécution de la requête
    db = create_db_handler(pg_conn_id)
    snapshot_id = db.fetch_all(query, params)
    return snapshot_id


@task
def create_projet_snapshot(
    pg_conn_id: str = DEFAULT_PG_CONFIG_CONN_ID, **context
) -> None:
    """ """
    params = context.get("params", {})
    nom_projet = params.get("nom_projet")
    if not nom_projet:
        raise ValueError("Project name must be provided in DAG parameters!")
    execution_date = context.get("execution_date")

    create_snapshot_id(
        nom_projet=nom_projet, execution_date=execution_date, pg_conn_id=pg_conn_id
    )


@task
def get_projet_snapshot(pg_conn_id: str = DEFAULT_PG_CONFIG_CONN_ID, **context) -> None:
    """
    Vérifie si une partition mensuelle existe pour une table partitionnée par date.
    Si elle n'existe pas, la crée.

    Args:
        pg_conn_id: Connexion Postgres. Valeur par défaut

    Returns:
        None. Ajoute le snapshot_id dans le context du DAG
    """
    params = context.get("params", {})
    nom_projet = params.get("nom_projet")
    if not nom_projet:
        raise ValueError("Project name must be provided in DAG parameters!")

    snapshot_id = get_snapshot_id(nom_projet=nom_projet, pg_conn_id=pg_conn_id)
    print(snapshot_id)
    print(f"Adding snapshot_id {snapshot_id} to context")
    context["ti"].xcom_push(key="snapshot_id", value=snapshot_id[0]["snapshot_id"])
    print("Snapshot_id added to context.")


def determine_partition_period(
    time_period: PartitionTimePeriod, execution_date: datetime
) -> tuple[datetime, datetime]:
    """Determine the start and end dates for a partition based on the time period."""
    if time_period == PartitionTimePeriod.YEAR:
        from_date_period = execution_date.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        to_date_period = from_date_period.replace(year=from_date_period.year + 1)
    elif time_period == PartitionTimePeriod.MONTH:
        from_date_period = execution_date.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        if from_date_period.month == 12:
            to_date_period = from_date_period.replace(
                year=from_date_period.year + 1, month=1
            )
        else:
            to_date_period = from_date_period.replace(month=from_date_period.month + 1)
    elif time_period == PartitionTimePeriod.WEEK:
        from_date_period = execution_date - timedelta(days=execution_date.weekday())
        from_date_period = from_date_period.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        to_date_period = from_date_period + timedelta(weeks=1)
    elif time_period == PartitionTimePeriod.DAY:
        from_date_period = execution_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        to_date_period = from_date_period + timedelta(days=1)
    else:
        raise ValueError(f"Unsupported time period: {time_period}")
    return (from_date_period, to_date_period)


@task
def ensure_partition(
    time_period: PartitionTimePeriod = PartitionTimePeriod.DAY,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    **context,
) -> None:
    """
    Vérifie si une partition mensuelle existe pour une table partitionnée par date.
    Si elle n'existe pas, la crée.

    Args:
        pg_conn_id: Connexion Postgres
        partition_column: Colonne de partition (par défaut 'import_date')

    Returns:
        Le nom de la partition (créée ou existante)
    """
    params = context.get("params", {})
    nom_projet = params.get("nom_projet")
    if not nom_projet:
        raise ValueError("Project name must be provided in DAG parameters!")

    db_info = params.get("db", {})
    prod_schema = db_info.get("prod_schema", None)

    if not prod_schema:
        raise ValueError("Database schema must be provided in DAG parameters!")

    # Récupérer les informations de la table parente
    tbl_names = get_tbl_names(nom_projet=nom_projet)

    db = create_db_handler(pg_conn_id)
    # Get timing information
    execution_date = context.get("execution_date")
    if not execution_date or not isinstance(execution_date, datetime):
        raise ValueError("Invalid execution date in Airflow context")

    # Get partition period range
    from_date, to_date = determine_partition_period(time_period, execution_date)

    for tbl in tbl_names:
        # Nom de la partition : parenttable_YYYY_MM
        partition_name = (
            f"{tbl}_{from_date.strftime('%Y%m%d')}_{to_date.strftime('%Y%m%d')}"
        )

        try:
            logging.info(f"Creating partition {partition_name} for {tbl}.")
            # Créer la partition
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {prod_schema}.{partition_name}
                PARTITION OF {prod_schema}.{tbl}
                FOR VALUES FROM ('{from_date}') TO ('{to_date}');
            """
            db.execute(create_sql)
            logging.info(f"Partition {partition_name} created successfully.")
        except psycopg2.errors.DuplicateTable:
            logging.info(
                f"Partition {partition_name} already exists. Skipping creation."
            )
        except Exception as e:
            logging.error(f"Error creating partition {partition_name}: {str(e)}")
            raise


@task(task_id="create_tmp_tables")
def create_tmp_tables(
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    reset_id_seq: bool = True,
    **context,
) -> None:
    """
    Used to create temporary tables in the database.
    """
    params = context.get("params", {})
    nom_projet = params.get("nom_projet")
    if not nom_projet:
        raise ValueError("Project name must be provided in DAG parameters!")

    db_info = params.get("db", {})
    prod_schema = db_info.get("prod_schema", None)
    tmp_schema = db_info.get("tmp_schema", None)

    if not prod_schema:
        raise ValueError("Database schema must be provided in DAG parameters!")
    if not tmp_schema:
        raise ValueError(
            "Temporary database schema must be provided in DAG parameters!"
        )

    # Hook
    db = create_db_handler(pg_conn_id)

    tbl_names = get_tbl_names(nom_projet=nom_projet)

    rows_result = db.fetch_all(
        query="""SELECT COUNT(*) as count_tmp_tables
            FROM information_schema.tables
            WHERE table_schema='temporaire'
                AND table_name LIKE 'tmp_%';
            """
    )

    drop_queries = []
    create_queries = []
    alter_queries = []

    for table in tbl_names:
        drop_queries.append(f"DROP TABLE IF EXISTS {tmp_schema}.tmp_{table};")
        create_queries.append(
            f"""CREATE TABLE
                IF NOT EXISTS {tmp_schema}.tmp_{table}
                ( LIKE {prod_schema}.{table} INCLUDING ALL);
            """
        )
        alter_queries.append(
            f"ALTER SEQUENCE {prod_schema}.{table}_id_seq RESTART WITH 1;"
        )

    if rows_result[0]["count_tmp_tables"] != 0:
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
    **context,
) -> None:
    """
    Used to delete temporary tables in the database.
    """
    params = context.get("params", {})
    nom_projet = params.get("nom_projet")
    if not nom_projet:
        raise ValueError("Project name must be provided in DAG parameters!")

    db_info = params.get("db", {})
    prod_schema = db_info.get("prod_schema", None)
    tmp_schema = db_info.get("tmp_schema", None)

    if not prod_schema:
        raise ValueError("Database schema must be provided in DAG parameters!")
    if not tmp_schema:
        raise ValueError(
            "Temporary database schema must be provided in DAG parameters!"
        )

    # Hook
    db = create_db_handler(pg_conn_id)

    tbl_names = get_tbl_names(nom_projet=nom_projet)

    for tbl in tbl_names:
        db.execute(query=f"DROP TABLE IF EXISTS {tmp_schema}.tmp_{tbl};")


@task(task_id="copy_tmp_table_to_real_table")
def copy_tmp_table_to_real_table(
    load_strategy: LoadStrategy = LoadStrategy.FULL_LOAD,  # Either "FULL_LOAD" or "INCREMENTAL"
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    **context,
) -> None:
    """
    Permet de copier les tables temporaires dans les tables réelles.

    strategy:
        FULL_LOAD      -> delete all prod rows, insert everything from tmp
        INCREMENTAL    -> UPSERT + delete missing rows based on primary key
    """
    params = context.get("params", {})
    nom_projet = params.get("nom_projet")
    if not nom_projet:
        raise ValueError("Project name must be provided in DAG parameters!")

    db_info = params.get("db", {})
    prod_schema = db_info.get("prod_schema", None)
    tmp_schema = db_info.get("tmp_schema", None)

    if not prod_schema:
        raise ValueError("Database schema must be provided in DAG parameters!")
    if not tmp_schema:
        raise ValueError(
            "Temporary database schema must be provided in DAG parameters!"
        )

    # Hook
    db = create_db_handler(pg_conn_id)

    tbl_names = get_tbl_names(nom_projet=nom_projet, order_tbl=True)
    print(f"Tables to copy: {', '.join(tbl_names)}")
    print(f"Load strategy : {load_strategy}")

    queries = []
    if load_strategy == LoadStrategy.FULL_LOAD:
        del_queries = []
        insert_queries = []
        for table in reversed(tbl_names):
            prod_table = f"{prod_schema}.{table}"
            del_queries.append(f"DELETE FROM {prod_table};")

        for table in tbl_names:
            prod_table = f"{prod_schema}.{table}"
            tmp_table = f"{tmp_schema}.tmp_{table}"
            insert_queries.append(
                f"INSERT INTO {prod_table} SELECT * FROM {tmp_table};"
            )

        queries = del_queries + insert_queries
    elif load_strategy == LoadStrategy.INCREMENTAL:
        for table in reversed(tbl_names):
            prod_table = f"{prod_schema}.{table}"
            tmp_table = f"{tmp_schema}.tmp_{table}"

            pk_cols = get_primary_keys(
                schema=prod_schema, table=table, pg_conn_id=pg_conn_id
            )
            col_list = sort_db_colnames(tbl_name=table, pg_conn_id=pg_conn_id)

            # UPSERT -- ({', '.join(col_list)})
            merge_query = f"""
                MERGE INTO {prod_table} tbl_target
                USING {tmp_table} tbl_source ON ({' AND '.join([f'tbl_source.{col} = tbl_target.{col}' for col in pk_cols])})
                WHEN MATCHED THEN
                    UPDATE SET {", ".join([f"{col}=tbl_source.{col}" for col in col_list if col not in pk_cols])}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(col_list)}) VALUES ({', '.join([f'tbl_source.{col}' for col in col_list])})
                /* Only for PG v17+
                WHEN NOT MATCHED BY SOURCE THEN
                    DELETE;
                */
            """

            # DELETE rows not in staging
            delete_query = f"""
                DELETE FROM {prod_table} tbl_target
                WHERE NOT EXISTS (
                    SELECT 1 FROM {tmp_table} tbl_source
                    WHERE {" AND ".join([f"tbl_source.{col} = tbl_target.{col}" for col in pk_cols])}
                );
            """
            queries = [merge_query]  # , delete_query]
    elif load_strategy == LoadStrategy.APPEND:
        insert_queries = []
        for table in tbl_names:
            prod_table = f"{prod_schema}.{table}"
            tmp_table = f"{tmp_schema}.tmp_{table}"

            insert_queries.append(
                f"INSERT INTO {prod_table} SELECT * FROM {tmp_table};"
            )

        queries = insert_queries
    else:
        raise ValueError(f"Unknown strategy: {load_strategy}")

    if queries:
        for q in queries:
            db.execute(query=q)
    else:
        print("No query to execute")


def sort_db_colnames(
    tbl_name: str,
    keep_file_id_col: bool = False,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    schema: str = DEFAULT_TMP_SCHEMA,
) -> list[str]:
    """Get sorted column names from a table.

    Args:
        tbl_name: Table name
        keep_file_id_col: Whether to include id column
        schema: Schema name

    Returns:
        Sorted list of column names
    """
    db = create_db_handler(pg_conn_id)
    df = db.fetch_df(f"SELECT * FROM {schema}.tmp_{tbl_name} LIMIT 0")

    cols = df.columns.tolist()
    if not keep_file_id_col:
        cols = [col for col in cols if col != "id"]

    sorted_cols = sorted(cols)
    logging.info(f"Sorted columns for table {tbl_name}: {sorted_cols}")
    return sorted_cols


def bulk_load_local_tsv_file_to_db(
    local_filepath: str,
    tbl_name: str,
    column_names: list[str],
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    schema: str = DEFAULT_TMP_SCHEMA,
) -> None:
    """Bulk load TSV file into database using COPY.

    Args:
        local_filepath: Path to local TSV file
        tbl_name: Target table name
        column_names: List of column names in order
        schema: Target schema
    """
    db = cast(PostgresDBHandler, create_db_handler(pg_conn_id))
    logging.info(f"Bulk importing {local_filepath} to {schema}.tmp_{tbl_name}")

    copy_sql = f"""
        COPY {schema}.tmp_{tbl_name} ({', '.join(column_names)})
        FROM STDIN WITH (
            FORMAT TEXT,
            DELIMITER E'\t',
            HEADER TRUE,
            NULL 'NULL'
        )
    """

    db.copy_expert(copy_sql, local_filepath)
    logging.info(f"Successfully loaded {local_filepath} into {schema}.tmp_{tbl_name}")


def _process_and_import_file(
    s3_filepath: str,
    local_filepath: str,
    tbl_name: str,
    pg_conn_id: str,
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    keep_file_id_col: bool = False,
) -> None:
    """
    warning: la fonction bulk_load fonctionne uniquement si les colonnes entre la source et la table de destination
    sont dans le même ordre !
    """
    # Define hooks
    s3_handler = FileHandlerFactory.create_handler(
        handler_type="s3",
        base_path=None,
        connection_id=DEFAULT_S3_CONN_ID,
        bucket=DEFAULT_S3_BUCKET,
    )
    local_handler = FileHandlerFactory.create_handler(
        handler_type="local", base_path=None, connection_id=None
    )
    db = create_db_handler(pg_conn_id)

    # Check if old file already exists in local system
    local_handler.delete(local_filepath)

    print(f"Reading file from remote < {s3_filepath} >")
    df = read_dataframe(file_handler=s3_handler, file_path=s3_filepath)

    df_cols = df.columns
    sorted_df_cols = sorted(df_cols)
    df = df.reindex(sorted_df_cols, axis=1).convert_dtypes()
    print(f"DF : {sorted_df_cols}")
    print(f"Saving file to local < {local_filepath} >")
    local_handler.write(
        file_path=local_filepath,
        content=df.to_csv(index=False, sep="\t", na_rep="NULL"),
    )

    sorted_db_colnames = sort_db_colnames(
        tbl_name=tbl_name, keep_file_id_col=keep_file_id_col, pg_conn_id=pg_conn_id
    )
    # Loading file to db
    if are_lists_egal(list_A=sorted_df_cols, list_B=sorted_db_colnames):
        bulk_load_local_tsv_file_to_db(
            local_filepath=local_filepath,
            tbl_name=tbl_name,
            column_names=sorted_db_colnames,
            pg_conn_id=pg_conn_id,
        )
    else:
        raise ValueError(
            "Il y a des différences entre les colonnes du DataFrame et de la Table. Impossible d'importer les données."
        )

    # Deleting file from local system
    local_handler.delete(local_filepath)


@task(task_id="import_files_to_db")
def import_files_to_db(
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    keep_file_id_col: bool = False,
    **context,
) -> None:
    params = context.get("params", {})
    nom_projet = params.get("nom_projet")
    if not nom_projet:
        raise ValueError("Project name must be provided in DAG parameters!")

    projet_config = get_projet_config(nom_projet=nom_projet)

    for config in projet_config:
        selecteur = config.selecteur
        local_filepath = config.filepath_local
        s3_filepath = config.filepath_tmp_s3
        tbl_name = config.tbl_name

        if tbl_name is None or tbl_name == "":
            print(
                f"Sélecteur {selecteur}: La table n'est pas définie. Skipping insertion !"
            )
        else:
            _process_and_import_file(
                s3_filepath=s3_filepath,
                local_filepath=local_filepath,
                tbl_name=tbl_name,
                pg_conn_id=pg_conn_id,
                s3_conn_id=s3_conn_id,
                keep_file_id_col=keep_file_id_col,
            )


@task(map_index_template="{{ import_task_name }}")
def import_file_to_db(
    selecteur_config: SelecteurConfig,
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    keep_file_id_col: bool = False,
) -> None:
    selecteur = selecteur_config.selecteur
    context = get_current_context()
    context["import_task_name"] = selecteur  # type: ignore

    # Variables
    local_filepath = selecteur_config.filepath_local
    s3_filepath = selecteur_config.filepath_tmp_s3
    tbl_name = selecteur_config.tbl_name

    if tbl_name is None or tbl_name == "":
        print(f"tbl_name is None for selecteur <{selecteur}>. Nothing to import to db")
    else:
        _process_and_import_file(
            s3_filepath=s3_filepath,
            local_filepath=local_filepath,
            tbl_name=tbl_name,
            pg_conn_id=pg_conn_id,
            s3_conn_id=s3_conn_id,
            keep_file_id_col=keep_file_id_col,
        )


@task(task_id="set_dataset_last_update")
def set_dataset_last_update_date(
    dataset_ids: list[int], pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID, **context
) -> None:
    db = create_db_handler(pg_conn_id)
    # Vérifier que le dataset existe
    datasets = db.fetch_df(
        """
            SELECT id
            FROM documentation.datasets
            WHERE id = ANY(%s);""",
        (dataset_ids,),
    )

    if len(datasets) == 0:
        raise ValueError("Aucune dataset ne correspond aux ids fournis")

    if len(datasets) < len(dataset_ids):
        raise ValueError("Certains datasets n'ont pas été trouvés.")

    # ne devrait jamais arriver
    if len(datasets) > len(dataset_ids):
        raise ValueError("Certains datasets possèdent le même id.")

    # update la date de dernière mise à jour
    execution_date = context.get("execution_date")
    if not execution_date or not isinstance(execution_date, datetime):
        raise ValueError("Invalid execution date in Airflow context")
    for dataset_id in dataset_ids:
        db.execute(
            query="""UPDATE documentation.datasets
                    SET last_update = %s
                    WHERE id=%s;
            """,
            parameters=(execution_date, dataset_id),
        )


@task
def refresh_views(pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID, **context) -> None:
    """Tâche pour actualiser les vues matérialisées"""
    db = create_db_handler(pg_conn_id)
    params = context.get("params", {})

    db_info = params.get("db", {})
    prod_schema = db_info.get("prod_schema", None)

    if not prod_schema:
        raise ValueError("Database schema must be provided in DAG parameters!")

    get_mview_query = """
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = %s;
    """

    views = db.fetch_df(get_mview_query, (prod_schema,))["matviewname"].tolist()

    if len(views) == 0:
        print(f"No materialized views found for schema {prod_schema}. Skipping ...")
    else:
        sql_queries = [
            f"REFRESH MATERIALIZED VIEW {prod_schema}.{view_name};"
            for view_name in views
        ]
        for query in sql_queries:
            db.execute(query=query)


# =========================
# PGLoader functions/Tasks
# =========================
def clean_grist_sqlite_file(local_sqlite_path: str) -> None:
    import sqlite3

    # Connect to the database
    conn = sqlite3.connect(local_sqlite_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    columns_info = {}
    # List all columns informations
    for (table_name,) in tables:
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        # columns tuples look like: (cid, name, type, notnull, dflt_value, pk)
        column_names = [col[1] for col in columns]
        columns_info[table_name] = column_names
        # print(columns_info)

    # Drop all columns related to Grist in tables
    for table, column_names in columns_info.items():
        for column_name in column_names:
            if column_name == "manualSort" or "grist" in column_name:
                print(f"Dropping column {column_name} from table {table}")
                cursor.execute(f"ALTER TABLE {table} DROP COLUMN {column_name}")

        if table.startswith("Struc_"):
            new_tbl_name = table.replace("Struc_", "")
            print(f"Renaming table {table} to {new_tbl_name}")
            cursor.execute(f"ALTER TABLE {table} RENAME TO {new_tbl_name}")

    # Commit and close
    conn.commit()
    conn.close()


@task()
def import_sqlitefile_to_db(
    s3_sqlite_path: str, db_schema: str, pg_conn_id: str
) -> None:
    import textwrap

    def generate_script(sqlite_path: str, pg_uri: str, db_schema: str) -> str:
        return textwrap.dedent(
            f"""
            LOAD DATABASE
                FROM sqlite:///{sqlite_path}
                INTO postgresql://{pg_uri}
            CAST type date TO date USING unix-timestamp-to-timestamptz
            SET search_path TO '{db_schema}'
            WITH include drop, create tables, create indexes, reset sequences
            EXCLUDING TABLE NAMES LIKE '_grist%', 'onglet_%', 'doc_%', '%_summary_%'
            ;
        """
        )

    # Hooks
    db = create_db_handler(pg_conn_id)
    db_uri = db.get_uri().replace("postgresql://", "")
    s3_handler = S3FileHandler(
        connection_id=DEFAULT_S3_CONN_ID, bucket=DEFAULT_S3_BUCKET
    )
    local_handler = FileHandlerFactory.create_handler(handler_type="local")

    # Copy s3 file to local system
    local_sqlite_path = "/tmp/tmp_grist.db"
    local_handler.write(
        file_path=local_sqlite_path, content=s3_handler.read(file_path=s3_sqlite_path)
    )

    # Clean SQLite file
    clean_grist_sqlite_file(local_sqlite_path=local_sqlite_path)

    # Generate script to bulk load
    script = generate_script(
        sqlite_path=local_sqlite_path, pg_uri=db_uri, db_schema=db_schema
    )
    print(script)

    # Create script file on file system
    PGLOADER_SCRIPT_NAME = "/tmp/tmp_grist.load"
    with open(PGLOADER_SCRIPT_NAME, "w") as f:
        f.write(script)

    # Run pgloader command
    import subprocess

    try:
        result = subprocess.run(
            ["pgloader", "--verbose", PGLOADER_SCRIPT_NAME],
            capture_output=True,
            text=True,
            check=True,
        )
        print(result.stdout)
        print(result.stderr)
    except subprocess.CalledProcessError as e:
        print("pgloader failed!")
        print("STDOUT:\n", e.stdout)
        print("STDERR:\n", e.stderr)
        raise

    # Remove script from file system
    import os

    os.remove(local_sqlite_path)
    os.remove(PGLOADER_SCRIPT_NAME)
