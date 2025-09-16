"""SQL task utilities using infrastructure handlers."""

import logging
import sqlite3
import tempfile
from typing import cast
from datetime import datetime

import psycopg2
from airflow.decorators import task
from airflow.operators.python import get_current_context
import pandas as pd

from infra.database.factory import create_db_handler
from infra.database.postgres import PostgresDBHandler

from infra.file_handling.dataframe import read_dataframe
from infra.file_handling.factory import FileHandlerFactory
from infra.file_handling.local import LocalFileHandler

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


def get_conn_from_s3_sqlite(sqlite_file_s3_filepath: str) -> sqlite3.Connection:
    """Create a SQLite connection from a S3 remote SQLite file.

    Args:
        sqlite_file_s3_filepath: Path to SQLite file in S3

    Returns:
        SQLite database connection
    """
    s3_handler = FileHandlerFactory.create_handler(
        handler_type="s3",
        base_path=None,
        connection_id=DEFAULT_S3_CONN_ID,
        bucket=DEFAULT_S3_BUCKET,
    )
    local_handler = FileHandlerFactory.create_handler(handler_type="local")

    # Copy s3 file to local system
    with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp_file:
        local_handler.write(
            file_path=tmp_file.name, content=s3_handler.read(sqlite_file_s3_filepath)
        )
        return sqlite3.connect(tmp_file.name)

    return conn


def get_data_from_s3_sqlite_file(
    table_name: str,
    sqlite_s3_filepath: str,
    sqlite_conn: sqlite3.Connection,
) -> pd.DataFrame:
    """Read data from SQLite table in S3.

    Args:
        table_name: Name of the table to read
        sqlite_s3_filepath: S3 path to SQLite file
        sqlite_conn: SQLite connection

    Returns:
        DataFrame with table contents, excluding internal columns
    """
    df = pd.read_sql_query("SELECT * FROM ?", params=(table_name,), con=sqlite_conn)

    # Remove internal columns (e.g., grist_*, manual_*)
    internal_cols = df.filter(regex="^(grist|manual)").columns
    return df.drop(columns=internal_cols)


@task(task_id="get_tbl_names_from_postgresql")
def get_tbl_names_from_postgresql(**context) -> list[str]:
    params = context.get("params", {})
    nom_projet = params.get("nom_projet")
    if not nom_projet:
        raise ValueError("Project name must be provided in DAG parameters!")

    tbl_names = get_tbl_names(nom_projet=nom_projet)
    return tbl_names


@task
def ensure_monthly_partition(
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

    for tbl in tbl_names:
        # Nom de la partition : parenttable_YYYY_MM
        partition_name = f"{tbl}_y{execution_date.year}m{execution_date.month:02d}"

        # Calcul des bornes de la partition
        from_date = execution_date.replace(day=1)
        if execution_date.month == 12:
            to_date = execution_date.replace(
                year=execution_date.year + 1, month=1, day=1
            )
        else:
            to_date = execution_date.replace(month=execution_date.month + 1, day=1)

        try:
            logging.info(f"Creating partition {partition_name} for {tbl}.")
            # Créer la partition
            create_sql = f"""
                CREATE TABLE {prod_schema}.{partition_name}
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


@task(task_id="copy_tmp_table_to_real_table")
def copy_tmp_table_to_real_table(
    pg_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
    **context,
) -> None:
    """Permet de copier les tables temporaires dans les tables réelles."""
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

    print(f"Tmp tables will be copied to the following tables: {', '.join(tbl_names)}")
    sql_queries = []
    for table in reversed(tbl_names):
        sql_queries.append(f"DELETE FROM {prod_schema}.{table};")

    for table in tbl_names:
        sql_queries.append(
            f"INSERT INTO {prod_schema}.{table} (SELECT * FROM {tmp_schema}.tmp_{table});"
        )

    str_queries = "\n".join(sql_queries)
    print(f"The following queries will be run: \n{str_queries}")
    for sql_query in sql_queries:
        db.execute(query=sql_query)


def sort_db_colnames(
    tbl_name: str, keep_file_id_col: bool = False, schema: str = DEFAULT_TMP_SCHEMA
) -> list[str]:
    """Get sorted column names from a table.

    Args:
        tbl_name: Table name
        keep_file_id_col: Whether to include id column
        schema: Schema name

    Returns:
        Sorted list of column names
    """
    db = create_db_handler(DEFAULT_PG_DATA_CONN_ID)
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
    schema: str = DEFAULT_TMP_SCHEMA,
) -> None:
    """Bulk load TSV file into database using COPY.

    Args:
        local_filepath: Path to local TSV file
        tbl_name: Target table name
        column_names: List of column names in order
        schema: Target schema
    """
    db = cast(PostgresDBHandler, create_db_handler(DEFAULT_PG_DATA_CONN_ID))
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
        tbl_name=tbl_name, keep_file_id_col=keep_file_id_col
    )
    # Loading file to db
    if are_lists_egal(list_A=sorted_df_cols, list_B=sorted_db_colnames):
        bulk_load_local_tsv_file_to_db(
            local_filepath=local_filepath,
            tbl_name=tbl_name,
            column_names=sorted_db_colnames,
        )
    else:
        raise ValueError(
            "Il y a des différences entre les colonnes du DataFrame et de la Table. Impossible d'importer les données."
        )

    # Deleting file from local system
    local_handler.delete(local_filepath)


@task(task_id="import_files_to_db")
def import_files_to_db(
    pg_data_conn_id: str = DEFAULT_PG_DATA_CONN_ID,
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
                pg_conn_id=pg_data_conn_id,
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
        sql_queries = [f"REFRESH MATERIALIZED VIEW {view_name};" for view_name in views]
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
