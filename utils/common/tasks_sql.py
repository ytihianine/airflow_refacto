import sqlite3
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.hooks.sql import DbApiHook
import pandas as pd

from utils.file_handler import MinioFileHandler, LocalFileHandler
from utils.common.config_func import get_tbl_names, get_storage_rows
from utils.control.structures import are_lists_egal
from utils.common.vars import DEFAULT_TMP_SCHEMA, DEFAULT_PG_CONN_ID, DEFAULT_S3_CONN_ID


CONF_SCHEMA = "conf_projets"


def get_conn_from_s3_sqlite(sqlite_file_s3_filepath: str) -> sqlite3.Connection:
    """Create a sqlite connecteur from a s3 remote sqlite file"""
    # Variables
    import tempfile

    s3_hook = MinioFileHandler(connection_id=DEFAULT_S3_CONN_ID, bucket="dsci")

    # Copy s3 file to local system
    sqlite_file = s3_hook._get_file_obj(file_key=sqlite_file_s3_filepath)
    with tempfile.NamedTemporaryFile(delete_on_close=True, suffix=".db") as fp:
        fp.write(sqlite_file)
        # Once the sqlite.db file on local system, create the con
        conn = sqlite3.connect(fp.name)

    return conn


def get_data_from_s3_sqlite_file(
    grist_tbl_name: str,
    sqlite_s3_filepath: str,
    sqlite_conn: sqlite3.Connection,
) -> pd.DataFrame:
    df = pd.read_sql_query(
        f"SELECT * FROM {grist_tbl_name}",
        con=sqlite_conn,
    )
    # Removing all Grist internal Columns
    df = df.drop(df.filter(regex="^(grist|manual)").columns, axis=1)

    return df


@task()
def get_project_config(**context) -> list[dict[str, any]]:
    """
    Obtenir toutes les informations relatives à un projet
    pour les rendre utilisables dans les autres tâches.
    """
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hook
    db_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    print(f"Getting config of project <{nom_projet}>")
    df_project_conf = db_hook.get_pandas_df(
        sql=f"""SELECT vcp.nom_projet, vcp.selecteur, vcp.nom_source, vcp.filename,
                vcp.filepath_source_s3, vcp.filepath_local, vcp.filepath_s3,
                vcp.tbl_name, vcp.tbl_order
            FROM {CONF_SCHEMA}.vue_conf_projets vcp
            WHERE vcp.nom_projet=%(nom_projet)s;
        """,
        parameters={"nom_projet": nom_projet},
    )

    # Creating configuration project data structure
    project_conf = {}
    project_conf["tbl_names_ordered"] = df_project_conf.sort_values(by=["tbl_order"])[
        "tbl_name"
    ].to_list()
    project_conf["s3_keys_source"] = df_project_conf.loc[
        df_project_conf["filepath_source_s3"].notna(), "filepath_source_s3"
    ].to_list()

    return project_conf


@task(task_id="get_tbl_names_from_sqlite")
def get_tbl_names_from_sqlite(sqlite_file_s3_filepath: str, **context) -> list[str]:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Request the sqlite file data
    conn = get_conn_from_s3_sqlite(sqlite_file_s3_filepath=sqlite_file_s3_filepath)
    cur = conn.cursor()
    tbl_rows = cur.execute(
        """SELECT Selecteurs.selecteur, Storage.db_tbl_name
            FROM Projets
            INNER JOIN Selecteurs ON Selecteurs.Projet = Projets.id
            INNER JOIN Storage ON Storage.selecteur = Selecteurs.id AND Storage.Projet = Projets.id
            WHERE Projets.Projet = :pram_projet
            ORDER BY Storage.tbl_order ASC;
        """,
        {"pram_projet": nom_projet},
    ).fetchall()

    print(tbl_rows)
    if len(tbl_rows) == 0:
        raise ValueError(
            f"Aucune ligne de configuration récupérée pour le projet {nom_projet}"
        )

    tbl_names = [row[1] for row in tbl_rows]
    conn.close()
    return tbl_names


@task(task_id="get_tbl_names_from_postgresql")
def get_tbl_names_from_postgresql(**context) -> list[str]:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hook
    db_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    print(f"Get table names from project <{nom_projet}>")
    tbl_names = get_tbl_names(nom_projet=nom_projet, db_hook=db_hook)
    return tbl_names


@task(task_id="create_tmp_tables")
def create_tmp_tables(
    prod_schema: str,
    tbl_names: list[str] = None,
    tbl_names_task_id: str = None,
    pg_conn_id: str = DEFAULT_PG_CONN_ID,
    tmp_schema: str = DEFAULT_TMP_SCHEMA,
    reset_id_seq: bool = True,
    **context,
) -> None:
    """
    Cette fonction a pour but de créer des tables temporaires vierges
    en copiant la structure des tables réelles
    """
    # Hook
    db_hook = PostgresHook(postgres_conn_id=pg_conn_id)

    if tbl_names is None:
        tbl_names = context["ti"].xcom_pull(
            task_ids=tbl_names_task_id, key="return_value"
        )
        print(tbl_names)

    rows_result = db_hook.get_records(
        sql="""SELECT COUNT(*)
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

    if rows_result[0][0] != 0:
        db_hook.run(sql=drop_queries)

    db_hook.run(sql=create_queries)
    if reset_id_seq:
        db_hook.run(sql=alter_queries)


@task(task_id="copy_tmp_table_to_real_table")
def copy_tmp_table_to_real_table(
    prod_schema: str,
    tmp_schema: str = DEFAULT_TMP_SCHEMA,
    tbl_names: list[str] = None,
    tbl_names_task_id: str = None,
    pg_conn_id: str = DEFAULT_PG_CONN_ID,
    **context,
) -> None:
    """Permet de copier les tables temporaires dans les tables réelles."""
    # Hook
    db_hook = PostgresHook(postgres_conn_id=pg_conn_id)

    if tbl_names is None:
        tbl_names = context["ti"].xcom_pull(
            task_ids=tbl_names_task_id, key="return_value"
        )

    tbl_names = list(tbl_names)
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
    db_hook.run(sql=sql_queries)


def sort_db_colnames(
    db_hook: DbApiHook, tbl_name: str, keep_file_id_col: bool = False
) -> list[str]:
    cols = db_hook.get_pandas_df(
        f"SELECT * FROM temporaire.tmp_{tbl_name} LIMIT 0;"
    ).columns
    # remove id column
    if keep_file_id_col:
        sorted_cols = sorted(cols)
    else:
        sorted_cols = sorted([col for col in cols if col != "id"])
    print(f"DB table <{tbl_name}> sorted columns: {sorted_cols}")
    return sorted_cols


def bulk_load_local_tsv_file_to_db(
    db_hook: DbApiHook, local_filepath: str, tbl_name: str, sorted_colnames: list[str]
) -> None:
    print(f"Bulk import file at {local_filepath} to db table temporaire.tmp_{tbl_name}")
    db_hook.copy_expert(
        sql=f"""COPY temporaire.tmp_{tbl_name}
            ({', '.join(sorted_colnames)})
            FROM STDIN WITH (
                FORMAT TEXT,
                DELIMITER E'\t',
                HEADER TRUE,
                NULL 'NULL'
            )
            """,
        filename=local_filepath,
    )
    print(
        f"Local file at {local_filepath} loaded to db table temporaire.tmp_{tbl_name}"
    )


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
    s3_hook = MinioFileHandler(connection_id=s3_conn_id, bucket="dsci")
    local_hook = LocalFileHandler()
    db_hook = PostgresHook(postgres_conn_id=pg_conn_id)

    # Check if old file already exists in local system
    local_hook.delete_file(local_filepath)

    # Read file based on extension
    filename = s3_filepath.split("/")[-1]
    file_extension = filename.split(".")[-1]

    print(f"Reading file from remote < {s3_filepath} >")
    df = s3_hook.read_file_based_on_extension(
        s3_filepath=s3_filepath, file_extension=file_extension
    )
    df_cols = df.columns
    sorted_df_cols = sorted(df_cols)
    df = df.reindex(sorted_df_cols, axis=1).convert_dtypes()
    print(f"DF : {sorted_df_cols}")
    print(f"Saving file to local < {local_filepath} >")
    local_hook.to_csv(
        df=df, path_or_buf=local_filepath, index=False, sep="\t", na_rep="NULL"
    )

    sorted_db_colnames = sort_db_colnames(
        db_hook=db_hook, tbl_name=tbl_name, keep_file_id_col=keep_file_id_col
    )
    # Loading file to db
    if are_lists_egal(list_A=sorted_df_cols, list_B=sorted_db_colnames):
        bulk_load_local_tsv_file_to_db(
            db_hook=db_hook,
            local_filepath=local_filepath,
            tbl_name=tbl_name,
            sorted_colnames=sorted_db_colnames,
        )
    else:
        raise ValueError(
            "Il y a des différences entre les colonnes du DataFrame et de la Table. Impossible d'importer les données."
        )

    # Deleting file from local system
    local_hook.delete_file(local_filepath)


@task(task_id="import_file_to_db_at_once")
def import_file_to_db_at_once(
    pg_conn_id: str,
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    storage_paths: pd.DataFrame = None,
    sqlite_file_s3_filepath: str = None,
    keep_file_id_col: bool = False,
    **context,
) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )

    storage_paths = get_storage_rows(
        nom_projet=nom_projet, pg_conn_id="db_depose_fichier"
    )

    for row in storage_paths.sort_values(by=["tbl_order"]).itertuples():
        selecteur = row.selecteur
        local_filepath = row.filepath_local
        s3_filepath = row.filepath_tmp_s3
        tbl_name = row.tbl_name

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
    storage_row: dict[str, str],
    pg_conn_id: str = DEFAULT_PG_CONN_ID,
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    keep_file_id_col: bool = False,
) -> None:
    selecteur = storage_row["selecteur"]
    context = get_current_context()
    context["import_task_name"] = selecteur

    # Variables
    local_filepath = storage_row["filepath_local"]
    s3_filepath = storage_row["filepath_tmp_s3"]
    tbl_name = storage_row["tbl_name"]

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
    dataset_ids: list[int], pg_conn_id: str = DEFAULT_PG_CONN_ID, **context
) -> None:
    db_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    # Vérifier que le dataset existe
    datasets = db_hook.get_records(
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
    execution_date = context.get("dag_run").get("execution_date")
    for dataset_id in dataset_ids:
        db_hook.run(
            sql=[
                """UPDATE documentation.datasets
                    SET last_update = %(last_update)s
                    WHERE id=%(dataset_id)s;
                """
            ],
            parameters={"last_update": execution_date, "dataset_id": dataset_id},
        )


@task
def refresh_views(views: list[str], pg_conn_id: str = DEFAULT_PG_CONN_ID) -> None:
    """Tâche pour actualiser les vues matérialisées"""
    if len(views) == 0:
        print("Aucune vue matérialisée à actualier. Skipping ...")
    else:
        db_hook = PostgresHook(postgres_conn_id=pg_conn_id)
        sql_queries = [f"REFRESH MATERIALIZED VIEW {view_name};" for view_name in views]
        db_hook.run(sql=sql_queries)


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
    db_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    db_uri = db_hook.get_uri().replace("postgresql://", "")
    s3_hook = MinioFileHandler(connection_id=DEFAULT_S3_CONN_ID, bucket="dsci")

    # Copy s3 file to local system
    sqlite_file = s3_hook._get_file_obj(file_key=s3_sqlite_path)
    local_sqlite_path = "/tmp/tmp_grist.db"
    with open(local_sqlite_path, "wb") as local_sqlite_file:
        local_sqlite_file.write(sqlite_file)

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
