import json
import os
import sqlite3
import uuid
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from dags.applications.configuration_projets import process
from modules.common_tasks.grist import generic_grist_processing
from modules.utils.logs import df_info
from modules.utils.process.structures import normalize_grist_dataframe
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values

# Enregistrer l'adaptateur pour les entiers numpy
register_adapter(typ=np.int64, callable=AsIs)

# Ordre des tables (dépendances)
TBL_ORDERED = [
    {
        "tbl_name": "ref_direction",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "id",
                "direction",
            ],
            cols_mapping={"id": "id_direction"},
            txt_columns=["direction"],
            custom_fn=process.process_direction,
        ),
    },
    {
        "tbl_name": "ref_service",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "id",
                "direction",
                "service",
            ],
            cols_mapping={"direction": "id_direction", "id": "id_service"},
            txt_columns=["service"],
            ref_columns=["id_direction"],
            custom_fn=process.process_service,
        ),
    },
    {
        "tbl_name": "projet",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "id",
                "projet",
                "direction",
                "service",
            ],
            cols_mapping={
                "id": "id_projet",
                "direction": "id_direction",
                "service": "id_service",
            },
            txt_columns=["projet"],
            ref_columns=["id_direction", "id_service"],
            custom_fn=process.process_projets,
        ),
    },
    {
        "tbl_name": "projet_contact",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "id",
                "projet",
                "contact_mail",
                "is_mail_generic",
            ],
            cols_mapping={
                "id": "id_contact",
                "projet": "id_projet",
            },
            txt_columns=["contact_mail"],
            ref_columns=["id_projet"],
            bool_columns=["is_mail_generic"],
            custom_fn=process.process_projet_contact,
        ),
    },
    {
        "tbl_name": "projet_documentation",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "projet",
                "type_documentation",
                "lien",
            ],
            cols_mapping={
                "projet": "id_projet",
            },
            txt_columns=["type_documentation", "lien"],
            ref_columns=["id_projet"],
            custom_fn=process.process_projet_documentation,
        ),
    },
    {
        "tbl_name": "projet_s3",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "projet",
                "bucket",
                "key",
                "key_tmp",
            ],
            cols_mapping={
                "projet": "id_projet",
            },
            txt_columns=["bucket", "key", "key_tmp"],
            ref_columns=["id_projet"],
            custom_fn=process.process_projet_s3,
        ),
    },
    {
        "tbl_name": "projet_selecteur",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "id",
                "projet",
                "type_de_selecteur",
                "selecteur",
            ],
            cols_mapping={
                "id": "id_selecteur",
                "projet": "id_projet",
                "type_de_selecteur": "type_selecteur",
            },
            txt_columns=["selecteur", "type_selecteur"],
            ref_columns=["id_projet"],
            custom_fn=process.process_projet_selecteur,
        ),
    },
    {
        "tbl_name": "selecteur_source",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "projet",
                "type",
                "selecteur",
                "id_source",
            ],
            cols_mapping={
                "projet": "id_projet",
                "selecteur": "id_selecteur",
                "type": "type_source",
            },
            txt_columns=["type_source", "id_source"],
            ref_columns=["id_projet", "id_selecteur"],
            custom_fn=process.process_selecteur_source,
        ),
    },
    {
        "tbl_name": "selecteur_s3",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "projet",
                "selecteur",
                "filename",
                "key",
            ],
            cols_mapping={
                "projet": "id_projet",
                "selecteur": "id_selecteur",
            },
            txt_columns=["filename", "key"],
            ref_columns=["id_projet", "id_selecteur"],
            custom_fn=process.process_selecteur_s3,
        ),
    },
    {
        "tbl_name": "selecteur_database",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "projet",
                "selecteur",
                "tbl_name",
            ],
            cols_mapping={
                "projet": "id_projet",
                "selecteur": "id_selecteur",
            },
            txt_columns=["tbl_name"],
            ref_columns=["id_projet", "id_selecteur"],
            custom_fn=process.process_selecteur_database,
        ),
    },
    {
        "tbl_name": "selecteur_column_mapping",
        "process_func": partial(
            generic_grist_processing,
            cols_to_keep=[
                "id",
                "projet",
                "selecteur",
                "colname_source",
                "colname_dest",
                "to_keep",
                "date_archivage",
            ],
            cols_mapping={
                "id": "id_col_mapping",
                "projet": "id_projet",
                "selecteur": "id_selecteur",
            },
            txt_columns=["colname_source", "colname_dest"],
            ref_columns=["id_projet", "id_selecteur"],
            bool_columns=["to_keep"],
            date_columns=["date_archivage"],
            custom_fn=process.process_selecteur_column_mapping,
        ),
    },
]


def clear_tables(pg_cur, schema: str, tbl_names: list, dry_run: bool) -> None:
    """Supprime les données des tables si dry_run est désactivé."""
    if dry_run:
        print("DRY_RUN is set to True. Skipping ...")
        return

    for tbl_name in tbl_names:
        drop_query = f"DELETE FROM {schema}.{tbl_name};"
        print(drop_query)
        pg_cur.execute(query=drop_query)


def process_table(
    sqlite_conn,
    tbl_desc: dict,
    add_metadata: bool,
    snapshot_id: uuid.UUID,
    import_timestamp: datetime,
) -> pd.DataFrame:
    # Lecture des données depuis SQLite
    df = pd.read_sql_query(
        sql=f"SELECT * FROM {tbl_desc['tbl_name'].capitalize()}",
        con=sqlite_conn,
    )
    df = normalize_grist_dataframe(df=df)
    df_info(df=df, df_name=tbl_desc["tbl_name"].capitalize())

    # Application de la fonction de traitement
    df = tbl_desc["process_func"](df=df)
    df = df.fillna(np.nan).replace([np.nan], [None])

    # Ajout des métadonnées
    if add_metadata:
        df["snapshot_id"] = str(snapshot_id)
        df["import_timestamp"] = import_timestamp

    # Affichage des résultats
    print(df.columns)
    print(df.dtypes)
    print(df.isnull().sum())
    df_info(df=df, df_name=tbl_desc["tbl_name"].capitalize())

    return df


def create_partition(pg_cur, schema: str, tbl_name: str, dry_run: bool, import_timestamp: datetime) -> None:
    """Crée une partition pour une table PostgreSQL si dry_run est désactivé."""

    from_date = import_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
    to_date = from_date + timedelta(days=1)
    partition_name = f"{tbl_name}_{from_date.strftime(format='%Y%m%d')}_{to_date.strftime(format='%Y%m%d')}"

    print(f"Creating partition {partition_name} for {tbl_name}.")
    # Créer la partition
    create_partition_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{partition_name}
        PARTITION OF {schema}.{tbl_name}
        FOR VALUES FROM
            ('{from_date.strftime(format="%Y-%m-%d")}') TO ('{to_date.strftime(format="%Y-%m-%d")}');
    """
    print(create_partition_query)

    if dry_run:
        print("DRY_RUN is set to True. Skipping ...")
        return

    pg_cur.execute(query=create_partition_query)


def insert_into_postgres(df: pd.DataFrame, tbl_desc: dict, schema: str, pg_cur, dry_run: bool) -> None:
    """Insère les données dans PostgreSQL."""
    # Insertion dans PostgreSQL si dry_run est désactivé
    if dry_run:
        print()
        return

    fetch_query = f"SELECT * FROM {schema}.{tbl_desc['tbl_name']} LIMIT 0;"
    pg_cur.execute(query=fetch_query)
    if pg_cur.description:
        sorted_cols = sorted([col.name for col in pg_cur.description if col.name in df.columns])
        print(sorted_cols)

        insert_records = df[sorted_cols].to_records(index=False).tolist()
        insert_query = f"""
            INSERT INTO {schema}.{tbl_desc['tbl_name']}
            ({", ".join(sorted_cols)})
            VALUES %s
        """
        print(insert_query)
        execute_values(cur=pg_cur, sql=insert_query, argslist=insert_records)
        print(f"Fin du traitement de la table <{tbl_desc['tbl_name']}>\n")
    else:
        print("Aucun résultat récupéré...")


if __name__ == "__main__":
    dir = os.path.dirname(os.path.realpath(__file__))
    config_path = Path(dir, "config.json")

    # Load config
    with open(file=config_path) as f:
        config = json.load(fp=f)

    # Variables
    dry_run = config["dry_run"]
    schema = config["db"]["dest_schema"]
    now = datetime.now()
    snapshot_id = uuid.uuid4()
    add_metadata = True  # Peut être configuré dans le JSON si nécessaire

    # Init db connections
    sqlite_conn = sqlite3.connect(config["grist"]["doc_path"])
    pg_conn = psycopg2.connect(
        host=config["db"]["host"],
        port=config["db"]["port"],
        dbname=config["db"]["name"],
        user=config["db"]["user"],
        password=config["db"]["password"],
    )
    pg_cur = pg_conn.cursor()

    # Nettoyer les tables si dry_run est désactivé
    tbl_names = [tbl["tbl_name"] for tbl in TBL_ORDERED]
    clear_tables(pg_cur=pg_cur, schema=schema, tbl_names=tbl_names, dry_run=dry_run)

    # Traiter chaque table
    for tbl_desc in TBL_ORDERED:
        """Traite une table : lecture, transformation et insertion."""
        print("\n", "=" * 50)
        print(f"Début du traitement de la table <{tbl_desc['tbl_name']}>")

        df = process_table(
            sqlite_conn=sqlite_conn,
            tbl_desc=tbl_desc,
            add_metadata=add_metadata,
            snapshot_id=snapshot_id,
            import_timestamp=now,
        )
        create_partition(
            pg_cur=pg_cur,
            schema=schema,
            tbl_name=tbl_desc["tbl_name"],
            dry_run=dry_run,
            import_timestamp=now,
        )
        insert_into_postgres(df=df, tbl_desc=tbl_desc, schema=schema, pg_cur=pg_cur, dry_run=dry_run)

    # Valider et fermer les connexions
    if not dry_run:
        pg_conn.commit()
    pg_conn.close()
    sqlite_conn.close()
