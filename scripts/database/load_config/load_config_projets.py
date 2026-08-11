import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from dags.applications.configuration_projets import process
from modules.utils.logs import df_info
from modules.utils.process.structures import normalize_grist_dataframe
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values

# Enregistrer l'adaptateur pour les entiers numpy
register_adapter(typ=np.int64, callable=AsIs)

# Ordre des tables (dépendances)
TBL_ORDERED = [
    {"tbl_name": "ref_direction", "process_func": process.process_direction},
    {"tbl_name": "ref_service", "process_func": process.process_service},
    {"tbl_name": "projet", "process_func": process.process_projets},
    {"tbl_name": "projet_contact", "process_func": process.process_projet_contact},
    {
        "tbl_name": "projet_documentation",
        "process_func": process.process_projet_documentation,
    },
    {"tbl_name": "projet_s3", "process_func": process.process_projet_s3},
    {"tbl_name": "projet_selecteur", "process_func": process.process_projet_selecteur},
    {"tbl_name": "selecteur_source", "process_func": process.process_selecteur_source},
    {
        "tbl_name": "selecteur_column_mapping",
        "process_func": process.process_selecteur_column_mapping,
    },
    {"tbl_name": "selecteur_s3", "process_func": process.process_selecteur_s3},
    {
        "tbl_name": "selecteur_database",
        "process_func": process.process_selecteur_database,
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
    schema: str,
    pg_cur,
    now: datetime,
    add_metadata: bool,
    dry_run: bool,
) -> None:
    """Traite une table : lecture, transformation et insertion."""
    print("\n", "=" * 50)
    print(f"Début du traitement de la table <{tbl_desc['tbl_name']}>")

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
        df["import_timestamp"] = now
        df["import_date"] = now.date()
        df["snapshot_id"] = now.strftime(format="%Y%m%d_%H:%M:%S")

    # Affichage des résultats
    print(df.columns)
    print(df.dtypes)
    print(df.isnull().sum())
    df_info(df=df, df_name=tbl_desc["tbl_name"].capitalize())

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
        process_table(
            sqlite_conn=sqlite_conn,
            tbl_desc=tbl_desc,
            schema=schema,
            pg_cur=pg_cur,
            now=now,
            add_metadata=add_metadata,
            dry_run=dry_run,
        )

    # Valider et fermer les connexions
    if not dry_run:
        pg_conn.commit()
    pg_conn.close()
    sqlite_conn.close()
