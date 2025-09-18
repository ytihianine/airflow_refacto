import os
import sqlite3
import pandas as pd
import numpy as np
from dags.applications.configuration_projets import process

import psycopg2
from psycopg2.extensions import register_adapter, AsIs
from psycopg2.extras import execute_values

register_adapter(np.int64, AsIs)


# Tbl order is important due to tbl dependancy !
tbl_ordered = [
    {"tbl_name": "ref_direction", "process_func": process.process_direction},
    {"tbl_name": "ref_service", "process_func": process.process_service},
    {"tbl_name": "projet", "process_func": process.process_projets},
    {"tbl_name": "selecteur", "process_func": process.process_selecteur},
    {"tbl_name": "source", "process_func": process.process_source},
    {"tbl_name": "correspondance_colonne", "process_func": process.process_col_mapping},
    {"tbl_name": "colonnes_requises", "process_func": process.process_col_requises},
    {"tbl_name": "storage_path", "process_func": process.process_storage_path},
]

ENV = os.environ.copy()
db_path = "/home/onyxia/work/Configuration - interne.grist"
sqlite_conn = sqlite3.connect(db_path)
schema = "conf_projets"
pg_conn = psycopg2.connect(
    host=ENV["CONFIG_DB_HOST"],
    port=ENV["CONFIG_DB_PORT"],
    dbname=ENV["CONFIG_DB_NAME"],
    user=ENV["CONFIG_DB_USER"],
    password=ENV["CONFIG_DB_PASSWORD"],
)
pg_cur = pg_conn.cursor()
# for each tbl
for tbl in tbl_ordered:
    # read data for sqlite file
    df = df = pd.read_sql_query(
        sql=f"SELECT * FROM {tbl["tbl_name"].capitalize()}", con=sqlite_conn
    )
    # apply process function
    df = tbl["process_func"](df=df)
    df = df.drop(df.filter(regex="^(grist|manual)").columns, axis=1)
    df = df.fillna(np.nan).replace([np.nan], [None])
    df = df.convert_dtypes()
    print(df.columns)
    print(df.dtypes)
    print(df.isnull().sum())
    # Get tbl columns and order them
    fetch_query = f"SELECT * FROM {schema}.{tbl["tbl_name"]} LIMIT 0;"
    pg_cur.execute(fetch_query)
    sorted_cols = sorted([col.name for col in pg_cur.description])
    print(sorted_cols)

    # load data to config db
    insert_records = df.to_records(index=False).tolist()
    insert_query = (
        f"INSERT INTO {schema}.{tbl["tbl_name"]} ({", ".join(sorted_cols)}) VALUES %s"
    )
    print(insert_query)
    execute_values(pg_cur, insert_query, insert_records)

pg_conn.commit()
pg_conn.close()
sqlite_conn.close()
