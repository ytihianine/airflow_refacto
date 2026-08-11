import os

import pandas as pd
import psycopg2
from modules.constants import custom_logger
from modules.utils.process.structures import are_lists_egal
from scripts.settings import get_settings

settings = get_settings()

# FIXED VARIABLES
TSV_FILE_PATH = settings.load_dataset.tsv_file_path

# USER VARIABLES
FILE_PATH = settings.load_dataset.file_path
DB_ID_COLNAME = settings.load_dataset.db_id_colname
SCHEMA = settings.load_dataset.db_schema
TABLE_NAME = settings.load_dataset.table_name
IMPORT_TIMESTAMP = settings.load_dataset.import_timestamp
SNAPSHOT_ID = settings.load_dataset.snapshot_id

pg_conn = psycopg2.connect(
    host=settings.db.host,
    port=settings.db.port,
    dbname=settings.db.name,
    user=settings.db.user,
    password=settings.db.password,
)
pg_cur = pg_conn.cursor()

# get table columns sorted and remove ID columns
pg_cur.execute(query=f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{SCHEMA}'
    AND table_name = '{TABLE_NAME}'
    AND column_name != '{DB_ID_COLNAME}'
    ORDER BY column_name
""")
db_columns = [row[0] for row in pg_cur.fetchall()]
custom_logger.info(msg=f"Database columns sorted: {db_columns}")

# read data
df = pd.read_parquet(path=FILE_PATH)

# Add metadata columns
df["import_timestamp"] = pd.to_datetime(IMPORT_TIMESTAMP)
df["import_date"] = pd.to_datetime(IMPORT_TIMESTAMP)
df["snapshot_id"] = SNAPSHOT_ID

# Drop ID columns and sort colnames
df = df.drop(columns=[DB_ID_COLNAME], errors="ignore")
df_columns = sorted(df.columns)
df = df[df_columns]
custom_logger.info(msg=f"Dataframe columns sorted: {df_columns}")

# export file to csv using "\" as sep
df.to_csv(path_or_buf=TSV_FILE_PATH, sep="\t", index=False)

# Copy expert to db
if not are_lists_egal(list_A=list(df.columns), list_B=db_columns):
    raise ValueError("Erreur")

with open(file=TSV_FILE_PATH) as f:
    pg_cur.copy_expert(
        sql=f"""
            COPY {SCHEMA}.{TABLE_NAME} ({', '.join(db_columns)})
            FROM STDIN WITH (
                FORMAT CSV,
                DELIMITER E'\t',
                HEADER TRUE,
                NULL 'NULL'
            );
        """,
        file=f,
    )

pg_conn.commit()
pg_conn.close()

# delete tmp file
os.remove(path=TSV_FILE_PATH)
