import os
from datetime import datetime, timedelta
import psycopg2


def get_tbl_names(schema: str, curseur) -> list[str]:
    curseur.execute(
        f"""
        SELECT DISTINCT
            pn.nspname AS schema_name,
            pc.relname AS parent_table
        FROM pg_inherits i
        JOIN pg_class pc ON pc.oid = i.inhparent
        JOIN pg_namespace pn ON pn.oid = pc.relnamespace
        WHERE pn.nspname = '{schema}'
        AND pc.relkind = 'p'   -- 'p' = table partitionnée
        ORDER BY pn.nspname, pc.relname;
        """
    )
    tbl_names = pg_cur.fetchall()
    return [tbl[1] for tbl in tbl_names]


def create_partition(schema: str, tbl: str, range_start: datetime, range_end: datetime, curseur) -> None:
    # Nom de la partition : parenttable_YYYY_MM
    partition_name = (
        f"{tbl_name}_{range_start.strftime('%Y%m%d')}_{range_end.strftime('%Y%m%d')}"
    )

    try:
        print(f"Creating partition {partition_name} for {tbl_name}.")
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{partition_name}
            PARTITION OF {schema}.{tbl_name}
            FOR VALUES FROM ('{range_start}') TO ('{range_end}');
        """
        pg_cur.execute(create_sql)
        print(f"Partition {partition_name} created successfully.")
    except psycopg2.errors.DuplicateTable:
        print(
            f"Partition {partition_name} already exists. Skipping creation."
        )
    except Exception as e:
        print(f"Error creating partition {partition_name}: {str(e)}")
        raise


ENV = os.environ.copy()
schema = "siep"
pg_conn = psycopg2.connect(
    host=ENV["CONFIG_DB_HOST"],
    port=ENV["CONFIG_DB_PORT"],
    dbname=ENV["CONFIG_DB_NAME"],
    user=ENV["CONFIG_DB_USER"],
    password=ENV["CONFIG_DB_PASSWORD"],
)
pg_cur = pg_conn.cursor()

# Récupérer la liste des tables
tbl_names = get_tbl_names(schema=schema, curseur=pg_cur)


# Définir la date de début et de fin pour la range de la partition
from_date = datetime.strptime('31/07/2025 00:00:00', '%d/%m/%Y %H:%M:%S')
to_date = from_date + timedelta(days=1)
print(from_date, to_date)

# Créer les partitions
for tbl in tbl_names:
    tbl_name = tbl
    create_partition(
        schema=schema,
        tbl=tbl,
        range_start=from_date,
        range_end=to_date,
        curseur=pg_cur
    )


pg_conn.commit()
pg_cur.close()
pg_conn.close()
