import psycopg2
from modules.constants import custom_logger
from scripts.handle_partitions import config
from scripts.handle_partitions.commun import Actions, get_partitions, list_table_names
from scripts.handle_partitions.partitions import (
    create_partitions,
    drop_partitions,
    update_import_timestamp,
    update_snapshot_id,
)
from scripts.settings import get_settings

# SETTINGS
settings = get_settings()


if __name__ == "__main__":
    # Connect to database
    pg_conn = psycopg2.connect(
        host=settings.db.host,
        port=settings.db.port,
        dbname=settings.db.name,
        user=settings.db.user,
        password=settings.db.password,
    )
    pg_cur = pg_conn.cursor()

    # Action to perform
    if config.action == Actions.DROP:
        try:
            # Récupérer la liste des partitions
            partition_names = get_partitions(schema=config.schema, curseur=pg_cur)
            partition_names = [
                partition for partition in partition_names if partition[1].startswith(config.tbl_to_keep)
            ]
            drop_partitions(
                partitions=partition_names,
                cursor=pg_cur,
                dry_run=config.dry_run,
                from_date=config.drop_from_date,
            )
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            custom_logger.info(msg=f"✗ Erreur lors de la suppression de partitions dans le schéma {config.schema}: {e}")

    if config.action == Actions.CREATE:
        try:
            # Récupérer la liste des tables
            table_names = list_table_names(schema=config.schema, curseur=pg_cur)
            table_names = [tbl for tbl in table_names if tbl[0].startswith(config.tbl_to_keep)]

            create_partitions(
                tbl_names=table_names,
                range_start=config.create_from_date,
                range_end=config.to_date,
                cursor=pg_cur,
                dry_run=config.dry_run,
            )
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            custom_logger.info(msg=f"✗ Erreur lors de la création de partitions dans le schéma {config.schema}: {e}")

    if config.action == Actions.UPDATE_TIMESTAMP:
        try:
            # Récupérer la liste des tables
            table_names = list_table_names(schema=config.schema, curseur=pg_cur)
            table_names = [tbl for tbl in table_names if tbl[0].startswith(config.tbl_to_keep)]

            update_import_timestamp(
                tbl_names=table_names,
                current_import_timestamp=config.curr_import_timestamp,
                new_import_timestamp=config.new_import_timestamp,
                cursor=pg_cur,
                dry_run=config.dry_run,
            )
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            custom_logger.info(msg=f"✗ Erreur lors de la création de partitions dans le schéma {config.schema}: {e}")

    if config.action == Actions.UPDATE_SNAPSHOT:
        try:
            # Récupérer la liste des tables
            table_names = list_table_names(schema=config.schema, curseur=pg_cur)
            table_names = [tbl for tbl in table_names if tbl[0].startswith(config.tbl_to_keep)]

            update_snapshot_id(
                tbl_names=table_names,
                current_snapshot_id=config.current_snapshot_id,
                new_snapshot_id=config.new_snapshot_id,
                cursor=pg_cur,
                dry_run=config.dry_run,
            )
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            custom_logger.info(msg=f"✗ Erreur lors de la création de partitions dans le schéma {config.schema}: {e}")
