import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
from modules.constants import custom_logger
from psycopg2 import extensions, sql
from scripts.settings import get_settings


@dataclass(frozen=True)
class DateMapping:
    current: str
    new: str

    @property
    def current_as_datetime(self) -> datetime:
        return datetime.strptime(self.current, "%Y-%m-%d %H:%M:%S")

    @property
    def new_as_datetime(self) -> datetime:
        return datetime.strptime(self.new, "%Y-%m-%d %H:%M:%S")


@dataclass(frozen=True)
class Config:
    dry_run: bool
    schema: str
    table_to_include: list[str]
    table_to_exclude: list[str]
    id_projet: int
    dates: list[DateMapping]
    run_create: bool
    run_update: bool
    run_delete: bool


def list_table_names(schema: str, curseur: extensions.cursor) -> list[tuple[str, ...]]:
    curseur.execute(query=f"""
        SELECT c.relname AS table_name, c.relnamespace::regnamespace::text AS schema_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}'
        AND c.relkind IN ('r', 'p')  -- 'r' = table normale, 'p' = table partitionnée
        AND NOT c.relispartition;     -- Exclut les partitions enfants
    """)
    return curseur.fetchall()


def create_partitions(
    tbl_names: list[tuple[Any, ...]],
    partition_start_date: datetime,
    cursor: extensions.cursor,
    dry_run: bool = True,
) -> None:
    custom_logger.info(msg=f"{len(tbl_names)} table(s) trouvée(s)")
    range_start = partition_start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    range_end = range_start + timedelta(days=1)

    created_count = 0
    for tbl_name, schema in tbl_names:
        # Nom de la partition : parenttable_YYYY_MM
        partition_name = "_".join(
            [
                tbl_name,
                range_start.strftime(format="%Y%m%d"),
                range_end.strftime(format="%Y%m%d"),
            ]
        )

        custom_logger.info(msg=f"Creating partition {partition_name} for {tbl_name}.")
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{partition_name}
            PARTITION OF {schema}.{tbl_name}
            FOR VALUES FROM ('{range_start}') TO ('{range_end}');
        """

        if dry_run:
            custom_logger.info(msg=f"[DRY RUN] {create_query}")
        else:
            cursor.execute(query=create_query)
            custom_logger.info(msg=f"✓ Partition {partition_name} created successfully.")
            created_count += 1

    if not dry_run:
        custom_logger.info(msg=f"\n{created_count}/{len(tbl_names)} partitions(s) créé(s) avec succès")
    else:
        custom_logger.info(msg=f"\n[DRY RUN] {len(tbl_names)} partitions(s) seraient créées")


def update_import_timestamp_in_versioning(
    id_projet: int,
    current_import_timestamp: datetime,
    new_import_timestamp: datetime,
    cursor: extensions.cursor,
    dry_run: bool = True,
) -> None:
    custom_logger.info(msg=f"Updating import timestamp for project {id_projet}.")

    update_query = f"""
        UPDATE versioning.snapshot
        SET import_timestamp = '{new_import_timestamp}', import_date = '{new_import_timestamp.date()}'
        WHERE id_projet = {id_projet}
        AND import_timestamp = '{current_import_timestamp}';
    """

    if dry_run:
        custom_logger.info(msg=f"[DRY RUN] {update_query}")
    else:
        cursor.execute(query=update_query)
        custom_logger.info(msg=f"✓ Project {id_projet} updated successfully.")


def update_import_timestamp(
    tbl_names: list[tuple[Any, ...]],
    cursor: extensions.cursor,
    id_projet: int,
    dry_run: bool = True,
) -> None:
    custom_logger.info(msg=f"{len(tbl_names)} table(s) trouvée(s)")

    updated_count = 0
    for tbl_name, schema in tbl_names:
        custom_logger.info(msg=f"Updating table {tbl_name}.")
        create_query = f"""
            UPDATE {schema}.{tbl_name} current
            SET import_timestamp = tmp_snap.import_timestamp
            FROM (
                SELECT import_timestamp, snapshot_id
                FROM versioning."snapshot" s
                WHERE id_projet={id_projet}) tmp_snap
            WHERE current.snapshot_id = tmp_snap.snapshot_id;
        """

        if dry_run:
            custom_logger.info(msg=f"[DRY RUN] {create_query}")
        else:
            cursor.execute(query=create_query)
            custom_logger.info(msg=f"✓ Table {tbl_name} updated successfully.")
            updated_count += 1

    if not dry_run:
        custom_logger.info(msg=f"\n{updated_count}/{len(tbl_names)} table(s) mise(s) à jour avec succès")
    else:
        custom_logger.info(msg=f"\n[DRY RUN] {len(tbl_names)} table(s) seraient misees à jour")


def drop_partitions(
    tbl_names: list[tuple[Any, ...]],
    cursor: extensions.cursor,
    partition_start_date: datetime,
    dry_run: bool = True,
) -> None:
    """
    Supprime toutes les partitions d'un schéma spécifique.

    Args:
        tbl_names (list[tuple[Any, ...]]): Liste des tables et schémas
        cursor (extensions.cursor): Curseur de la base de données
        partition_start_date (datetime): Date de début de la partition
        dry_run (bool): Si True, affiche les commandes sans les exécuter
    """
    range_start = partition_start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    range_end = range_start + timedelta(days=1)

    # Suppression des partitions
    dropped_count = 0
    for tbl_name, schema in tbl_names:
        # Nom de la partition : parenttable_YYYY_MM
        partition_name = "_".join(
            [
                tbl_name,
                range_start.strftime(format="%Y%m%d"),
                range_end.strftime(format="%Y%m%d"),
            ]
        )

        drop_query = sql.SQL(string="DROP TABLE IF EXISTS {}.{} CASCADE").format(
            sql.Identifier(schema), sql.Identifier(partition_name)
        )
        if dry_run:
            custom_logger.info(msg=f"[DRY RUN] {drop_query.as_string(context=cursor)}")
        else:
            cursor.execute(query=drop_query)
            custom_logger.info(msg=f"✓ Supprimée: {schema}.{partition_name}")

        dropped_count += 1

    if not dry_run:
        custom_logger.info(msg=f"\n{dropped_count} partition(s) supprimée(s) avec succès")
    else:
        custom_logger.info(msg=f"\n[DRY RUN] {dropped_count} partition(s) seraient supprimées")


if __name__ == "__main__":
    dir = os.path.dirname(os.path.realpath(__file__))
    config_path = Path(dir, "config.json")

    # Load config
    with open(file=config_path) as f:
        _config = json.load(fp=f)
        config = Config(
            dry_run=_config.get("dry_run", True),
            schema=_config.get("schema"),
            table_to_include=_config.get("table_to_include"),
            table_to_exclude=_config.get("table_to_exclude"),
            id_projet=_config.get("id_projet"),
            dates=[DateMapping(**date_mapping) for date_mapping in _config.get("dates", [])],
            run_create=_config.get("run_create", False),
            run_update=_config.get("run_update", False),
            run_delete=_config.get("run_delete", False),
        )
    settings = get_settings()

    # Connect to database
    pg_conn = psycopg2.connect(
        host=settings.db.host,
        port=settings.db.port,
        dbname=settings.db.name,
        user=settings.db.user,
        password=settings.db.password,
    )
    pg_cur = pg_conn.cursor()

    # Récupérer les tables concernées
    all_tables = list_table_names(schema=config.schema, curseur=pg_cur)
    tables = [tbl for tbl in all_tables if tbl[0].startswith(tuple(config.table_to_include))]
    tables = [tbl for tbl in tables if not tbl[0].startswith(tuple(config.table_to_exclude))]
    custom_logger.info(msg=f"{len(tables)} Tables to process: {[tbl[0] for tbl in tables]}")

    # Créer les nouvelles partitions si nécessaire
    pg_cur.execute(query="SET session_replication_role = replica;")
    for _, date_mapping in enumerate(config.dates):
        curr_date = date_mapping.current_as_datetime
        new_date = date_mapping.new_as_datetime

        custom_logger.info(
            msg=f"[{_ + 1} / {len(config.dates)}] Processing date mapping: {date_mapping.current} -> {date_mapping.new}"
        )
        if config.run_create:
            create_partitions(
                tbl_names=tables,
                partition_start_date=new_date,
                cursor=pg_cur,
                dry_run=config.dry_run,
            )
            pg_conn.commit()

        # Mettre à jour les timestamps
        if config.run_update:
            update_import_timestamp_in_versioning(
                id_projet=config.id_projet,
                current_import_timestamp=curr_date,
                new_import_timestamp=new_date,
                cursor=pg_cur,
                dry_run=config.dry_run,
            )
            update_import_timestamp(
                tbl_names=tables,
                cursor=pg_cur,
                id_projet=config.id_projet,
                dry_run=config.dry_run,
            )
            pg_conn.commit()

    # We do the delete part once everythin is created and updated, to avoid any issues with foreign keys
    for _, date_mapping in enumerate(config.dates):
        curr_date = date_mapping.current_as_datetime
        new_date = date_mapping.new_as_datetime

        custom_logger.info(
            msg=f"[{_ + 1} / {len(config.dates)}] Processing date mapping: {date_mapping.current} -> {date_mapping.new}"
        )
        if config.run_delete:
            try:
                drop_partitions(
                    tbl_names=tables,
                    cursor=pg_cur,
                    partition_start_date=curr_date,
                    dry_run=config.dry_run,
                )
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                custom_logger.info(
                    msg=f"✗ Erreur lors de la suppression de partitions dans le schéma {config.schema}: {e}"
                )
    pg_cur.execute(query="SET session_replication_role = DEFAULT;")
    pg_cur.close()
    pg_conn.close()
