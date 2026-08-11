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
class CreateAction:
    enable: bool
    dates: list[str]


@dataclass(frozen=True)
class UpdateAction:
    enable: bool
    current_timestamp: str
    new_timestamp: str


@dataclass(frozen=True)
class DeleteAction:
    enable: bool
    dates: list[str]


@dataclass(frozen=True)
class Config:
    dry_run: bool
    schema: str
    table_to_include: list[str]
    create: CreateAction
    update: UpdateAction
    delete: DeleteAction


def get_partitions(schema: str, curseur: extensions.cursor) -> list[tuple[str, ...]]:
    curseur.execute(query=f"""
        SELECT
            child.relname AS partition_name,
            parent.relname AS parent_table,
            child.relnamespace::regnamespace::text AS schema_name
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace nmsp_parent ON parent.relnamespace = nmsp_parent.oid
        WHERE nmsp_parent.nspname = '{schema}'
        AND child.relispartition = true
        ORDER BY parent.relname, child.relname;
        """)
    return curseur.fetchall()


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
    range_start: datetime,
    range_end: datetime,
    cursor: extensions.cursor,
    dry_run: bool = True,
) -> None:
    custom_logger.info(msg=f"{len(tbl_names)} table(s) trouvée(s)")

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


def update_import_timestamp(
    tbl_names: list[tuple[Any, ...]],
    current_import_timestamp: datetime,
    new_import_timestamp: datetime,
    cursor: extensions.cursor,
    dry_run: bool = True,
) -> None:
    custom_logger.info(msg=f"{len(tbl_names)} table(s) trouvée(s)")

    updated_count = 0
    for tbl_name, schema in tbl_names:
        custom_logger.info(msg=f"Updating table {tbl_name}.")
        create_query = f"""
            UPDATE {schema}.{tbl_name}
            SET import_timestamp = '{new_import_timestamp}',
            WHERE import_timestamp = '{current_import_timestamp}';
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
    partitions: list[tuple[Any, ...]],
    cursor: extensions.cursor,
    dry_run: bool = True,
    dates: list[datetime] | None = None,
) -> None:
    """
    Supprime toutes les partitions d'un schéma spécifique.

    Args:
        connection_params (dict): Paramètres de connexion PostgreSQL
        schema_name (str): Nom du schéma
        dry_run (bool): Si True, affiche les commandes sans les exécuter
    """
    custom_logger.info(msg=f"{len(partitions)} partition(s) trouvée(s)")

    if dates is not None:
        partitions = [
            partition
            for partition in partitions
            if any(substring.strftime(format="%Y%m%d") in partition[0] for substring in dates)
        ]

    # Suppression des partitions
    dropped_count = 0
    for partition_name, _parent_table, schema in partitions:
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
        custom_logger.info(msg=f"\n{dropped_count}/{len(partitions)} partition(s) supprimée(s) avec succès")
    else:
        custom_logger.info(msg=f"\n[DRY RUN] {len(partitions)} partition(s) seraient supprimées")

    cursor.close()


if __name__ == "__main__":
    dir = os.path.dirname(os.path.realpath(__file__))
    config_path = Path(dir, "config.json")

    # Load config
    with open(file=config_path) as f:
        _config = json.load(fp=f)
        config = Config(
            dry_run=_config.get("dry_run", True),
            schema=_config.get("schema"),
            table_to_include=_config.get("table_to_include", []),
            create=CreateAction(**_config.get("create", {})),
            update=UpdateAction(**_config.get("update", {})),
            delete=DeleteAction(**_config.get("delete", {})),
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

    # Créer les nouvelles partitions si nécessaire
    if config.create.enable:
        for _, str_date in enumerate(config.create.dates):
            try:
                start_date = datetime.strptime(str_date, "%Y-%m-%d")
                end_date = start_date + timedelta(days=1)
                create_partitions(
                    tbl_names=tables,
                    range_start=start_date,
                    range_end=end_date,
                    cursor=pg_cur,
                    dry_run=config.dry_run,
                )
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                custom_logger.info(
                    msg=f"✗ Erreur lors de la création de partitions dans le schéma {config.schema}: {e}"
                )

    # Mettre à jour les timestamps
    if config.update.enable:
        try:
            current_timestamp = datetime.strptime(config.update.current_timestamp, "%Y-%m-%d %H:%M:%S")
            new_timestamp = datetime.strptime(config.update.new_timestamp, "%Y-%m-%d %H:%M:%S")

            update_import_timestamp(
                tbl_names=tables,
                current_import_timestamp=current_timestamp,
                new_import_timestamp=new_timestamp,
                cursor=pg_cur,
                dry_run=config.dry_run,
            )
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            custom_logger.info(
                msg=f"✗ Erreur lors de la mise à jour des timestamps dans le schéma {config.schema}: {e}"
            )

    # Supprimer les partitions
    if config.delete.enable:
        # Récupérer la liste des partitions
        _partitions = get_partitions(schema=config.schema, curseur=pg_cur)
        partitions = [partition for partition in _partitions if partition[0].startswith(tuple(config.table_to_include))]
        custom_logger.info(msg=f"Partitions filtrées dans le schéma {config.schema}: {partitions}")
        dates = [datetime.strptime(str_date, "%Y-%m-%d") for str_date in config.delete.dates]
        for _, str_date in enumerate(config.delete.dates):
            try:
                start_date = datetime.strptime(str_date, "%Y-%m-%d")
                drop_partitions(
                    partitions=partitions,
                    cursor=pg_cur,
                    dry_run=config.dry_run,
                    dates=dates,
                )
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                custom_logger.info(
                    msg=f"✗ Erreur lors de la suppression de partitions dans le schéma {config.schema}: {e}"
                )
