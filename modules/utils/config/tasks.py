"""Functions for retrieving and managing project configurations."""

import logging
from collections.abc import Mapping

import pandas as pd
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from modules.constants import DEFAULT_PG_DATA_CONN_ID
from modules.infra.database.base import DBInterface
from modules.infra.database.exceptions import DatabaseError
from modules.infra.database.factory import create_db_handler
from modules.types.projet import (
    Contact,
    Documentation,
    ProjetS3,
    SelecteurConfig,
    SelecteurStorageInfo,
    SelecteurStorageOptions,
)
from modules.utils.exceptions import ConfigError

CONF_SCHEMA = "conf_projets"
logger = logging.getLogger(name=__name__)


def _get_db(db: DBInterface | None = None) -> DBInterface:
    """Return the provided db handler or create the default one."""
    if db is not None:
        return db
    return create_db_handler(connection_id=DEFAULT_PG_DATA_CONN_ID)


# Configuration du retry decorator
db_retry = retry(
    retry=retry_if_exception_type(exception_types=(ConnectionError, TimeoutError, DatabaseError, OSError)),
    stop=stop_after_attempt(max_attempt_number=3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    before_sleep=before_sleep_log(logger, log_level=logging.WARNING),
    reraise=True,
)


@db_retry
def column_mapping_dataframe(
    nom_projet: str,
    selecteur: str,
    db: DBInterface | None = None,
) -> pd.DataFrame:
    """
    Permet de récupérer la correspondance des colonnes entre
    le fichier source et la sortie.
    Output:
        Dataframe
        Columns: selecteur, colname_source, colname_dest
    """
    db = _get_db(db)

    df = db.fetch_df(
        query=f"""
            SELECT cpcm.projet, cpcm.selecteur, cpcm.colname_source, cpcm.colname_dest
            FROM {CONF_SCHEMA}.cols_mapping_vw cpcm
            WHERE cpcm.projet = %s AND cpcm.selecteur = %s AND rang = 1;
        """,
        parameters=(nom_projet, selecteur),
    )
    return df


def column_mapping_dict(df_cols_map: pd.DataFrame, selecteur: str | None = None) -> dict:
    logger.debug("Colonnes du dataframe de mapping: %s", df_cols_map.columns.tolist())
    logger.debug(
        "Selecteurs du dataframe de mapping: %s",
        df_cols_map["selecteur"].unique().tolist(),
    )
    if selecteur is not None:
        df_cols_map = df_cols_map.loc[df_cols_map["selecteur"] == selecteur]

    records = df_cols_map.set_index("colname_source")["colname_dest"].to_dict(into=dict)
    return records


@db_retry
def get_list_contact(nom_projet: str, db: DBInterface | None = None) -> list[Contact]:
    if not nom_projet:
        raise ValueError(
            "Variable nom_projet is required to fetch contact information. Current value is None or empty."
        )

    db = _get_db(db)

    query = f"""
        SELECT cppc.projet, cppc.contact_mail, cppc.is_mail_generic
        FROM {CONF_SCHEMA}.projet_contact_vw cppc
        WHERE cppc.projet = %s AND rang = 1;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))
    records = df.to_dict("records", into=dict)
    return [Contact(**record) for record in records]


@db_retry
def get_list_documentation(
    nom_projet: str,
    db: DBInterface | None = None,
) -> list[Documentation]:
    if not nom_projet:
        raise ValueError("Variable nom_projet is required to fetch documentation. Current value is None or empty.")

    db = _get_db(db)

    query = f"""
        SELECT cppd.projet, cppd.type_documentation, cppd.lien
        FROM {CONF_SCHEMA}.projet_documentation_vw cppd
        WHERE cppd.projet = %s AND rang = 1;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))
    records = df.to_dict("records", into=dict)
    return [Documentation(**record) for record in records]


@db_retry
def get_projet_s3_info(
    nom_projet: str,
    db: DBInterface | None = None,
) -> ProjetS3:
    """Get S3 configuration for a specific selecteur.

    Args:
        nom_projet: Project name
        db: Optional database handler (defaults to Airflow config connection)

    Returns:
        ProjetS3 object with S3 configuration

    Raises:
        ConfigError: If no S3 configuration is found
    """
    if not nom_projet:
        raise ValueError("Variable nom_projet is required to fetch S3 configuration. Current value is None or empty.")

    db = _get_db(db)

    query = f"""
        SELECT cpps3.projet, cpps3.bucket,
            cpps3.key,
            cpps3.key_tmp
        FROM {CONF_SCHEMA}.projet_s3_vw cpps3
        WHERE cpps3.projet = %s AND rang = 1;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))

    if df.empty:
        raise ConfigError(
            message=f"No S3 configuration found for project {nom_projet}",
            nom_projet=nom_projet,
        )

    record = df.iloc[0].to_dict(into=dict)
    return ProjetS3(**record)  # type: ignore


def merge_selecteur_config(
    storage_info: list[SelecteurStorageInfo],
    storage_options: Mapping[str, SelecteurStorageOptions] | None = None,
) -> list[SelecteurConfig]:
    """Merge SelecteurStorageInfo from DB with local SelecteurStorageOptions config.

    Items like S3 paths and table names come from the DB (via ``storage_info``).
    Per-selecteur behavioural options (write flags, load strategy, partition
    period …) are supplied locally as a plain Python dict so that callers can
    use Enum values directly.  Any selecteur that is not present in
    ``storage_options`` falls back to the ``SelecteurStorageOptions`` defaults.

    Args:
        storage_info: List of SelecteurStorageInfo objects retrieved from the DB.
        storage_options: Optional mapping of selecteur name → SelecteurStorageOptions.
                     Selecteurs absent from the map receive default options.

    Returns:
        List of SelecteurConfig objects combining DB info with local options.
    """
    if storage_options is None:
        logger.info(msg="No storage_options provided to merge_selecteur_config, using empty defaults.")
        storage_options = {}

    return [
        SelecteurConfig.load(
            storage_info=info,
            storage_options=storage_options.get(info.selecteur, SelecteurStorageOptions()),
        )
        for info in storage_info
    ]


@db_retry
def _get_selecteur_storage_info(
    nom_projet: str,
    selecteur: str | None = None,
    local_dir: str = "/tmp",
    only_source: bool = False,
    only_grist: bool = False,
    only_fichier: bool = False,
    db: DBInterface | None = None,
) -> list[SelecteurStorageInfo]:
    """Get SelecteurStorageInfo for a project and optionally a specific selecteur.

    Args:
        nom_projet: Project name
        selecteur: Optional selecteur filter
        local_dir: Local directory used to build local_path (default: /tmp)
        db: Optional database handler (defaults to Airflow config connection)

    Returns:
        List of SelecteurStorageInfo objects
    """
    if not nom_projet:
        raise ValueError(
            "Variable nom_projet is required to fetch selecteur storage info. Current value is None or empty."
        )

    db = _get_db(db)

    query = f"""
        SELECT cpss3db.projet, cpss3db.selecteur, cpss3db.type_source, cpss3db.id_source,
            cpss3db.bucket, cpss3db.s3_key, cpss3db.filename,
            cpss3db.tbl_name
        FROM {CONF_SCHEMA}.selecteur_s3_db_vw cpss3db
        WHERE 1=1 AND cpss3db.projet = %s AND rang = 1
    """

    if only_source:
        query += " AND cpss3db.type_selecteur = 'Source'"

    if only_grist:
        query += " AND cpss3db.type_source = 'Grist'"

    if only_fichier:
        query += " AND cpss3db.type_source = 'Fichier'"

    params: list[str] = [nom_projet]

    if selecteur is not None:
        query += " AND cpss3db.selecteur = %s"
        params.append(selecteur)

    query += " ORDER BY cpss3db.projet;"

    df = db.fetch_df(query, parameters=tuple(params))

    if df.empty:
        return []

    records = df.to_dict("records", into=dict)
    return [SelecteurStorageInfo(**record, local_dir=local_dir) for record in records]


def get_list_selecteur_storage_info(
    nom_projet: str,
    local_dir: str = "/tmp",
) -> list[SelecteurStorageInfo]:
    """Get SelecteurStorageInfo for all selecteurs in a modules.

    Args:
        context: Airflow task context
        nom_projet: Project name
        local_dir: Local directory used to build local_path (default: /tmp)

    Returns:
        List of SelecteurStorageInfo objects for all selecteurs
    """
    return _get_selecteur_storage_info(nom_projet=nom_projet, selecteur=None, local_dir=local_dir)


def get_selecteur_storage_info(
    nom_projet: str,
    selecteur: str,
    local_dir: str = "/tmp",
) -> SelecteurStorageInfo:
    """Get SelecteurStorageInfo for a specific selecteur.

    Args:
        selecteur: Selecteur name
        context: Optional Airflow task context
        nom_projet: Project name
        local_dir: Local directory used to build local_path (default: /tmp)

    Returns:
        SelecteurStorageInfo object

    Raises:
        ConfigError: If no configuration is found
    """
    configs = _get_selecteur_storage_info(nom_projet=nom_projet, selecteur=selecteur, local_dir=local_dir)

    if not configs:
        raise ConfigError(
            message=f"No storage info found for project {nom_projet} and selecteur {selecteur}",
            nom_projet=nom_projet,
            selecteur=selecteur,
        )

    return configs[0]


def get_list_source_fichier(nom_projet: str) -> list[str]:
    """Get SelecteurStorageInfo for all selecteurs with file source."""
    selecteur_storage_info = _get_selecteur_storage_info(nom_projet=nom_projet, only_source=True, only_fichier=True)
    return [info.get_full_s3_key(use_id_source=True) for info in selecteur_storage_info]
