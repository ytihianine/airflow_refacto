from airflow.decorators import task
from airflow.models import Variable

from utils.file_handler import MinioFileHandler
from utils.api_client.base import AbstractApiClient

from utils.config.tasks import get_storage_rows
from utils.df_utility import df_info

from dags.sg.siep.mmsi.api_operat.process import (
    split_declaration_and_adresse_efa,
    process_declarations,
    process_adresse_efa,
    process_detail_conso,
    process_detail_conso_activite,
    process_detail_conso_indicateur,
)


def build_header() -> dict[str, str]:
    return {
        "Content-type": "application/json",
        "client_id": Variable.get("client_id_api_operat"),
        "client_secret": Variable.get("client_secret_api_operat"),
    }


def get_token(api_client: AbstractApiClient, url: str) -> str:
    endpoint = "/api/v1/operat/authentification"
    id_structure_assujettie = "ETAT_MIN_EF"
    # [
    #     'ETAT_MIN_EF', 'ETAT_REG_ARA', 'ETAT_REG_BFC', 'ETAT_REG_BRE', 'ETAT_REG_COR',
    #     'ETAT_REG_CVL', 'ETAT_REG_GES', 'ETAT_REG_GUA', 'ETAT_REG_GUF', 'ETAT_REG_HDF',
    #     'ETAT_REG_IDF', 'ETAT_REG_LRE', 'ETAT_REG_MAY', 'ETAT_REG_MTQ', 'ETAT_REG_NAQ',
    #     'ETAT_REG_NOR', 'ETAT_REG_OCC', 'ETAT_REG_PAC', 'ETAT_REG_PDL', '194416186',
    #     '313320244', '195726476', '195936489', '180092025', '180080012', '130014228',
    #     '383181575', '197534936', '775665912', '451930051', '180053027'
    # ]

    body = {
        "cleTiers": Variable.get("cle_tiers_api_operat"),
        "idStructureAssujettie": id_structure_assujettie,
        "cleUtilisateur": Variable.get("cle_utilisateur_api_operat"),
    }
    headers = build_header()

    result = api_client.post(endpoint=url + endpoint, json=body, headers=headers)

    return result["token"]


def get_liste_declarations(api_client: AbstractApiClient, url: str, token: str) -> None:
    endpoint = "/api/v1/operat/consommations"
    headers = build_header() | {"Authorization": f"Bearer {token}"}

    result = api_client.get(endpoint=url + endpoint, headers=headers)

    return result


def get_consommation_by_id(
    api_client: AbstractApiClient, url: str, token: str, id_consommation: str
) -> dict:
    endpoint = "/api/v1/operat/consommation/"
    full_url = url + endpoint + id_consommation
    headers = build_header() | {"Authorization": f"Bearer {token}"}

    result = api_client.get(endpoint=full_url, headers=headers)
    return result


@task(task_id="liste_declaration")
def liste_declaration(nom_projet: str, url: str) -> None:
    import pandas as pd
    from utils.common.vars import PROXY, AGENT
    from utils.api_client.adapters import HttpxAPIClient

    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")
    # Http client
    httpx_internet_client = HttpxAPIClient(proxy=PROXY, user_agent=AGENT)
    # Storage paths
    storage_paths = get_storage_rows(nom_projet=nom_projet)

    # Main part
    token = get_token(api_client=httpx_internet_client, url=url)
    declarations = get_liste_declarations(
        api_client=httpx_internet_client, url=url, token=token
    )
    declarations, adresses_efa = split_declaration_and_adresse_efa(
        declarations=declarations["resultat"]
    )
    df_declarations = process_declarations(pd.DataFrame(data=declarations))
    df_adresses_efa = process_adresse_efa(pd.DataFrame(data=adresses_efa))

    df_info(df=df_declarations, df_name="Déclarations")
    df_info(df=df_adresses_efa, df_name="Adresses EFA")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_declarations.to_parquet(path=None, index=False),
        key=storage_paths.loc[
            storage_paths["selecteur"] == "declaration_ademe", "filepath_tmp_s3"
        ]
        + ".parquet",
        replace=True,
    )
    s3_hook.load_bytes(
        bytes_data=df_adresses_efa.to_parquet(path=None, index=False),
        key=storage_paths.loc[
            storage_paths["selecteur"] == "adresse_efa", "filepath_tmp_s3"
        ]
        + ".parquet",
        replace=True,
    )


@task(task_id="consommation_by_id")
def consommation_by_id(nom_projet: str, url: str) -> None:
    from utils.common.vars import PROXY, AGENT
    from utils.api_client.adapters import HttpxAPIClient

    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")
    # Http client
    httpx_internet_client = HttpxAPIClient(proxy=PROXY, user_agent=AGENT)
    # Storage paths
    storage_paths = get_storage_rows(nom_projet=nom_projet)

    # Main part
    token = get_token(api_client=httpx_internet_client, url=url)
    df_declarations = s3_hook.read_parquet(
        file_name=storage_paths.loc[
            storage_paths["selecteur"] == "declaration_ademe", "filepath_s3"
        ]
    )

    conso_ids = df_declarations["id_consommation"]

    conso_detail = []
    conso_activite = []
    conso_indicateur = []
    for id_conso in conso_ids:
        detail_conso = get_consommation_by_id(
            api_client=httpx_internet_client,
            url=url,
            token=token,
            id_consommation=str(id_conso),
        )
        lst_activite = [
            activite | {"id_consommation": id_conso}
            for activite in detail_conso["detail"].pop("activites", [])
        ]
        lst_indicateur = [
            indicateur | {"id_consommation": id_conso}
            for indicateur in detail_conso["detail"].pop("indicateurs", [])
        ]
        conso_activite.extend(lst_activite)
        conso_indicateur.extend(lst_indicateur)
        conso_detail.append(detail_conso["detail"] | {"id_consommation": id_conso})

    df_activite = process_detail_conso_activite(raw_data=conso_activite)
    df_indicateur = process_detail_conso_indicateur(raw_data=conso_indicateur)
    df_detail = process_detail_conso(raw_data=conso_detail)

    df_info(df=df_detail, df_name="Détail de la consommation")
    df_info(df=df_activite, df_name="Détail activité")
    df_info(df=df_indicateur, df_name="Détail indicateur")

    # Export files
    s3_hook.load_bytes(
        bytes_data=df_activite.to_parquet(path=None, index=False),
        key=storage_paths.loc[
            storage_paths["selecteur"] == "activite", "filepath_tmp_s3"
        ]
        + ".parquet",
        replace=True,
    )
    s3_hook.load_bytes(
        bytes_data=df_indicateur.to_parquet(path=None, index=False),
        key=storage_paths.loc[
            storage_paths["selecteur"] == "indicateur", "filepath_tmp_s3"
        ]
        + ".parquet",
        replace=True,
    )
    s3_hook.load_bytes(
        bytes_data=df_detail.to_parquet(path=None, index=False),
        key=storage_paths.loc[storage_paths["selecteur"] == "detail", "filepath_tmp_s3"]
        + ".parquet",
        replace=True,
    )
