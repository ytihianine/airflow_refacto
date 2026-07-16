import logging

import pandas as pd
from airflow.sdk import Variable
from dags.sg.siep.mmsi.api_operat.config import BASE_URL, ID_STRUCTURES
from modules.constants import AGENT, PROXY
from modules.infra.http_client.adapters import ClientConfig, HttpxClient


# ================
# API Fonctions
# ================
def build_header() -> dict[str, str]:
    return {
        "Content-type": "application/json",
        "client_id": Variable.get(key="client_id_api_operat"),
        "client_secret": Variable.get(key="client_secret_api_operat"),
    }


def get_token(
    api_client: HttpxClient,
    url: str,
    id_structure_assujettie: str,
    cle_tiers: str,
    cle_utilisateur: str,
) -> str:
    endpoint = "/api/v1/operat/authentification"

    body = {
        "cleTiers": cle_tiers,
        "idStructureAssujettie": id_structure_assujettie,
        "cleUtilisateur": cle_utilisateur,
    }
    headers = build_header()

    result = api_client.post(endpoint=url + endpoint, json=body, headers=headers)

    return result.json()["token"]


def get_liste_declarations(api_client: HttpxClient, url: str, token: str) -> dict:
    endpoint = "/api/v1/operat/consommations"
    headers = build_header() | {"Authorization": f"Bearer {token}"}

    result = api_client.get(endpoint=url + endpoint, headers=headers)
    try:
        return result.json()
    except Exception:
        return {"resultat": [{"idConsommation": -1}]}


def get_consommation_by_id(api_client: HttpxClient, url: str, token: str, id_consommation: str) -> dict:
    endpoint = "/api/v1/operat/consommation/"
    full_url = url + endpoint + id_consommation
    headers = build_header() | {"Authorization": f"Bearer {token}"}

    result = api_client.get(endpoint=full_url, headers=headers)
    return result.json()


# ================
# Fonctions de processing pour les tâches
# ================
def liste_declaration(cle_tiers: str | None = None, cle_utilisateur: str | None = None) -> pd.DataFrame:
    if cle_tiers is None:
        cle_tiers = str(Variable.get(key="cle_tiers_api_operat"))
    if cle_utilisateur is None:
        cle_utilisateur = str(Variable.get(key="cle_utilisateur_api_operat"))

    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    httpx_internet_client = HttpxClient(config=client_config)

    # Main part
    api_result = []
    for idx, id_structure in enumerate(ID_STRUCTURES):
        logging.info(
            msg=f"({idx+1}/{len(ID_STRUCTURES)}) Récupération des déclarations pour la structure {id_structure}"
        )
        token = get_token(
            api_client=httpx_internet_client,
            url=BASE_URL,
            id_structure_assujettie=id_structure,
            cle_tiers=cle_tiers,
            cle_utilisateur=cle_utilisateur,
        )
        lst_declarations = get_liste_declarations(api_client=httpx_internet_client, url=BASE_URL, token=token)
        result_with_structure = [
            result | {"id_structure": id_structure} for result in lst_declarations.get("resultat", [])
        ]
        api_result.extend(result_with_structure)

    df = pd.DataFrame(data=api_result)
    return df


def consommation_by_id(
    df: pd.DataFrame, cle_tiers: str | None = None, cle_utilisateur: str | None = None
) -> pd.DataFrame:
    if cle_tiers is None:
        cle_tiers = str(Variable.get(key="cle_tiers_api_operat"))
    if cle_utilisateur is None:
        cle_utilisateur = str(Variable.get(key="cle_utilisateur_api_operat"))

    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    httpx_internet_client = HttpxClient(config=client_config)

    # Récupérer le token pour chaque structure
    _token_registry = {}
    for idx, id_structure in enumerate(ID_STRUCTURES):
        logging.info(msg=f"({idx+1}/{len(ID_STRUCTURES)}) Récupération du token pour la structure {id_structure}")
        _token_registry[id_structure] = get_token(
            api_client=httpx_internet_client,
            url=BASE_URL,
            id_structure_assujettie=id_structure,
            cle_tiers=cle_tiers,
            cle_utilisateur=cle_utilisateur,
        )

    api_result = []
    for index, row in df.iterrows():
        logging.info(
            msg=f"({index+1}/{len(df)}) Récupération des consommations pour la structure {row["id_structure"]} - idConso : {row["idConsommation"]}"
        )
        if row["idConsommation"] == -1:
            logging.warning(msg="Aucune idConsommation pour cette structure")
        else:
            detail_conso = get_consommation_by_id(
                api_client=httpx_internet_client,
                url=BASE_URL,
                token=_token_registry[row["id_structure"]],
                id_consommation=str(row["idConsommation"]),
            )
            # print(detail_conso)
            api_result.append(detail_conso)

    df = pd.DataFrame(data=api_result)
    return df
