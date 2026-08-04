import logging

import pandas as pd
from dags.sg.siep.mmsi.api_operat.config import ID_STRUCTURES
from dags.sg.siep.mmsi.api_operat.types import ApiOperat
from modules.constants import AGENT, PROXY
from modules.infra.http_client.adapters import ClientConfig, HttpxClient


# ================
# API Fonctions
# ================
def get_liste_declarations(api_client: HttpxClient, api_operat: ApiOperat, token: str) -> dict:
    route = api_operat.base_url + api_operat.endpoint_consommations

    result = api_client.get(endpoint=route, headers=api_operat.build_header(token=token))
    try:
        return result.json()
    except Exception:
        return {"resultat": [{"idConsommation": -1}]}


def get_consommation_by_id(api_client: HttpxClient, api_operat: ApiOperat, token: str, id_consommation: str) -> dict:
    route = api_operat.base_url + api_operat.endpoint_consommation_by_id + id_consommation
    headers = api_operat.build_header(token=token)

    result = api_client.get(endpoint=route, headers=headers)
    return result.json()


# ================
# Fonctions de processing pour les tâches
# ================
def liste_declaration(api_operat: ApiOperat) -> pd.DataFrame:
    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    httpx_internet_client = HttpxClient(config=client_config)

    # Main part
    api_result = []
    for idx, id_structure in enumerate(ID_STRUCTURES):
        logging.info(
            msg=f"({idx+1}/{len(ID_STRUCTURES)}) Récupération des déclarations pour la structure {id_structure}"
        )
        token = api_operat.get_token(
            api_client=httpx_internet_client,
            id_structure_assujettie=id_structure,
        )
        lst_declarations = get_liste_declarations(api_client=httpx_internet_client, api_operat=api_operat, token=token)
        result_with_structure = [
            result | {"id_structure": id_structure} for result in lst_declarations.get("resultat", [])
        ]
        api_result.extend(result_with_structure)

    df = pd.DataFrame(data=api_result)
    return df


def consommation_by_id(df: pd.DataFrame, api_operat: ApiOperat) -> pd.DataFrame:
    # Http client
    client_config = ClientConfig(user_agent=AGENT, proxy=PROXY)
    httpx_internet_client = HttpxClient(config=client_config)

    # Récupérer le token pour chaque structure
    _token_registry = {}
    for idx, id_structure in enumerate(ID_STRUCTURES):
        logging.info(msg=f"({idx+1}/{len(ID_STRUCTURES)}) Récupération du token pour la structure {id_structure}")
        _token_registry[id_structure] = api_operat.get_token(
            api_client=httpx_internet_client,
            id_structure_assujettie=id_structure,
        )

    api_result = []
    for _index, (_, row) in enumerate(df.iterrows(), start=1):
        logging.info(
            msg=f"({_index}/{len(df)}) Récupération des consommations pour la structure {row['id_structure']} - idConso : {row['idConsommation']}"
        )
        if row["idConsommation"] == -1:
            logging.warning(msg=f"Aucune idConsommation pour la structure {row['id_structure']}")
        else:
            detail_conso = get_consommation_by_id(
                api_client=httpx_internet_client,
                api_operat=api_operat,
                token=_token_registry[row["id_structure"]],
                id_consommation=str(row["idConsommation"]),
            )
            # print(detail_conso)
            api_result.append(detail_conso)

    df = pd.DataFrame(data=api_result)
    return df
