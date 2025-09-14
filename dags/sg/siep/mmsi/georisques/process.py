import pandas as pd

from infra.http_client.base import AbstractApiClient


def clean_rename(df: pd.DataFrame) -> pd.DataFrame:
    return df


def format_risque_results(risques: dict[str, str]) -> dict[str, str]:
    formated_risques = []
    code_bat_ter = risques.get("code_bat_ter")

    # Format risques naturels
    for risque_type, risque_data in risques.get("risquesNaturels", {}).items():
        formated_risques.append(
            {
                "code_bat_ter": code_bat_ter,
                "risque_categorie": "Naturel",
                "risque_present": risque_data.get("present"),
                "risque_libelle": risque_data.get("libelle"),
            }
        )

    # Format risques technologiques
    for risque_type, risque_data in risques.get("risquesTechnologiques", {}).items():
        formated_risques.append(
            {
                "code_bat_ter": code_bat_ter,
                "risque_categorie": "Technologique",
                "risque_present": risque_data.get("present"),
                "risque_libelle": risque_data.get("libelle"),
            }
        )

    return formated_risques


def format_query_param(adresse: str, latitude: float, longitude: float) -> str:
    if isinstance(latitude, float) and isinstance(longitude, float):
        return f"latlon={longitude},{latitude}"

    if adresse:
        return f"adresse={adresse}"

    return "no geo data"


def get_risque(
    api_client: AbstractApiClient, url: str, query_param: str
) -> dict[str, str]:
    result_json = {}

    if query_param == "no geo data":
        result_json["statut"] = "Echec"
        result_json["statut_code"] = None
        result_json["raison"] = "no geo data"
        return result_json

    full_url = f"{url}?{query_param}"

    response = api_client.get(full_url, timeout=180)

    if response.status_code == 200:
        result_json["statut"] = "Succès"
        result_json["statut_code"] = response.status_code
        result_json["raison"] = None
        result_json.update(response.json())
        return result_json
    else:
        result_json["statut"] = "Echec"
        result_json["statut_code"] = response.status_code
        result_json["raison"] = response.content
        return result_json
