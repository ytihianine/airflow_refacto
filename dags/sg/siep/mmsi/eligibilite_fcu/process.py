import pandas as pd
from infra.http_client.base import AbstractApiClient


def can_perform_api_call(lat: float, lon: float) -> bool:
    """_summary_

    Args:
        lat (float): latitude
        lon (float): longitude

    Returns:
        bool: True if both lat and lon are defined
    """
    if isinstance(lat, float) and isinstance(lon, float):
        return True

    return False


def process_result(df: pd.DataFrame) -> pd.DataFrame:
    cols_mapping = {
        "isEligible": "is_eligible",
        "distance": "distance",
        "inPDP": "in_pdp",
        "isBasedOnIris": "is_based_on_iris",
        "futurNetwork": "futur_network",
        "id": "id_fcu",
        "name": "name",
        "gestionnaire": "gestionnaire_fcu",
        "rateENRR": "rate_enrr",
        "rateCO2": "rate_co2",
    }

    df = df.rename(columns=cols_mapping)
    df["name"] = df["name"].str.split().str.join(" ")
    df["gestionnaire_fcu"] = df["gestionnaire_fcu"].str.split().str.join(" ")

    return df.convert_dtypes()


def get_eligibilite_fcu(
    api_client: AbstractApiClient, url: str, latitude: float, longitude: float
) -> dict[str, str]:
    result_json = {
        "api_status": None,
        "api_status_code": None,
        "api_raison": None,
    }

    if not can_perform_api_call(lat=latitude, lon=longitude):
        result_json["api_status"] = "Echec"
        result_json["api_raison"] = "Missing or invalid latitude/longitude"
        return result_json

    full_url = url + f"?lat={latitude}&lon={longitude}"
    response = api_client.get(full_url)

    if response.status_code == 200:
        result_json["api_status"] = "Succès"
        result_json["api_status_code"] = response.status_code
        result_json["api_raison"] = None
        result_json.update(response.json())
        return result_json
    else:
        result_json["api_status"] = "Echec"
        result_json["api_status_code"] = response.status_code
        result_json["api_raison"] = response.text
        return result_json
