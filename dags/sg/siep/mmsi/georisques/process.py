import pandas as pd


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
