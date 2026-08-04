from dataclasses import dataclass

from airflow.sdk import Variable
from modules.infra.http_client.adapters import HttpxClient


@dataclass(frozen=True)
class ApiOperat:
    """Class to hold the credentials for the API Operat."""

    # Credentials for the API Operat
    client_id_api_operat: str | None = None
    client_secret_api_operat: str | None = None
    cle_tiers: str | None = None
    cle_utilisateur: str | None = None

    # Endpoints for the API Operat
    base_url: str = "https://api-externe.ademe.fr"
    endpoint_authentification: str = "/api/v1/operat/authentification"
    endpoint_consommations: str = "/api/v1/operat/consommations"
    endpoint_consommation_by_id: str = "/api/v1/operat/consommation/"

    def get_client_id_api_operat(self) -> str:
        if self.client_id_api_operat is None:
            return str(Variable.get(key="client_id_api_operat"))
        return self.client_id_api_operat

    def get_client_secret_api_operat(self) -> str:
        if self.client_secret_api_operat is None:
            return str(Variable.get(key="client_secret_api_operat"))
        return self.client_secret_api_operat

    def get_cle_tiers(self) -> str:
        if self.cle_tiers is None:
            return str(Variable.get(key="cle_tiers"))
        return self.cle_tiers

    def get_cle_utilisateur(self) -> str:
        if self.cle_utilisateur is None:
            return str(Variable.get(key="cle_utilisateur"))
        return self.cle_utilisateur

    def build_header(self, token: str | None = None) -> dict[str, str]:
        header = {
            "Content-type": "application/json",
            "client_id": self.get_client_id_api_operat(),
            "client_secret": self.get_client_secret_api_operat(),
        }
        if token is not None:
            header["Authorization"] = f"Bearer {token}"
        return header

    def get_token(
        self,
        api_client: HttpxClient,
        id_structure_assujettie: str,
    ) -> str:
        endpoint = self.endpoint_authentification

        body = {
            "cleTiers": self.get_cle_tiers(),
            "idStructureAssujettie": id_structure_assujettie,
            "cleUtilisateur": self.get_cle_utilisateur(),
        }
        headers = self.build_header()

        result = api_client.post(endpoint=self.base_url + endpoint, json=body, headers=headers)

        return result.json()["token"]
