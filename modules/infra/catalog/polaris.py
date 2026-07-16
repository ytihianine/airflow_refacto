from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class PolarisCatalog:
    url: str
    realm: str
    api_management_endpoint: str = "api/management/v1"

    def get_root_access_token(self, client_id: str, client_secret: str) -> str:
        token_endpoint = f"{self.url}/api/catalog/v1/oauth/tokens"
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "PRINCIPAL_ROLE:ALL",
        }
        response = requests.post(url=token_endpoint, data=payload, verify=False)
        response.raise_for_status()
        token = response.json().get("access_token")
        if not token:
            raise ValueError("Failed to obtain access token")
        return token

    def get_catalog_info(self, token: str, catalog_name: str, realm: str | None = None):
        import json

        realm = realm if realm else self.realm
        catalog_endpoint = f"{self.url}/{self.api_management_endpoint}/catalogs/{catalog_name}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": realm,
        }
        response = requests.get(url=catalog_endpoint, headers=headers, verify=False)
        response.raise_for_status()
        print(json.dumps(obj=response.json(), indent=2))
        return response.json()

    def create_catalog(
        self,
        token: str,
        catalog_name: str,
        storage_config: Mapping[str, Any],
        realm: str | None = None,
    ) -> None:
        realm = realm if realm else self.realm
        catalog_endpoint = f"{self.url}/{self.api_management_endpoint}/catalogs"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Polaris-Realm": realm,
        }
        payload = {
            "catalog": {
                "name": catalog_name,
                "type": "INTERNAL",
                "properties": {
                    "default-base-location": "s3://dsci/data_store",
                },
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": ["s3://dsci/data_store"],
                    "endpoint": "https://minio.lab.incubateur.finances.rie.gouv.fr",
                    "s3.access-key-id": None,
                    "s3.secret-access-key": None,
                    "region": "us-east-1",
                    "stsUnavailable": True,
                    "pathStyleAccess": True,  # Required for MinIO
                },
            }
        }
        print(payload)

        response = requests.post(url=catalog_endpoint, headers=headers, json=payload, verify=False)
        response.raise_for_status()

    def delete_catalog(self, token: str, catalog_name: str, realm: str | None = None) -> None:
        realm = realm if realm else self.realm
        delete_endpoint = f"{self.url}/{self.api_management_endpoint}/catalogs/{catalog_name}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": realm,
        }
        response = requests.delete(url=delete_endpoint, headers=headers, verify=False)
        print(response.status_code, response.text)
        response.raise_for_status()

    def create_principal(self, token: str, principal_name: str, realm: str | None = None) -> tuple[str, str]:
        realm = realm if realm else self.realm
        principal_endpoint = f"{self.url}/{self.api_management_endpoint}/principals"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Polaris-Realm": realm,
        }
        payload = {"principal": {"name": principal_name, "properties": {}}}
        response = requests.post(url=principal_endpoint, headers=headers, json=payload, verify=False)
        response.raise_for_status()
        data = response.json()
        return data["credentials"]["clientId"], data["credentials"]["clientSecret"]

    def create_principal_role(self, token: str, role_name: str, realm: str | None = None) -> None:
        realm = realm if realm else self.realm
        role_endpoint = f"{self.url}/{self.api_management_endpoint}/principal-roles"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Polaris-Realm": realm,
        }
        payload = {"principalRole": {"name": role_name, "properties": {}}}
        response = requests.post(url=role_endpoint, headers=headers, json=payload, verify=False)
        response.raise_for_status()

    def create_catalog_role(self, token: str, catalog_name: str, role_name: str, realm: str | None = None):
        realm = realm if realm else self.realm
        role_endpoint = f"{self.url}/{self.api_management_endpoint}/catalogs/{catalog_name}/catalog-roles"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Polaris-Realm": realm,
        }
        payload = {"catalogRole": {"name": role_name, "properties": {}}}
        response = requests.post(url=role_endpoint, headers=headers, json=payload, verify=False)
        response.raise_for_status()

    def assign_principal_role(self, token: str, principal_name: str, role_name: str, realm: str | None = None):
        realm = realm if realm else self.realm
        assign_endpoint = f"{self.url}/{self.api_management_endpoint}/principals/{principal_name}/principal-roles"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Polaris-Realm": realm,
        }
        payload = {"principalRole": {"name": role_name}}
        response = requests.put(url=assign_endpoint, headers=headers, json=payload, verify=False)
        response.raise_for_status()

    def assign_catalog_role(
        self,
        token: str,
        principal_role_name: str,
        catalog_name: str,
        catalog_role_name: str,
        realm: str | None = None,
    ):
        realm = realm if realm else self.realm
        assign_endpoint = f"{self.url}/{self.api_management_endpoint}/principal-roles/{principal_role_name}/catalog-roles/{catalog_name}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Polaris-Realm": realm,
        }
        payload = {"catalogRole": {"name": catalog_role_name}}
        response = requests.put(url=assign_endpoint, headers=headers, json=payload, verify=False)
        response.raise_for_status()

    def grant_catalog_privileges(
        self,
        token: str,
        catalog_name: str,
        catalog_role_name: str,
        realm: str | None = None,
    ) -> None:
        realm = realm if realm else self.realm
        grant_endpoint = f"{self.url}/{self.api_management_endpoint}/catalogs/{catalog_name}/catalog-roles/{catalog_role_name}/grants"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Polaris-Realm": realm,
        }
        payload = {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}
        response = requests.put(url=grant_endpoint, headers=headers, json=payload, verify=False)
        response.raise_for_status()

    def create_table(self) -> None:
        pass

    def write_to_table(self) -> None:
        pass
