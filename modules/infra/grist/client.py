import logging
from dataclasses import dataclass
from typing import Any

import pandas as pd

from modules.infra.http_client.base import HttpInterface
from modules.infra.http_client.types import HTTPResponse
from modules.infra.grist.endpoints import (
    DocsEndpointBuilder,
    OrgsEndpointBuilder,
    WorkspacesEndpointBuilder,
    RecordsEndpointBuilder,
    TablesEndpointBuilder,
    SQLEndpointBuilder,
    WebhooksEndpointBuilder,
)



@dataclass(frozen=True)
class GristClient:
    http_client: HttpInterface
    base_url: str
    workspace_id: str
    doc_id: str
    api_token: str

    # Endpoints
    orgs_endpoint: OrgsEndpointBuilder = OrgsEndpointBuilder()
    workspaces_endpoint: WorkspacesEndpointBuilder = WorkspacesEndpointBuilder()
    doc_endpoint: DocsEndpointBuilder = DocsEndpointBuilder()
    tables_endpoint: TablesEndpointBuilder = TablesEndpointBuilder()
    records_endpoint: RecordsEndpointBuilder = RecordsEndpointBuilder()
    sql_endpoint: SQLEndpointBuilder = SQLEndpointBuilder()
    webhooks_endpoint: WebhooksEndpointBuilder = WebhooksEndpointBuilder()

    def _build_route(self, endpoint: str) -> str:
        """Build the full route for a given endpoint."""
        return f"{self.base_url}/{endpoint}"

    def _build_headers(self, api_token: str | None = None) -> dict[str, str]:
        api_token = api_token if api_token is not None else self.api_token

        if api_token is None:
            raise ValueError("API Token value must be defined at top level or at method level ! ")

        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
            "accept": "*/*",
        }

        return headers

    # ==========================
    # Orgs methods
    # ==========================
    def list_orgs(self) -> HTTPResponse:
        route = self._build_route(endpoint=self.orgs_endpoint.list_orgs())
        headers = self._build_headers()

        response = self.http_client.get(
            endpoint=route,
            headers=headers,
        )
        return response

    def get_org(self, org_id: str) -> HTTPResponse:
        route = self._build_route(endpoint=self.orgs_endpoint.get_org(org_id=org_id))
        headers = self._build_headers()

        response = self.http_client.get(
            endpoint=route,
            headers=headers,
        )
        return response

    def update_org(self, org_id: str, body: dict[str, Any]) -> HTTPResponse:
        route = self._build_route(endpoint=self.orgs_endpoint.update_org(org_id=org_id))
        headers = self._build_headers()

        response = self.http_client.post(
            endpoint=route,
            headers=headers,
            json=body,
        )
        return response

    def delete_org(self, org_id: str, name: str) -> HTTPResponse:
        route = self._build_route(endpoint=self.orgs_endpoint.delete_org(org_id=org_id, name=name))
        headers = self._build_headers()

        response = self.http_client.delete(
            endpoint=route,
            headers=headers,
        )
        return response

    def list_org_access(self, org_id: str) -> HTTPResponse:
        route = self._build_route(endpoint=self.orgs_endpoint.list_org_access(org_id=org_id))
        headers = self._build_headers()

        response = self.http_client.get(
            endpoint=route,
            headers=headers,
        )
        return response

    def update_org_access(self, org_id: str, data: dict[str, Any]) -> HTTPResponse:
        route = self._build_route(endpoint=self.orgs_endpoint.update_org_access(org_id=org_id))
        headers = self._build_headers()

        response = self.http_client.post(
            endpoint=route,
            headers=headers,
            json=data,
        )
        return response

    def get_org_usage(self, org_id: str) -> HTTPResponse:
        route = self._build_route(endpoint=self.orgs_endpoint.get_org_usage(org_id=org_id))
        headers = self._build_headers()

        response = self.http_client.get(
            endpoint=route,
            headers=headers,
        )
        return response

















    # def _convert_grist_to_df(self, records: dict[str, Any]) -> pd.DataFrame:
    #     results = [{"id": result["id"]} | result["fields"] for result in records["records"]]

    #     if len(results) == 0:
    #         raise ValueError("No data was provided. records['records'] is empty.")

    #     colonnes = [key for key, value in results[0].items()]
    #     df = pd.DataFrame(data=results, columns=colonnes)  # type: ignore
    #     return df

    # def send_dataframe_to_grist(
    #     self,
    #     df: pd.DataFrame,
    #     tbl_name: str,
    #     rename_columns: dict[str, str] | None = None,
    #     batch_size: int = 400,
    #     skip_empty: bool = True,
    #     base_url: str | None = None,
    #     doc_id: str | None = None,
    #     api_token: str | None = None,
    # ) -> None:
    #     """Send a pandas DataFrame to a Grist table.

    #     Args:
    #         df (pd.DataFrame): DataFrame to send to Grist
    #         tbl_name (str): Name of the Grist table
    #         rename_columns (dict[str, str], optional): Mapping to rename DataFrame columns before sending. Defaults to None.
    #         base_url (str, optional): Grist base URL. Defaults to None (uses instance default).
    #         doc_id (str, optional): Grist document ID. Defaults to None (uses instance default).
    #         api_token (str, optional): API token for authentication. Defaults to None (uses instance default).
    #         batch_size (int, optional): Number of records per batch. Defaults to 400.
    #         skip_empty (bool, optional): Skip sending if DataFrame is empty. Defaults to True.

    #     Returns:
    #         None

    #     Notes:
    #         The DataFrame is converted to a list of records
    #     """
    #     # Rename columns if mapping is provided
    #     df_to_send = df.rename(columns=rename_columns) if rename_columns else df.copy()

    #     # Convert DataFrame to list of records
    #     new_rows = df_to_send.to_dict(orient="records")
    #     logging.info(msg=f"Nombre de nouvelles lignes à envoyer: {len(new_rows)}")

    #     if len(new_rows) == 0:
    #         if skip_empty:
    #             logging.info(msg=f"Aucune nouvelle ligne à ajouter dans la table {tbl_name} ... Skipping")
    #             return
    #         else:
    #             raise ValueError("DataFrame is empty. No records to send.")

    #     # Prepare data in Grist format
    #     data = {"records": [{"fields": record} for record in new_rows]}
    #     logging.info(msg=f"Ajout des nouvelles lignes dans la table {tbl_name}")
    #     logging.debug(msg=f"Exemple: {data['records'][0]}")

    #     # Send to Grist using post_records with batching
    #     self.post_records(
    #         base_url=base_url,
    #         doc_id=doc_id,
    #         tbl_name=tbl_name,
    #         json=data,
    #         api_token=api_token,
    #         batch_size=batch_size,
    #     )

    # def get_records(
    #     self,
    #     base_url: str | None = None,
    #     doc_id: str | None = None,
    #     tbl_name: str | None = None,
    #     query_params: list[str] | None = None,
    #     api_token: str | None = None,
    # ) -> HTTPResponse:
    #     """_summary_

    #     Args:
    #         base_url (str, optional): _description_. Defaults to None.
    #         doc_id (str, optional): _description_. Defaults to None.
    #         tbl_name (str, optional): _description_. Defaults to None.
    #         api_token (str, optional): _description_. Defaults to None.

    #     Returns:
    #         list[dict[str, any]]: _description_
    #     """
    #     url = self._build_url_records(base_url=base_url, doc_id=doc_id, tbl_name=tbl_name)
    #     if query_params is not None:
    #         url = url + "?" + "&".join(query_params)
    #     headers = self._build_headers(api_token=api_token)
    #     grist_response = self.http_client.get(endpoint=url, headers=headers)
    #     return grist_response

    # def post_records(
    #     self,
    #     base_url: str | None = None,
    #     doc_id: str | None = None,
    #     tbl_name: str | None = None,
    #     query_params: list[str] | None = None,
    #     data: dict[str, Any] | None = None,
    #     json: dict[str, Any] | None = None,
    #     api_token: str | None = None,
    #     batch_size: int = 400,
    # ) -> None:
    #     """_summary_

    #     Args:
    #         base_url (str, optional): _description_. Defaults to None.
    #         doc_id (str, optional): _description_. Defaults to None.
    #         tbl_name (str, optional): _description_. Defaults to None.
    #         query_params (list[str], optional): _description_. Defaults to None.
    #         json (dict[str, any], optional): _description_. Defaults to None.
    #         api_token (str, optional): _description_. Defaults to None.

    #     Returns:
    #         _type_: _description_
    #     """
    #     url = self._build_url_records(base_url=base_url, doc_id=doc_id, tbl_name=tbl_name)
    #     if query_params is not None:
    #         url = url + "?" + "&".join(query_params)

    #     headers = self._build_headers(api_token=api_token)

    #     # Determine which payload is being used
    #     payload = json if json is not None else data
    #     if payload is None or "records" not in payload:
    #         raise ValueError("Either 'data' or 'json' must contain a 'records' list.")

    #     records = payload["records"]

    #     total = len(records)
    #     total_batches = (total + batch_size - 1) // batch_size

    #     logging.info(msg=f"Starting upload of {total} records in {total_batches} batches...")

    #     # Process in batches
    #     for batch_index in range(total_batches):
    #         start = batch_index * batch_size
    #         end = start + batch_size
    #         batch = records[start:end]

    #         logging.info(
    #             msg=f"Sending batch {batch_index + 1}/{total_batches} "
    #             f"({len(batch)} records, indexes {start}-{end-1})"
    #         )

    #         batch_payload = {"records": batch}

    #         response = self.http_client.post(
    #             endpoint=url,
    #             headers=headers,
    #             json=batch_payload,
    #             data=batch_payload if data is not None else None,
    #         )
    #         logging.info(msg=response.status_code)
    #         logging.info(msg=f"Batch {batch_index + 1}/{total_batches} completed.")

    #     logging.info(msg="All batches sent successfully.")

    # def put_records(
    #     self,
    #     base_url: str | None = None,
    #     doc_id: str | None = None,
    #     tbl_name: str | None = None,
    #     query_params: list[str] | None = None,
    #     data: dict[str, Any] | None = None,
    #     json: dict[str, Any] | None = None,
    #     api_token: str | None = None,
    # ) -> HTTPResponse:
    #     """_summary_

    #     Args:
    #         base_url (str, optional): _description_. Defaults to None.
    #         doc_id (str, optional): _description_. Defaults to None.
    #         tbl_name (str, optional): _description_. Defaults to None.
    #         api_token (str, optional): _description_. Defaults to None.
    #     """
    #     json = json or {}
    #     data = data or {}

    #     url = self._build_url_records(base_url=base_url, doc_id=doc_id, tbl_name=tbl_name)
    #     if query_params is not None:
    #         url = url + "?" + "&".join(query_params)

    #     headers = self._build_headers(api_token=api_token)
    #     grist_response = self.http_client.put(endpoint=url, headers=headers, data=data, json=json)

    #     return grist_response

    # def patch_records(
    #     self,
    #     base_url: str | None = None,
    #     doc_id: str | None = None,
    #     tbl_name: str | None = None,
    #     api_token: str | None = None,
    # ):
    #     """_summary_

    #     Args:
    #         base_url (str, optional): _description_. Defaults to None.
    #         doc_id (str, optional): _description_. Defaults to None.
    #         tbl_name (str, optional): _description_. Defaults to None.
    #         api_token (str, optional): _description_. Defaults to None.
    #     """
    #     url = self._build_url_records(base_url=base_url, doc_id=doc_id, tbl_name=tbl_name)
    #     logging.info(msg=url)

    # def get_df_from_records(
    #     self,
    #     base_url: str | None = None,
    #     doc_id: str | None = None,
    #     tbl_name: str | None = None,
    #     query_params: list[str] | None = None,
    #     api_token: str | None = None,
    # ) -> pd.DataFrame:
    #     """_summary_

    #     Args:
    #         query_params (list[str]): _description_
    #         base_url (str, optional): _description_. Defaults to None.
    #         doc_id (str, optional): _description_. Defaults to None.
    #         tbl_name (str, optional): _description_. Defaults to None.
    #         api_token (str, optional): _description_. Defaults to None.

    #     Returns:
    #         pd.DataFrame: _description_
    #     """
    #     grist_response = self.get_records(
    #         base_url=base_url,
    #         doc_id=doc_id,
    #         tbl_name=tbl_name,
    #         api_token=api_token,
    #         query_params=query_params,
    #     )

    #     raw_data = grist_response
    #     if isinstance(raw_data, dict):
    #         df = self._convert_grist_to_df(records=raw_data)
    #         return df
    #     else:
    #         raise ValueError("The response from Grist is not a dictionary!")

    # def get_doc_sqlite_file(
    #     self,
    #     base_url: str | None = None,
    #     doc_id: str | None = None,
    #     api_token: str | None = None,
    # ) -> bytes:
    #     url = self._build_url_docs(base_url=base_url, doc_id=doc_id)
    #     headers = self._build_headers(api_token=api_token)
    #     grist_response = self.http_client.get(endpoint=url, headers=headers, params={"nohistory": True})
    #     if grist_response is None:
    #         raise ValueError("The response from Grist is None!")

    #     if not isinstance(grist_response, HTTPResponse):
    #         raise ValueError("The response from Grist is not a valid HTTPResponse!")

    #     return grist_response.content
