import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from modules.infra.grist.endpoints import (
    DocsEndpointBuilder,
    OrgsEndpointBuilder,
    RecordsEndpointBuilder,
    SQLEndpointBuilder,
    TablesEndpointBuilder,
    WebhooksEndpointBuilder,
    WorkspacesEndpointBuilder,
)
from modules.infra.http_client.base import HttpInterface
from modules.infra.http_client.types import HTTPResponse

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GristClient:
    http_client: HttpInterface
    grist_host: str
    api_token: str

    # Endpoints
    orgs_endpoint: OrgsEndpointBuilder = field(default_factory=OrgsEndpointBuilder)
    workspaces_endpoint: WorkspacesEndpointBuilder = field(default_factory=WorkspacesEndpointBuilder)
    doc_endpoint: DocsEndpointBuilder = field(default_factory=DocsEndpointBuilder)
    tables_endpoint: TablesEndpointBuilder = field(default_factory=TablesEndpointBuilder)
    records_endpoint: RecordsEndpointBuilder = field(default_factory=RecordsEndpointBuilder)
    sql_endpoint: SQLEndpointBuilder = field(default_factory=SQLEndpointBuilder)
    webhooks_endpoint: WebhooksEndpointBuilder = field(default_factory=WebhooksEndpointBuilder)

    @property
    def host(self) -> str:
        return self.grist_host.rstrip("/")

    def _build_url(self, endpoint: str) -> str:
        return f"{self.host}/{endpoint.lstrip('/')}"

    def _build_headers(self, api_token: str | None = None) -> dict[str, str]:
        token = api_token if api_token is not None else self.api_token
        if token is None:
            raise ValueError("API Token value must be defined at top level or at method level ! ")
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "accept": "*/*",
        }

    # ==========================
    # Orgs methods
    # ==========================
    def list_orgs(self) -> HTTPResponse:
        url = self._build_url(self.orgs_endpoint.list_orgs())
        return self.http_client.get(url=url, headers=self._build_headers())

    def get_org(self, org_id: str) -> HTTPResponse:
        url = self._build_url(self.orgs_endpoint.get_org(org_id=org_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def update_org(self, org_id: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.orgs_endpoint.update_org(org_id=org_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def delete_org(self, org_id: str, name: str) -> HTTPResponse:
        url = self._build_url(self.orgs_endpoint.delete_org(org_id=org_id, name=name))
        return self.http_client.delete(url=url, headers=self._build_headers())

    def list_org_access(self, org_id: str) -> HTTPResponse:
        url = self._build_url(self.orgs_endpoint.list_org_access(org_id=org_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def update_org_access(self, org_id: str, data: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.orgs_endpoint.update_org_access(org_id=org_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=data)

    def get_org_usage(self, org_id: str) -> HTTPResponse:
        url = self._build_url(self.orgs_endpoint.get_org_usage(org_id=org_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    # ==========================
    # Workspaces methods
    # ==========================
    def list_workspaces(self, org_id: str) -> HTTPResponse:
        url = self._build_url(self.workspaces_endpoint.get_workspaces_list(org_id=org_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def create_workspace(self, org_id: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.workspaces_endpoint.create_workspace(org_id=org_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def get_workspace(self, workspace_id: str) -> HTTPResponse:
        url = self._build_url(self.workspaces_endpoint.get_workspace(workspace_id=workspace_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def update_workspace(self, workspace_id: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.workspaces_endpoint.update_workspace(workspace_id=workspace_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def delete_workspace(self, workspace_id: str) -> HTTPResponse:
        url = self._build_url(self.workspaces_endpoint.delete_workspace(workspace_id=workspace_id))
        return self.http_client.delete(url=url, headers=self._build_headers())

    def remove_workspace(self, workspace_id: str) -> HTTPResponse:
        url = self._build_url(self.workspaces_endpoint.remove_workspace(workspace_id=workspace_id))
        return self.http_client.post(url=url, headers=self._build_headers())

    def restore_workspace(self, workspace_id: str) -> HTTPResponse:
        url = self._build_url(self.workspaces_endpoint.restore_workspace(workspace_id=workspace_id))
        return self.http_client.post(url=url, headers=self._build_headers())

    def list_workspace_access(self, workspace_id: str) -> HTTPResponse:
        url = self._build_url(self.workspaces_endpoint.list_workspace_access(workspace_id=workspace_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def update_workspace_access(self, workspace_id: str, data: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.workspaces_endpoint.update_workspace_access(workspace_id=workspace_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=data)

    # ==========================
    # Docs methods
    # ==========================
    def create_doc(self, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.create_doc())
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def get_doc(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.get_doc(doc_id=doc_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def update_doc_metadata(self, doc_id: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.update_doc_metadata(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def delete_doc(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.delete_doc(doc_id=doc_id))
        return self.http_client.delete(url=url, headers=self._build_headers())

    def remove_doc(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.remove_doc(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers())

    def restore_doc(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.restore_doc(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers())

    def move_doc(self, doc_id: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.move_doc(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def pin_doc(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.pin_doc(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers())

    def unpin_doc(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.unpin_doc(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers())

    def disable_doc(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.disable_doc(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers())

    def enable_doc(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.enable_doc(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers())

    def list_doc_access(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.list_doc_access(doc_id=doc_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def update_doc_access(self, doc_id: str, data: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.update_doc_access(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=data)

    def list_doc_users_for_view_as(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.list_doc_users_for_view_as(doc_id=doc_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def download_doc(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.download_doc(doc_id=doc_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def download_doc_as(self, doc_id: str, fmt: str) -> HTTPResponse:
        _ALLOWED_FORMATS = ["xlsx", "csv", "tsv", "dsv"]
        if fmt not in _ALLOWED_FORMATS:
            raise ValueError(f"Invalid format '{fmt}'. Allowed formats are: {', '.join(_ALLOWED_FORMATS)}")
        url = self._build_url(self.doc_endpoint.download_doc_as(doc_id=doc_id, format=fmt))
        return self.http_client.get(url=url, headers=self._build_headers())

    def download_doc_table_schema(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.doc_endpoint.download_doc_table_schema(doc_id=doc_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    # ==========================
    # Tables methods
    # ==========================
    def list_tables(self, doc_id: str) -> HTTPResponse:
        url = self._build_url(self.tables_endpoint.list_tables(doc_id=doc_id))
        return self.http_client.get(url=url, headers=self._build_headers())

    def create_table(self, doc_id: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.tables_endpoint.create_table(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def update_table(self, doc_id: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.tables_endpoint.update_table(doc_id=doc_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    # ==========================
    # Records methods
    # ==========================
    def list_records(
        self,
        doc_id: str,
        table_id: str,
        query_params: list[str] | None = None,
    ) -> HTTPResponse:
        url = self._build_url(self.records_endpoint.list_records(doc_id=doc_id, table_id=table_id))
        if query_params:
            url = url + "?" + "&".join(query_params)
        return self.http_client.get(url=url, headers=self._build_headers())

    def add_records(
        self,
        doc_id: str,
        table_id: str,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        batch_size: int = 400,
    ) -> None:
        url = self._build_url(self.records_endpoint.add_records(doc_id=doc_id, table_id=table_id))
        headers = self._build_headers()
        payload = json if json is not None else data
        if payload is None or "records" not in payload:
            raise ValueError("Either 'data' or 'json' must contain a 'records' list.")
        records = payload["records"]
        total = len(records)
        total_batches = (total + batch_size - 1) // batch_size
        logger.info(f"Starting upload of {total} records in {total_batches} batches...")
        for batch_index in range(total_batches):
            start = batch_index * batch_size
            end = start + batch_size
            batch = records[start:end]
            logger.info(
                f"Sending batch {batch_index + 1}/{total_batches} " f"({len(batch)} records, indexes {start}-{end - 1})"
            )
            batch_payload = {"records": batch}
            self.http_client.post(url=url, headers=headers, json=batch_payload)
            logger.info(f"Batch {batch_index + 1}/{total_batches} completed.")
        logger.info("All batches sent successfully.")

    def update_records(
        self,
        doc_id: str,
        table_id: str,
        query_params: list[str] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> HTTPResponse:
        json = json or {}
        data = data or {}
        url = self._build_url(self.records_endpoint.update_records(doc_id=doc_id, table_id=table_id))
        if query_params:
            url = url + "?" + "&".join(query_params)
        return self.http_client.put(url=url, headers=self._build_headers(), data=data, json=json)

    def add_update_records(
        self,
        doc_id: str,
        table_id: str,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> HTTPResponse:
        url = self._build_url(self.records_endpoint.add_update_records(doc_id=doc_id, table_id=table_id))
        logger.info(url)
        payload = json if json is not None else data or {}
        return self.http_client.patch(url=url, headers=self._build_headers(), json=payload)

    def delete_records(
        self,
        doc_id: str,
        table_id: str,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> HTTPResponse:
        url = self._build_url(self.records_endpoint.delete_records(doc_id=doc_id, table_id=table_id))
        payload = json if json is not None else data
        return self.http_client.post(url=url, headers=self._build_headers(), json=payload)

    def get_df_from_records(
        self,
        doc_id: str,
        table_id: str,
        query_params: list[str] | None = None,
    ) -> pd.DataFrame:
        response = self.list_records(doc_id=doc_id, table_id=table_id, query_params=query_params)
        raw_data = response.json() if isinstance(response, HTTPResponse) else response
        if not isinstance(raw_data, dict):
            raise ValueError("The response from Grist is not a dictionary!")
        return self._convert_grist_to_df(raw_data)

    def _convert_grist_to_df(self, records: dict[str, Any]) -> pd.DataFrame:
        results = [{"id": result["id"]} | result["fields"] for result in records["records"]]
        if len(results) == 0:
            raise ValueError("No data was provided. records['records'] is empty.")
        return pd.DataFrame(data=results)  # type: ignore

    def send_dataframe_to_grist(
        self,
        df: pd.DataFrame,
        doc_id: str,
        table_id: str,
        rename_columns: dict[str, str] | None = None,
        batch_size: int = 400,
        skip_empty: bool = True,
    ) -> None:
        df_to_send = df.rename(columns=rename_columns) if rename_columns else df.copy()
        new_rows = df_to_send.to_dict(orient="records")
        logger.info(f"Nombre de nouvelles lignes à envoyer: {len(new_rows)}")
        if len(new_rows) == 0:
            if skip_empty:
                logger.info(f"Aucune nouvelle ligne à ajouter dans la table {table_id} ... Skipping")
                return
            raise ValueError("DataFrame is empty. No records to send.")
        data = {"records": [{"fields": record} for record in new_rows]}
        logger.info(f"Ajout des nouvelles lignes dans la table {table_id}")
        logger.debug(f"Exemple: {data['records'][0]}")
        self.add_records(
            doc_id=doc_id,
            table_id=table_id,
            json=data,
            batch_size=batch_size,
        )

    # ==========================
    # Webhooks methods
    # ==========================
    def list_webhooks(self, doc_id_for_call: str) -> HTTPResponse:
        url = self._build_url(self.webhooks_endpoint.list_webhooks(doc_id=doc_id_for_call))
        return self.http_client.get(url=url, headers=self._build_headers())

    def create_webhook(self, doc_id_for_call: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.webhooks_endpoint.create_webhook(doc_id=doc_id_for_call))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def update_webhook(self, doc_id_for_call: str, webhook_id: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.webhooks_endpoint.update_webhook(doc_id=doc_id_for_call, webhook_id=webhook_id))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def delete_webhook(self, doc_id_for_call: str, webhook_id: str) -> HTTPResponse:
        url = self._build_url(self.webhooks_endpoint.delete_webhook(doc_id=doc_id_for_call, webhook_id=webhook_id))
        return self.http_client.delete(url=url, headers=self._build_headers())

    def clear_webhook_doc_queue(self, doc_id_for_call: str, webhook_id: str) -> HTTPResponse:
        url = self._build_url(
            self.webhooks_endpoint.clear_webhook_doc_queue(doc_id=doc_id_for_call, webhook_id=webhook_id)
        )
        return self.http_client.post(url=url, headers=self._build_headers())

    def clear_webhook_queue(self, doc_id_for_call: str, webhook_id: str) -> HTTPResponse:
        url = self._build_url(self.webhooks_endpoint.clear_webhook_queue(doc_id=doc_id_for_call, webhook_id=webhook_id))
        return self.http_client.post(url=url, headers=self._build_headers())

    # ==========================
    # SQL methods
    # ==========================
    def execute_sql(self, doc_id_for_call: str, query: str) -> HTTPResponse:
        url = self._build_url(self.sql_endpoint.execute_sql(doc_id=doc_id_for_call))
        body = {"query": query}
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)

    def execute_sql_with_params(self, doc_id_for_call: str, body: dict[str, Any]) -> HTTPResponse:
        url = self._build_url(self.sql_endpoint.execute_sql_with_query_params(doc_id=doc_id_for_call))
        return self.http_client.post(url=url, headers=self._build_headers(), json=body)
