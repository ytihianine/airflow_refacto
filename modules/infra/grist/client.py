import logging
from typing import Any

import pandas as pd

from modules.infra.http_client.base import HttpInterface
from modules.infra.http_client.types import HTTPResponse


class GristAPI:
    def __init__(
        self,
        http_client: HttpInterface,
        base_url: str | None = None,
        workspace_id: str | None = None,
        doc_endpoint: str = "api/docs",
        doc_id: str | None = None,
        tbl_endpoint: str = "tables",
        records_endpoint: str = "records",
        api_token: str | None = None,
    ):
        self.http_client = http_client
        self.base_url = base_url
        self.workspace_id = workspace_id
        self.doc_endpoint = doc_endpoint
        self.doc_id = doc_id
        self.tbl_endpoint = tbl_endpoint
        self.records_endpoint = records_endpoint
        self.api_token = api_token

    def _build_url_records(
        self,
        base_url: str | None = None,
        workspace_id: str | None = None,
        doc_id: str | None = None,
        tbl_name: str | None = None,
    ) -> str:
        base_url = base_url if base_url is not None else self.base_url
        workspace_id = workspace_id if workspace_id is not None else self.workspace_id
        doc_id = doc_id if doc_id is not None else self.doc_id

        if workspace_id is None:
            raise ValueError("Grist Workspace id must be defined at top level or at method level!")
        if tbl_name is None:
            raise ValueError("Table name must be defined!")
        if doc_id is None:
            raise ValueError("Doc id must be defined at top level or at method level!")

        url = "/".join(
            [
                str(base_url),
                "o",
                workspace_id,
                self.doc_endpoint,
                doc_id,
                self.tbl_endpoint,
                tbl_name,
                self.records_endpoint,
            ]
        )
        return url

    def _build_url_docs(
        self,
        base_url: str | None = None,
        workspace_id: str | None = None,
        doc_id: str | None = None,
    ) -> str:
        """Build url for all docs endpoints"""
        base_url = base_url if base_url is not None else self.base_url
        workspace_id = workspace_id if workspace_id is not None else self.workspace_id
        doc_id = doc_id if doc_id is not None else self.doc_id

        if base_url is None:
            raise ValueError("Grist base_url must be defined at top level or at method level!")
        if workspace_id is None:
            raise ValueError("Grist Workspace id must be defined at top level or at method level!")
        if doc_id is None:
            raise ValueError("Doc id must be defined at top level or at method level!")

        url = "/".join([base_url, "o", workspace_id, self.doc_endpoint, doc_id, "download"])
        return url

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

    def _convert_grist_to_df(self, records: dict[str, Any]) -> pd.DataFrame:
        results = [{"id": result["id"]} | result["fields"] for result in records["records"]]

        if len(results) == 0:
            raise ValueError("No data was provided. records['records'] is empty.")

        colonnes = [key for key, value in results[0].items()]
        df = pd.DataFrame(data=results, columns=colonnes)  # type: ignore
        return df

    def send_dataframe_to_grist(
        self,
        df: pd.DataFrame,
        tbl_name: str,
        rename_columns: dict[str, str] | None = None,
        batch_size: int = 400,
        skip_empty: bool = True,
        base_url: str | None = None,
        doc_id: str | None = None,
        api_token: str | None = None,
    ) -> None:
        """Send a pandas DataFrame to a Grist table.

        Args:
            df (pd.DataFrame): DataFrame to send to Grist
            tbl_name (str): Name of the Grist table
            rename_columns (dict[str, str], optional): Mapping to rename DataFrame columns before sending. Defaults to None.
            base_url (str, optional): Grist base URL. Defaults to None (uses instance default).
            doc_id (str, optional): Grist document ID. Defaults to None (uses instance default).
            api_token (str, optional): API token for authentication. Defaults to None (uses instance default).
            batch_size (int, optional): Number of records per batch. Defaults to 400.
            skip_empty (bool, optional): Skip sending if DataFrame is empty. Defaults to True.

        Returns:
            None

        Notes:
            The DataFrame is converted to a list of records
        """
        # Rename columns if mapping is provided
        df_to_send = df.rename(columns=rename_columns) if rename_columns else df.copy()

        # Convert DataFrame to list of records
        new_rows = df_to_send.to_dict(orient="records")
        logging.info(msg=f"Nombre de nouvelles lignes à envoyer: {len(new_rows)}")

        if len(new_rows) == 0:
            if skip_empty:
                logging.info(msg=f"Aucune nouvelle ligne à ajouter dans la table {tbl_name} ... Skipping")
                return
            else:
                raise ValueError("DataFrame is empty. No records to send.")

        # Prepare data in Grist format
        data = {"records": [{"fields": record} for record in new_rows]}
        logging.info(msg=f"Ajout des nouvelles lignes dans la table {tbl_name}")
        logging.debug(msg=f"Exemple: {data['records'][0]}")

        # Send to Grist using post_records with batching
        self.post_records(
            base_url=base_url,
            doc_id=doc_id,
            tbl_name=tbl_name,
            json=data,
            api_token=api_token,
            batch_size=batch_size,
        )

    def get_records(
        self,
        base_url: str | None = None,
        doc_id: str | None = None,
        tbl_name: str | None = None,
        query_params: list[str] | None = None,
        api_token: str | None = None,
    ) -> HTTPResponse:
        """_summary_

        Args:
            base_url (str, optional): _description_. Defaults to None.
            doc_id (str, optional): _description_. Defaults to None.
            tbl_name (str, optional): _description_. Defaults to None.
            api_token (str, optional): _description_. Defaults to None.

        Returns:
            list[dict[str, any]]: _description_
        """
        url = self._build_url_records(base_url=base_url, doc_id=doc_id, tbl_name=tbl_name)
        if query_params is not None:
            url = url + "?" + "&".join(query_params)
        headers = self._build_headers(api_token=api_token)
        grist_response = self.http_client.get(endpoint=url, headers=headers)
        return grist_response

    def post_records(
        self,
        base_url: str | None = None,
        doc_id: str | None = None,
        tbl_name: str | None = None,
        query_params: list[str] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        api_token: str | None = None,
        batch_size: int = 400,
    ) -> None:
        """_summary_

        Args:
            base_url (str, optional): _description_. Defaults to None.
            doc_id (str, optional): _description_. Defaults to None.
            tbl_name (str, optional): _description_. Defaults to None.
            query_params (list[str], optional): _description_. Defaults to None.
            json (dict[str, any], optional): _description_. Defaults to None.
            api_token (str, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        url = self._build_url_records(base_url=base_url, doc_id=doc_id, tbl_name=tbl_name)
        if query_params is not None:
            url = url + "?" + "&".join(query_params)

        headers = self._build_headers(api_token=api_token)

        # Determine which payload is being used
        payload = json if json is not None else data
        if payload is None or "records" not in payload:
            raise ValueError("Either 'data' or 'json' must contain a 'records' list.")

        records = payload["records"]

        total = len(records)
        total_batches = (total + batch_size - 1) // batch_size

        logging.info(msg=f"Starting upload of {total} records in {total_batches} batches...")

        # Process in batches
        for batch_index in range(total_batches):
            start = batch_index * batch_size
            end = start + batch_size
            batch = records[start:end]

            logging.info(
                msg=f"Sending batch {batch_index + 1}/{total_batches} "
                f"({len(batch)} records, indexes {start}-{end-1})"
            )

            batch_payload = {"records": batch}

            response = self.http_client.post(
                endpoint=url,
                headers=headers,
                json=batch_payload,
                data=batch_payload if data is not None else None,
            )
            logging.info(msg=response.status_code)
            logging.info(msg=f"Batch {batch_index + 1}/{total_batches} completed.")

        logging.info(msg="All batches sent successfully.")

    def put_records(
        self,
        base_url: str | None = None,
        doc_id: str | None = None,
        tbl_name: str | None = None,
        query_params: list[str] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        api_token: str | None = None,
    ) -> HTTPResponse:
        """_summary_

        Args:
            base_url (str, optional): _description_. Defaults to None.
            doc_id (str, optional): _description_. Defaults to None.
            tbl_name (str, optional): _description_. Defaults to None.
            api_token (str, optional): _description_. Defaults to None.
        """
        json = json or {}
        data = data or {}

        url = self._build_url_records(base_url=base_url, doc_id=doc_id, tbl_name=tbl_name)
        if query_params is not None:
            url = url + "?" + "&".join(query_params)

        headers = self._build_headers(api_token=api_token)
        grist_response = self.http_client.put(endpoint=url, headers=headers, data=data, json=json)

        return grist_response

    def patch_records(
        self,
        base_url: str | None = None,
        doc_id: str | None = None,
        tbl_name: str | None = None,
        api_token: str | None = None,
    ):
        """_summary_

        Args:
            base_url (str, optional): _description_. Defaults to None.
            doc_id (str, optional): _description_. Defaults to None.
            tbl_name (str, optional): _description_. Defaults to None.
            api_token (str, optional): _description_. Defaults to None.
        """
        url = self._build_url_records(base_url=base_url, doc_id=doc_id, tbl_name=tbl_name)
        logging.info(msg=url)

    def get_df_from_records(
        self,
        base_url: str | None = None,
        doc_id: str | None = None,
        tbl_name: str | None = None,
        query_params: list[str] | None = None,
        api_token: str | None = None,
    ) -> pd.DataFrame:
        """_summary_

        Args:
            query_params (list[str]): _description_
            base_url (str, optional): _description_. Defaults to None.
            doc_id (str, optional): _description_. Defaults to None.
            tbl_name (str, optional): _description_. Defaults to None.
            api_token (str, optional): _description_. Defaults to None.

        Returns:
            pd.DataFrame: _description_
        """
        grist_response = self.get_records(
            base_url=base_url,
            doc_id=doc_id,
            tbl_name=tbl_name,
            api_token=api_token,
            query_params=query_params,
        )

        raw_data = grist_response
        if isinstance(raw_data, dict):
            df = self._convert_grist_to_df(records=raw_data)
            return df
        else:
            raise ValueError("The response from Grist is not a dictionary!")

    def get_doc_sqlite_file(
        self,
        base_url: str | None = None,
        doc_id: str | None = None,
        api_token: str | None = None,
    ) -> bytes:
        url = self._build_url_docs(base_url=base_url, doc_id=doc_id)
        headers = self._build_headers(api_token=api_token)
        grist_response = self.http_client.get(endpoint=url, headers=headers, params={"nohistory": True})
        if grist_response is None:
            raise ValueError("The response from Grist is None!")

        if not isinstance(grist_response, HTTPResponse):
            raise ValueError("The response from Grist is not a valid HTTPResponse!")

        return grist_response.content
