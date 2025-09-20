from typing import Optional, Any
import pandas as pd
from infra.http_client.base import AbstractHTTPClient
from infra.http_client.types import HTTPResponse


class GristAPI:
    def __init__(
        self,
        http_client: AbstractHTTPClient,
        base_url: Optional[str] = None,
        workspace_id: Optional[str] = None,
        doc_endpoint: str = "api/docs",
        doc_id: Optional[str] = None,
        tbl_endpoint: str = "tables",
        records_endpoint: str = "records",
        api_token: Optional[str] = None,
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
        base_url: Optional[str] = None,
        workspace_id: Optional[str] = None,
        doc_id: Optional[str] = None,
        tbl_name: Optional[str] = None,
    ) -> str:
        base_url = base_url if base_url is not None else self.base_url
        workspace_id = workspace_id if workspace_id is not None else self.workspace_id
        doc_id = doc_id if doc_id is not None else self.doc_id

        if workspace_id is None:
            raise ValueError(
                "Grist Workspace id must be defined at top level or at method level!"
            )
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
        base_url: Optional[str] = None,
        workspace_id: Optional[str] = None,
        doc_id: Optional[str] = None,
    ) -> str:
        """Build url for all docs endpoints"""
        base_url = base_url if base_url is not None else self.base_url
        workspace_id = workspace_id if workspace_id is not None else self.workspace_id
        doc_id = doc_id if doc_id is not None else self.doc_id

        if base_url is None:
            raise ValueError(
                "Grist base_url must be defined at top level or at method level!"
            )
        if workspace_id is None:
            raise ValueError(
                "Grist Workspace id must be defined at top level or at method level!"
            )
        if doc_id is None:
            raise ValueError("Doc id must be defined at top level or at method level!")

        url = "/".join(
            [base_url, "o", workspace_id, self.doc_endpoint, doc_id, "download"]
        )
        return url

    def _build_headers(self, api_token: Optional[str] = None) -> dict[str, str]:
        api_token = api_token if api_token is not None else self.api_token

        if api_token is None:
            raise ValueError(
                "API Token value must be defined at top level or at method level ! "
            )

        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
            "accept": "*/*",
        }

        return headers

    def _convert_grist_to_df(self, records: dict[str, Any]) -> pd.DataFrame:
        results = [
            {"id": result["id"]} | result["fields"] for result in records["records"]
        ]

        if len(results) == 0:
            raise ValueError("No data was provided. records['records'] is empty.")

        colonnes = [key for key, value in results[0].items()]
        df = pd.DataFrame(results, columns=colonnes)
        return df

    def get_records(
        self,
        base_url: Optional[str] = None,
        doc_id: Optional[str] = None,
        tbl_name: Optional[str] = None,
        query_params: Optional[list[str]] = None,
        api_token: Optional[str] = None,
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
        url = self._build_url_records(
            base_url=base_url, doc_id=doc_id, tbl_name=tbl_name
        )
        if query_params is not None:
            url = url + "?" + "&".join(query_params)
        headers = self._build_headers(api_token=api_token)
        grist_response = self.http_client.get(endpoint=url, headers=headers)
        return grist_response

    def post_records(
        self,
        base_url: Optional[str] = None,
        doc_id: Optional[str] = None,
        tbl_name: Optional[str] = None,
        query_params: Optional[list[str]] = None,
        data: Optional[dict[str, Any]] = None,
        json: Optional[dict[str, Any]] = None,
        api_token: Optional[str] = None,
    ) -> HTTPResponse:
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
        url = self._build_url_records(
            base_url=base_url, doc_id=doc_id, tbl_name=tbl_name
        )
        if query_params is not None:
            url = url + "?" + "&".join(query_params)

        headers = self._build_headers(api_token=api_token)
        grist_response = self.http_client.post(
            endpoint=url, headers=headers, data=data, json=json
        )

        return grist_response

    def put_records(
        self,
        base_url: Optional[str] = None,
        doc_id: Optional[str] = None,
        tbl_name: Optional[str] = None,
        query_params: Optional[list[str]] = None,
        data: Optional[dict[str, Any]] = None,
        json: Optional[dict[str, Any]] = None,
        api_token: Optional[str] = None,
    ) -> HTTPResponse:
        """_summary_

        Args:
            base_url (str, optional): _description_. Defaults to None.
            doc_id (str, optional): _description_. Defaults to None.
            tbl_name (str, optional): _description_. Defaults to None.
            api_token (str, optional): _description_. Defaults to None.
        """
        url = self._build_url_records(
            base_url=base_url, doc_id=doc_id, tbl_name=tbl_name
        )
        if query_params is not None:
            url = url + "?" + "&".join(query_params)

        headers = self._build_headers(api_token=api_token)
        grist_response = self.http_client.put(
            endpoint=url, headers=headers, data=data, json=json
        )

        return grist_response

    def patch_records(
        self,
        base_url: Optional[str] = None,
        doc_id: Optional[str] = None,
        tbl_name: Optional[str] = None,
        api_token: Optional[str] = None,
    ):
        """_summary_

        Args:
            base_url (str, optional): _description_. Defaults to None.
            doc_id (str, optional): _description_. Defaults to None.
            tbl_name (str, optional): _description_. Defaults to None.
            api_token (str, optional): _description_. Defaults to None.
        """
        url = self._build_url_records(
            base_url=base_url, doc_id=doc_id, tbl_name=tbl_name
        )
        print(url)

    def get_df_from_records(
        self,
        base_url: Optional[str] = None,
        doc_id: Optional[str] = None,
        tbl_name: Optional[str] = None,
        query_params: Optional[list[str]] = None,
        api_token: Optional[str] = None,
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
        base_url: Optional[str] = None,
        doc_id: Optional[str] = None,
        api_token: Optional[str] = None,
    ) -> bytes:
        url = self._build_url_docs(base_url=base_url, doc_id=doc_id)
        headers = self._build_headers(api_token=api_token)
        grist_response = self.http_client.get(
            endpoint=url, headers=headers, params={"nohistory": True}
        )
        if grist_response is None:
            raise ValueError("The response from Grist is None!")

        if not isinstance(grist_response, bytes):
            raise ValueError("The response from Grist is not bytes!")

        return grist_response
