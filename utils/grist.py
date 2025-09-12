import pandas as pd

from utils.api_client.base import AbstractApiClient


class GristAPI:
    def __init__(
        self,
        api_client: AbstractApiClient,
        base_url: str = None,
        workspace_id: str = None,
        doc_endpoint: str = "api/docs",
        doc_id: str = None,
        tbl_endpoint: str = "tables",
        records_endpoint: str = "records",
        api_token: str = None,
    ):
        self.api_client = api_client
        self.base_url = base_url
        self.workspace_id = workspace_id
        self.doc_endpoint = doc_endpoint
        self.doc_id = doc_id
        self.tbl_endpoint = tbl_endpoint
        self.records_endpoint = records_endpoint
        self.api_token = api_token

    def _build_url_records(
        self,
        base_url: str = None,
        workspace_id: str = None,
        doc_id: str = None,
        tbl_name: str = None,
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
                base_url,
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
        self, base_url: str = None, workspace_id: str = None, doc_id: str = None
    ) -> str:
        """Build url for all docs endpoints"""
        base_url = base_url if base_url is not None else self.base_url
        workspace_id = workspace_id if workspace_id is not None else self.workspace_id
        doc_id = doc_id if doc_id is not None else self.doc_id

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

    def _build_headers(self, api_token: str = None) -> dict[str, str]:
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

    def _convert_grist_to_df(self, records: dict[str, any]) -> pd.DataFrame:
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
        base_url: str = None,
        doc_id: str = None,
        tbl_name: str = None,
        query_params: list[str] = None,
        api_token: str = None,
    ) -> list[dict[str, any]]:
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
        grist_response = self.api_client.get(endpoint=url, headers=headers)
        if grist_response.status_code == 200:
            return grist_response

        raise ValueError("Error API response:")

    def post_records(
        self,
        base_url: str = None,
        doc_id: str = None,
        tbl_name: str = None,
        query_params: list[str] = None,
        data: dict[str, any] = None,
        json: dict[str, any] = None,
        api_token: str = None,
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
        url = self._build_url_records(
            base_url=base_url, doc_id=doc_id, tbl_name=tbl_name
        )
        if query_params is not None:
            url = url + "?" + "&".join(query_params)

        headers = self._build_headers(api_token=api_token)
        grist_response = self.api_client.post(
            endpoint=url, headers=headers, data=data, json=json
        )

        if grist_response.status_code == 200:
            return grist_response
        grist_response.raise_for_status()
        # raise ValueError(f"Error API response:")

    def put_records(
        self,
        base_url: str = None,
        doc_id: str = None,
        tbl_name: str = None,
        query_params: list[str] = None,
        data: dict[str, any] = None,
        json: dict[str, any] = None,
        api_token: str = None,
    ) -> None:
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
        grist_response = self.api_client.put(
            url=url, headers=headers, data=data, json=json
        )

        if grist_response.status_code == 200:
            return grist_response
        grist_response.raise_for_status()

    def patch_records(
        self,
        base_url: str = None,
        doc_id: str = None,
        tbl_name: str = None,
        api_token: str = None,
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
        base_url: str = None,
        doc_id: str = None,
        tbl_name: str = None,
        query_params: list[str] = None,
        api_token: str = None,
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

        if grist_response.status_code == 200:
            raw_data = grist_response.json()
            df = self._convert_grist_to_df(records=raw_data)
            return df

    def get_doc_sqlite_file(
        self,
        base_url: str = None,
        doc_id: str = None,
        api_token: str = None,
    ) -> None:
        url = self._build_url_docs(base_url=base_url, doc_id=doc_id)
        headers = self._build_headers(api_token=api_token)
        grist_response = self.api_client.get(
            endpoint=url, headers=headers, params={"nohistory": True}
        )
        grist_response.raise_for_status()

        if grist_response.status_code == 200:
            return grist_response
