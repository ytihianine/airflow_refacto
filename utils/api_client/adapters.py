import requests
import httpx

from utils.api_client.base import AbstractApiClient


class HttpxAPIClient(AbstractApiClient):
    def __init__(self, proxy: str = None, user_agent: str = None, verify: bool = True):
        self.headers = {"User-Agent": user_agent} if user_agent else {}
        self.proxy = f"http://{proxy}" if proxy else None
        if proxy and proxy.startswith("http://"):
            raise ValueError('proxy must not start with "http://"!')

        self.client = httpx.Client(
            proxy=self.proxy, headers=self.headers, verify=verify
        )

    def get(self, endpoint: str, **kwargs):
        try:
            response = self.client.get(endpoint, **kwargs)
            return response
        except Exception as e:
            print(f"Error during GET request: {e}")
            raise

    def post(self, endpoint: str, data: dict[any, any] = None, json=None, **kwargs):
        try:
            response = self.client.post(endpoint, data=data, json=json, **kwargs)
            return response
        except Exception as e:
            print(f"Error during POST request: {e}")
            raise

    def put(self, url: str, data: dict[any, any] = None, json=None, **kwargs):
        try:
            response = self.client.put(url, data=data, json=json, **kwargs)
            return response
        except Exception as e:
            print(f"Error during PUT request: {e}")
            raise

    def patch(self, url: str, data: dict[any, any] = None, json=None, **kwargs):
        try:
            with self.client as client:
                response = self.client.post(url, data=data, json=json, **kwargs)
                return response
        except Exception as e:
            print(f"Error during PATCH request: {e}")
            raise

    def delete(self, url: str, **kwargs):
        pass


class RequestsAPIClient(AbstractApiClient):
    def __init__(self, proxy: str = None, user_agent: str = None):
        self.session = requests.Session()

        if user_agent:
            self.session.headers.update({"User-Agent": user_agent})

        if proxy:
            # Configure proxies
            self.proxies = {"http": proxy, "https": proxy}
            self.session.proxies = self.proxies

            # Ensure proxy headers include the User-Agent
            if user_agent:
                self.session.get_adapter("https://").proxy_manager_for(
                    f"http://{proxy}"
                ).proxy_headers["User-Agent"] = user_agent

    def get(self, endpoint: str, **kwargs):
        try:
            response = self.session.get(endpoint, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Error during GET request: {e}")
            raise

    def post(self, endpoint: str, data: dict[any, any] = None, json=None, **kwargs):
        try:
            response = self.session.post(endpoint, data=data, json=json, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Error during POST request: {e}")
            raise

    def put(self):
        pass

    def patch(self):
        pass

    def delete(self, url: str, **kwargs):
        pass
