import abc


class AbstractApiClient(abc.ABC):
    @abc.abstractmethod
    def get(self, endpoint: str, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def post(self, endpoint: str, data=None, json=None, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def put(self, url: str, data=None, json=None, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def patch(self, url: str, data=None, json=None, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, url: str, **kwargs):
        raise NotImplementedError
