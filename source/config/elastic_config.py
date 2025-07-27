from elasticsearch7 import Elasticsearch
from config.base import settings

class ElasticConfig:
    def __init__(self, **context) -> None:
        self.__host = context['ELASTICSEARCH_HOST']
        self.__port = context['ELASTICSEARCH_PORT']
        self.__user = context['ELASTICSEARCH_USERNAME']
        self.__pass = context['ELASTICSEARCH_PASSWORD']

    def connect(self) -> Elasticsearch:
        return Elasticsearch(
            f'http://{self.__host}:{self.__port}',
                    http_auth=(self.__user, self.__pass)
        )

    def __call__(self) -> Elasticsearch:
        return self.connect()

conn_elastic = ElasticConfig(**settings.dict()).connect()