from elasticsearch7 import Elasticsearch

class ElasticConfig:
    def __init__(self, **context) -> None:
        self.__host = context['ELASTICSEARCH_HOST']
        self.__port = context['ELASTICSEARCH_PORT']

    def connect(self) -> Elasticsearch:
        return Elasticsearch(
            f'http://{self.__host}:{self.__port}'
        )

    def ping(self) -> bool:
        es = self.connect()
        return es.ping()

    def __call__(self) -> Elasticsearch:
        return self.connect()
