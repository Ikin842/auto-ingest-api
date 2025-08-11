import time
import jmespath
from loguru import logger
from elasticsearch import helpers
from elasticsearch.helpers import streaming_bulk
from config.elastic_config import conn_elastic
from helper.generate import (
    read_file,
    generate_id,
    success_response,
    error_response
)

class ElasticService:
    def __init__(self, raw: dict, contents):
        self.__es_conn = conn_elastic
        self.__results = []
        self.__index = "index-elasticsearch"
        self.__params = raw
        self.__contents = contents

    def elastic_auto_ingest(self):
        start_time = time.time()
        df = read_file(self.__contents, self.__params['filename'])
        datas = df.to_dict(orient="records")

        for data in datas:
            doc_id = jmespath.search('id', data)
            if doc_id is None:
                data['id'] = generate_id(data)
            self.__results.append((data['id'], data))

        if self.__results:
            row_count = self.ingest_data(self.__results, "insert")
            if row_count:
                self.__results.clear()
                return success_response(start_time, self.__results)

        return error_response("error_ingest")

    def read_query(self, query, index):
        scan_results = helpers.scan(
            self.__es_conn,
            query=query,
            index=index,
            size=1000,
            scroll="60m",
            request_timeout=3600
        )

        for hit in scan_results:
            source = hit['_source']
            self.__results.append(source)

        return self.__results

    @staticmethod
    def _insert_actions(id_data_pairs, index_name):
        for doc_id, data in id_data_pairs:
            yield {
                "_index": index_name,
                "_id": doc_id,
                "_source": data
            }

    @staticmethod
    def _update_actions(id_data_pairs, index_name):
        for doc_id, data in id_data_pairs:
            yield {
                "_op_type": "update",
                "_index": index_name,
                "_id": doc_id,
                "doc": data,
            }

    def _action_generator(self, id_data_pairs, operation_type):
        if operation_type == 'insert':
            actions_generator = self._insert_actions(id_data_pairs, self.__index)
            return actions_generator
        elif operation_type == 'update':
            actions_generator = self._update_actions(id_data_pairs, self.__index)
            return actions_generator
        else:
            logger.error("Invalid operation type! Choose either 'insert' or 'update'.")
            return None

    def ingest_data(self, id_data_pairs: list, operation_type: str = 'insert'):
        try:
            total = 0
            yield_es = self._action_generator(id_data_pairs, operation_type)

            for success, info in streaming_bulk(self.__es_conn, yield_es, chunk_size=100):
                if success:
                    total += 1
                else:
                    logger.error(f'Failed to update document: {info}')
            return total

        except Exception as e:
            logger.error(f"Error updating Elasticsearch: {e}")
            return 0
