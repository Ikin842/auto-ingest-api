import json
import time
import pymongo
import jmespath
from loguru import logger
from config.base import settings
from models.response import (
    success_response,
    error_response
)
from helper.generate import (
    read_file,
    generate_uuid,
    clean_dirty_values
)

class MongoService:
    def __init__(self, raw: dict, contents):
        self.__client = pymongo.MongoClient(settings.MONGODB_CLIENT)
        self.__database = self.__client[settings.MONGODB_DATABASE]
        self.__params = raw
        self.__contents = contents

    def check_connection(self) -> bool:
        try:
            self.__client.admin.command('ping')
            logger.info("✅ Koneksi ke MongoDB berhasil.")
            return True
        except Exception as e:
            logger.error(f"❌ Gagal koneksi ke MongoDB: {e}")
            return False

    def mongo_auto_ingest(self):
        try:
            start_time = time.time()
            df = read_file(self.__contents, self.__params['filename'])
            datas = df.to_dict(orient="records")

            for data in datas:
                data = clean_dirty_values(data)
                doc_id = jmespath.search('id', data)
                if doc_id is None:
                    data['_id'] = generate_uuid(data)

                status_ingest = self.ingest_data(
                    data_raw=data,
                    collection_name=self.__params['collection_name']
                )
            logger.info("success ingest all data")
            return success_response(
                start_time,
                message={
                    "status_ingest": "success",
                    "collection_name": self.__params['collection_name']
                })

        except Exception as e:
            return error_response(e)

    def ingest_data(self, data_raw: dict, match_field: str = "_id", collection_name: str = None):
        try:
            target_collection_name = collection_name
            collection = self.__database[target_collection_name]

            filter_query = {match_field: data_raw.get(match_field)}
            update = {"$set": data_raw}

            result = collection.update_one(filter_query, update, upsert=True)
            return result.acknowledged
        except Exception as e:
            logger.error(f"Error updating MongoDB: {e}")
            return False
