# from elasticsearch7 import Elasticsearch
# from elasticsearch7.exceptions import RequestError
#
# es = Elasticsearch("http://localhost:9200")
#
# if es.ping():
#     print("✅ Connected to Elasticsearch")
# else:
#     print("❌ Failed to connect to Elasticsearch")
#     exit()
#
# index_name = "sample-data"
#
# if es.indices.exists(index=index_name):
#     es.indices.delete(index=index_name)
#     print(f"ℹ️ Existing index '{index_name}' deleted.")
#
# try:
#     es.indices.create(index=index_name)
#     print(f"✅ Index '{index_name}' created.")
# except RequestError as e:
#     print(f"⚠️ Index creation failed: {e}")
#
# sample_docs = [
#     {"id": 1, "name": "Alice", "age": 30, "city": "Jakarta"},
#     {"id": 2, "name": "Bob", "age": 25, "city": "Bandung"},
#     {"id": 3, "name": "Charlie", "age": 35, "city": "Surabaya"},
# ]
# # 6. Indexing data with auto refresh
# for doc in sample_docs:
#     response = es.index(index=index_name, document=doc, refresh="wait_for")
#     print(f"📦 Document indexed: {response['_id']}")
#
#
# count = es.count(index=index_name)["count"]
# print(f"🔍 Total documents in '{index_name}': {count}")


from services.mongo_service import MongoService
mongo = MongoService()
if mongo.check_connection():
    print("Success")
