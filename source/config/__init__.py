from config.elastif_config import ElasticConfig
from config.base import settings

elastic_config = ElasticConfig(**settings.dict())

if elastic_config.ping():
    print("✅ Connected to Elasticsearch")
else:
    print("❌ Failed to connect to Elasticsearch")

conn_elastic = elastic_config.connect()
