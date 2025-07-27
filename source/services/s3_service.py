from config.s3_config import s3_connection

class S3Services:
    def __init__(self, params : dict):
        self.raw = params
        self.s3_connection = s3_connection

    def ingest_s3(self):
        pass


