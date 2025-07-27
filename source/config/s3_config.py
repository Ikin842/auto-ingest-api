import s3fs
from config.base import settings

class S3Config:
    def __init__(self, **context):
        self.__access_key  = context['ACCESS_KEY']
        self.__secret_key  = context['SECRET_KEY']
        self.__s3_endpoint  = context['S3_ENDPOINT']

    def connection_s3(self):
        return s3fs.S3FileSystem(
            client_kwargs={'endpoint_url': self.__s3_endpoint},
            key=self.__access_key,
            secret=self.__secret_key
        )

s3_connection = S3Config(**settings.dict()).connection_s3()
