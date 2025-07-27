from pydantic_settings import BaseSettings

class Config(BaseSettings):

    ELASTICSEARCH_HOST: str
    ELASTICSEARCH_PORT:str
    ELASTICSEARCH_USERNAME: str
    ELASTICSEARCH_PASSWORD: str

    S3_ENDPOINT: str
    ACCESS_KEY: str
    SECRET_KEY: str
    BUCKET_NAME: str

    MONGODB_CLIENT:str
    MONGODB_DATABASE:str
    MONGODB_COLLECTION: str

    POSTGRE_NAME: str
    POSTGRE_HOST: str
    POSTGRE_PORT: int
    POSTGRE_USERNAME: str
    POSTGRE_PASSWORD: str
    POSTGRE_DATABASE: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

settings = Config()