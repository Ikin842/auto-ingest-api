from sqlalchemy import create_engine
from sqlalchemy import URL
from config.base import settings

class PostgresConfig:
    def __init__(self, **context):
        self.__driver = context['POSTGRE_NAME']
        self.__dbase = context['POSTGRE_DATABASE']
        self.__host = context['POSTGRE_HOST']
        self.__port = context['POSTGRE_PORT']
        self.__user = context['POSTGRE_USERNAME']
        self.__pass = context['POSTGRE_PASSWORD']

    def prod_conn_sql_alchemy(self):
        url_object = URL.create(
            drivername=self.__driver,
            username=self.__user,
            password=self.__pass,
            host=self.__host,
            database=self.__dbase,
            port=self.__port
        )
        db = create_engine(url_object)
        return db.connect()
