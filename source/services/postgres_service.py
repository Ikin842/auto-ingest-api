
import time
from loguru import logger
from config.base import settings
from helper.generate import read_file
from sqlalchemy import URL, create_engine
from sqlalchemy.dialects.postgresql import insert
from models.response import (
    error_response,
    success_response
)

class PostgresService:
    def __init__(self, raw : dict, contents):
        self.__result = []
        self.__params = raw
        self.__contents = contents
        self.__url = URL.create(
            drivername=settings.POSTGRE_NAME,
            username=settings.POSTGRE_USERNAME,
            password=settings.POSTGRE_PASSWORD,
            host=settings.POSTGRE_HOST,
            port=settings.POSTGRE_PORT,
            database=settings.POSTGRE_DATABASE,
        )

    @staticmethod
    def _insert_on_conflict_upsert(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        insert_statement = insert(table.table).values(data)
        conflict_update = insert_statement.on_conflict_do_update(
            constraint=f"{table.table.name}_pkey",
            set_={column.key: column for column in insert_statement.excluded},
        )
        result = conn.execute(conflict_update)
        return result.rowcount

    def postgres_auto_ingest(self):
        try:
            start_time = time.time()
            filename = self.__params['filename']
            table_name = self.__params['table_name']
            df = read_file(self.__contents, filename)
            row_count = self.ingest(df, table_name)

            if row_count:
                return success_response(
                    start_time=start_time,
                    message={
                        "status_ingest":"success",
                        "table_name": table_name}
                )

            else:
                return error_response("error ingest postgres")

        except Exception as e:
            return error_response(e)

    def ingest(self, df, table_name: str):
        try:
            engine = create_engine(self.__url)
            conn = engine.connect()
            # df = df.drop_duplicates(subset=['id'], keep='last')
            logger.info(f"Attempting to insert {len(df)} rows into {table_name}")

            row_count = df.to_sql(
                table_name,
                con=conn,
                if_exists='append',
                index=False,
                # method=self._conflict_do_update
            )
            logger.info(f"Successfully inserted rows into {table_name}")
            conn.connection.commit()
            conn.connection.close()
            return row_count

        except Exception as e:
            logger.error(f"Error during insert to {table_name}: {e}")
            raise e