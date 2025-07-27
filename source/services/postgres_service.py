
import time
import traceback
from loguru import logger
import pandas as pd
from io import BytesIO
from sqlalchemy.dialects.postgresql import insert
from config.postgres_config import pg_config
from starlette.responses import JSONResponse
from helper.generate import read_file

class PostgresService:
    def __init__(self, raw : dict, contents):
        self.connect = pg_config
        self.result = []
        self.params = raw
        self.contents = contents

    def read_query(self, query):
        df = pd.read_sql_query(query, self.connect)
        self.connect.connection.close()
        return df.to_dict(orient='records')

    def postgres_auto_ingest(self):
        try:
            start_time = time.time()

            filename = self.params['filename']
            table_name = self.params['table_name']

            df = read_file(self.contents, filename)
            # row_count = self.ingest(df, table_name)
            row_count = True

            if row_count:
                return JSONResponse(
                    content={
                        "execute_time": round(time.time() - start_time, 4),
                        "message": "success",
                        "status_code": 200,
                        "data": df.to_dict(orient="records")
                    }
                )

            else:
                return JSONResponse(
                    status_code=500,
                    content={
                        "message": "Internal Server Error",
                        "status_code": 500,
                        "data": [],
                    }
                )

        except Exception as e:
            return JSONResponse(
                status_code=500,
                content={
                    "message": f"Internal Server Error {e}",
                    "status_code": 500,
                    "data": [],
                }
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

    def ingest(self, df, table_name: str):
        row_count = df.to_sql(
            table_name,
            self.connect,
            if_exists="append",
            index=False,
            schema=self.params['schema_table'],
            method=self._insert_on_conflict_upsert
        )
        self.connect.connection.commit()
        self.connect.connection.close()
        return row_count
