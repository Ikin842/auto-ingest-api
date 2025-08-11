import time
import traceback
import pandas as pd

from io import BytesIO
from loguru import logger

from helper.generate import error_response
from starlette.responses import JSONResponse
from services.postgres_service import PostgresService
from services.elastic_service import ElasticService
from fastapi import APIRouter, File, UploadFile, Form

app = APIRouter()

@app.post("/ingest-postgres/")
async def upload_file(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    schema_table: str = Form(...)
):
    try:
        filename = file.filename.lower()
        contents = await file.read()

        params = {
            "filename": filename,
            "table_name": table_name,
            "schema_table": schema_table
        }

        services = PostgresService(params, contents)
        response = services.postgres_auto_ingest()
        return response

    except Exception as e:
        return error_response(e)

@app.post("/ingest-elasticsearch/")
async def upload_file(
    file: UploadFile = File(...),
    index_name: str = Form(...),
):
    try:
        filename = file.filename.lower()
        contents = await file.read()

        params = {
            "filename": filename,
            "index_name": index_name,
        }

        services = ElasticService(params, contents)
        response = services.elastic_auto_ingest()
        return response

    except Exception as e:
        return error_response(e)

@app.post("/ingest-s3/")
async def upload_file(
    file: UploadFile = File(...),
):
    try:
        start_time = time.time()
        filename = file.filename.lower()
        contents = await file.read()

        params = {
            "filename": filename
        }

        if filename.endswith(".csv"):
            df = pd.read_csv(BytesIO(contents))
        elif filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(BytesIO(contents))
        else:
            logger.info("input must csv and xlsx")
            return None

        df = df.replace([float("inf"), float("-inf")], pd.NA)
        df = df.fillna("null")

        return JSONResponse(
            content={
                "execute_time": round(time.time() - start_time, 4),
                "message": "success",
                "status_code": 200,
                "data": df.to_dict(orient="records")
            }
        )

    except Exception as e:
        return error_response(e)

@app.post("/ingest-mongodb/")
async def upload_file(
    file: UploadFile = File(...),
):
    try:
        start_time = time.time()
        filename = file.filename.lower()
        contents = await file.read()

        params = {
            "filename": filename
        }

        if filename.endswith(".csv"):
            df = pd.read_csv(BytesIO(contents))
        elif filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(BytesIO(contents))
        else:
            logger.info("input must csv and xlsx")
            return None

        df = df.replace([float("inf"), float("-inf")], pd.NA)
        df = df.fillna("null")

        return JSONResponse(
            content={
                "execute_time": round(time.time() - start_time, 4),
                "message": "success",
                "status_code": 200,
                "data": df.to_dict(orient="records")
            }
        )

    except Exception as e:
        return error_response(e)
