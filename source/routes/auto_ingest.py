from models.response import error_response
from services.postgres_service import PostgresService
from fastapi import APIRouter, File, UploadFile, Form
from services.elastic_service import ElasticService
from services.mongo_service import MongoService

app = APIRouter()

@app.post("/postgres/")
async def upload_file(
    file: UploadFile = File(...),
    title_name: str = Form(...),
):
    try:
        filename = file.filename.lower()
        contents = await file.read()
        params = {
            "filename": filename,
            "table_name": title_name
        }
        services = PostgresService(params, contents)
        response = services.postgres_auto_ingest()
        return response

    except Exception as e:
        return error_response(e)

@app.post("/elasticsearch/")
async def upload_file(
    file: UploadFile = File(...),
    title_name : str = Form(...),
):
    try:
        filename = file.filename.lower()
        contents = await file.read()
        params = {
            "filename": filename,
            "index_name": title_name,
        }
        services = ElasticService(params, contents)
        response = services.elastic_auto_ingest()
        return response

    except Exception as e:
        return error_response(e)

@app.post("/mongodb/")
async def upload_file(
    file: UploadFile = File(...),
    title_name : str = Form(...),
):
    try:
        filename = file.filename.lower()
        contents = await file.read()
        params = {
            "filename": filename,
            "collection_name": title_name,
        }
        services = MongoService(params, contents)
        response = services.mongo_auto_ingest()
        return response

    except Exception as e:
        return error_response(e)
