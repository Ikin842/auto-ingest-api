import json
import uuid
import time
import hashlib
from loguru import logger
import pandas as pd
from io import BytesIO
import traceback
from starlette.responses import JSONResponse

def generate_uuid(data: dict):
    json_data = json.dumps(data, sort_keys=True)
    namespace = uuid.NAMESPACE_DNS
    return str(uuid.uuid5(namespace, json_data))

def error_response(e):
    traceback.print_exc()
    return JSONResponse(
        status_code=500,
        content={
            "message": "Internal Server Error",
            "status_code": 500,
            "detail": str(e)
        }
    )

def success_response(start_time, message):
    return JSONResponse(
        content={
            "execute_time": round(time.time() - start_time, 4),
            "message": "success",
            "status_code": 200,
            "data": message
        }
    )

def generate_id(data: dict) -> str:
    json_data = json.dumps(data, sort_keys=True)
    md5_hash = hashlib.md5(json_data.encode('utf-8')).hexdigest()
    return md5_hash

def read_file(contents, filename):
    if filename.endswith(".csv"):
        df = pd.read_csv(BytesIO(contents))
    elif filename.endswith((".xls", ".xlsx")):
        df = pd.read_excel(BytesIO(contents))
    else:
        logger.info("input must csv and xlsx")
        return None

    df = df.replace([float("inf"), float("-inf")], pd.NA)
    df = df.fillna("null")
    return df


def log_align(level, label, message):
    getattr(logger, level)(f"{label:<22}: {message}")
