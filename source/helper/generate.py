import json
import uuid
import time
import hashlib
from loguru import logger
import pandas as pd
from io import BytesIO
import traceback

def generate_uuid(data: dict):
    json_data = json.dumps(data, sort_keys=True)
    namespace = uuid.NAMESPACE_DNS
    return str(uuid.uuid5(namespace, json_data))

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

def clean_dirty_values(data: dict) -> dict:
    cleaned = {}

    for key, value in data.items():
        if isinstance(value, str):
            stripped = value.strip().lower()
            if stripped in ["", "null", "none"]:
                cleaned[key] = None
            else:
                cleaned[key] = value.strip()
        elif value != value:  # NaN check (NaN != NaN)
            cleaned[key] = None
        else:
            cleaned[key] = value

    return cleaned
