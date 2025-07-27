import json
import uuid
import hashlib
from loguru import logger
import pandas as pd
from io import BytesIO

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

def extract_year_from_range(date_range: str) -> str:
    return date_range.strip().split("-")[0].strip()

def generate_index(data_type: str, date_range: str) -> list:
    year = extract_year_from_range(date_range)

    if data_type == 'news':
        base_indexes = ['ima-online-news', 'ima-tv-news', 'ima-printed-news']
        return [f"{base_index}-{year}*" for base_index in base_indexes]

    else:
        if 'rta' in data_type:
            return [f"isa-data-{year}*", f"socmed-content.{year}*"]

        return [f"isa-data-{year}*"]

def generate_index_ingest(date_range, client, platform):
    year = extract_year_from_range(date_range)
    client = client.replace(' ', '-').lower()

    if platform == ['online', 'printed', 'tv']:
        return f"ai-news-{client}-{year}-v2"

    else:
        return f"ai-social-media-{client}-{year}*"
