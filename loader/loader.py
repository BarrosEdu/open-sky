# loader.py
import json
import os
import time
import io
from datetime import datetime

import pandas as pd
import redis
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# -------------------------------------------------
# Environment
# -------------------------------------------------
load_dotenv()

# -------------------------------------------------
# Redis config
# -------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

# -------------------------------------------------
# Azure Blob config
# -------------------------------------------------
AZURE_CONTAINER = os.getenv("AZURE_BLOB_CONTAINER", "bronze")
BLOB_PREFIX = os.getenv("OPENSKY_BLOB_PREFIX", "open_sky")

REFRESH_SECONDS = int(os.getenv("LOADER_REFRESH_SECONDS", "10"))

# -------------------------------------------------
# Azure client
# -------------------------------------------------
def get_container_client():
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not set")

    service = BlobServiceClient.from_connection_string(conn_str)
    return service.get_container_client(AZURE_CONTAINER)


# -------------------------------------------------
# Find latest parquet in Blob
# -------------------------------------------------
def latest_parquet_blob(container):
    blobs = container.list_blobs(name_starts_with=BLOB_PREFIX)

    latest_blob = None
    latest_time = None

    for blob in blobs:
        if not blob.name.endswith(".parquet"):
            continue

        if latest_time is None or blob.last_modified > latest_time:
            latest_blob = blob
            latest_time = blob.last_modified

    if not latest_blob:
        raise RuntimeError("No parquet files found in blob storage")

    return latest_blob


# -------------------------------------------------
# Main loop
# -------------------------------------------------
def run():
    container = get_container_client()
    last_processed_blob = None

    print("[loader] started")

    while True:
        try:
            blob = latest_parquet_blob(container)

            # Avoid reprocessing same file
            if last_processed_blob == blob.name:
                time.sleep(REFRESH_SECONDS)
                continue

            print(f"[loader] loading blob: {blob.name}")

            blob_client = container.get_blob_client(blob.name)
            data = blob_client.download_blob().readall()

            df = pd.read_parquet(io.BytesIO(data))

            # Normalize datetimes
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            df = df.where(pd.notnull(df), None)
            df = df.replace([float("inf"), float("-inf")], None)

            payload = df.to_dict(orient="records")

            pipe = r.pipeline()
            pipe.set("opensky:states", json.dumps(payload))
            pipe.set("opensky:updated_at", time.time())
            pipe.set("opensky:source_blob", blob.name)
            pipe.execute()

            last_processed_blob = blob.name

            print(
                f"[loader] Redis updated ({len(payload)} rows) "
                f"from {blob.name}"
            )

        except Exception as e:
            print("[loader] error:", str(e))

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    run()
