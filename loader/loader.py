import json
import time
from pathlib import Path

import pandas as pd
import redis

import os

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


DATA_DIR = Path("/data/bronze/open_sky")
REFRESH_SECONDS = 10

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

def latest_parquet():
    return max(DATA_DIR.rglob("*.parquet"), key=lambda p: p.stat().st_mtime)

while True:
    try:
        parquet_file = latest_parquet()
        df = pd.read_parquet(parquet_file)

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        df = df.where(pd.notnull(df), None)
        df = df.replace([float("inf"), float("-inf")], None)
        payload = df.to_dict(orient="records")

        pipe = r.pipeline()
        pipe.set("opensky:states", json.dumps(payload))
        pipe.set("opensky:updated_at", time.time())
        pipe.set("opensky:source_file", parquet_file.name)
        pipe.execute()

        print(f"[loader] Redis updated ({len(payload)} rows) from {parquet_file.name}")

    except Exception as e:
        print("[loader] error:", e)

    time.sleep(REFRESH_SECONDS)
