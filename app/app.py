import os, sys
import json
import math

from datetime import datetime, date
import pandas as pd
import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import duckdb
from dotenv import load_dotenv

load_dotenv()


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

app = FastAPI()


def sanitize(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, float):
        return None if math.isnan(obj) or math.isinf(obj) else obj
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    return obj


@app.get("/states")
def get_states():
    try:
        data = r.get("opensky:states")
    except redis.exceptions.ConnectionError:
        raise HTTPException(status_code=503, detail="Redis not available")

    if not data:
        raise HTTPException(status_code=503, detail="Data not available")

    payload = json.loads(data)

    response = {
        "updated_at": r.get("opensky:updated_at"),
        "source": r.get("opensky:source_file"),
        "rows": len(payload),
        "data": payload,
    }
    return JSONResponse(content=sanitize(response))


@app.get("/states/preview")
def get_states_preview(limit: int = 50):
    data = r.get("opensky:states")
    if not data:
        raise HTTPException(status_code=503, detail="Data not available")

    payload = json.loads(data)[:limit]

    response = {
        "rows": len(payload),
        "data": payload,
    }

    return JSONResponse(content=sanitize(response))


# new endpoint for historic data
@app.on_event("startup")
def startup():
    con = duckdb.connect()
    con.execute("INSTALL azure;")
    con.execute("LOAD azure;")
    con.close()


@app.get("/states/historic")
def get_states_historic(
    year: int = 2025, month: int = 12, day: int = 25, limit: int = 1000
):
    try:
        from historic_data import historic_data

        df = historic_data(year=year, month=month, day=day, limit=limit)
        data = df.to_dict(orient="records")

        return JSONResponse(content={"rows": len(data), "data": sanitize(data)})
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
