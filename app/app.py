import os
import json
import math

import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    decode_responses=True,
)

app = FastAPI()

def sanitize(obj):
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
