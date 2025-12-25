import os
import time
import requests

_TOKEN_CACHE = {
    "access_token": None,
    "expires_at": 0.0,
}

def get_opensky_token() -> str:
    now = time.time()

    # Reuse token if still valid (with safety margin)
    if _TOKEN_CACHE["access_token"] and now < (_TOKEN_CACHE["expires_at"] - 60):
        return _TOKEN_CACHE["access_token"]

    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET not set")

    token_url = (
        "https://auth.opensky-network.org/"
        "auth/realms/opensky-network/"
        "protocol/openid-connect/token"
    )

    resp = requests.post(
        token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=15,
    )

    resp.raise_for_status()
    payload = resp.json()

    access_token = payload["access_token"]
    expires_in = payload.get("expires_in", 300)

    _TOKEN_CACHE["access_token"] = access_token
    _TOKEN_CACHE["expires_at"] = now + expires_in

    return access_token
