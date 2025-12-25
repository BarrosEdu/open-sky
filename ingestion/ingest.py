from __future__ import annotations

import io
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.storage.blob import BlobServiceClient

from api_auth import get_opensky_token

# -------------------------------------------------
# Environment
# -------------------------------------------------
load_dotenv()

# -------------------------------------------------
# Config
# -------------------------------------------------
@dataclass(frozen=True)
class Config:
    # OpenSky
    url: str = "https://opensky-network.org/api/states/all"
    poll_seconds: float = float(os.getenv("OPENSKY_POLL_SECONDS", "10"))
    timeout_seconds: float = float(os.getenv("OPENSKY_TIMEOUT_SECONDS", "25"))

    # Flush policy
    flush_rows: int = int(os.getenv("OPENSKY_FLUSH_ROWS", "20000"))
    flush_seconds: float = float(os.getenv("OPENSKY_FLUSH_SECONDS", "30"))

    # Azure Blob
    blob_container: str = os.getenv("AZURE_BLOB_CONTAINER", "bronze")
    blob_prefix: str = os.getenv("OPENSKY_BLOB_PREFIX", "open_sky")
    parquet_compression: str = os.getenv("OPENSKY_PARQUET_COMPRESSION", "snappy")

    # Auth (optional)
    username: Optional[str] = os.getenv("OPENSKY_USERNAME")
    password: Optional[str] = os.getenv("OPENSKY_PASSWORD")


COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
]

# -------------------------------------------------
# Logging
# -------------------------------------------------
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("opensky_ingest")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)sZ %(levelname)s %(message)s",
        "%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    return logger


LOGGER = setup_logger()

# -------------------------------------------------
# Graceful shutdown
# -------------------------------------------------
_STOP = False


def _handle_stop(sig, frame):
    global _STOP
    _STOP = True
    LOGGER.warning("event=signal_received signal=%s", sig)


signal.signal(signal.SIGINT, _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)

# -------------------------------------------------
# HTTP session
# -------------------------------------------------
def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "opensky-ingest/1.0"})
    return session


# -------------------------------------------------
# Azure Blob
# -------------------------------------------------
def azure_blob_container(container_name: str):
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING not set")

    service = BlobServiceClient.from_connection_string(conn_str)
    container = service.get_container_client(container_name)

    try:
        container.create_container()
        LOGGER.info("event=container_created name=%s", container_name)
    except Exception:
        pass  # already exists

    return container


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def partition_prefix(base: str, ts: datetime) -> str:
    return f"{base}/date={ts:%Y-%m-%d}/hour={ts:%H}"


def build_filename(ts: datetime, seq: int) -> str:
    return f"part-{ts:%Y%m%dT%H%M%SZ}-{seq:06d}.parquet"


def normalize_states(states: List[List[Any]], snapshot_time: datetime) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for st in states:
        row = dict(zip(COLUMNS, st))
        row["snapshot_time"] = snapshot_time
        rows.append(row)
    return rows


def upload_parquet_to_blob(
    df: pd.DataFrame,
    container,
    blob_path: str,
    compression: str,
) -> None:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, compression=compression)
    buffer.seek(0)

    blob = container.get_blob_client(blob_path)
    blob.upload_blob(buffer, overwrite=True)


# -------------------------------------------------
# Main
# -------------------------------------------------
def run(cfg: Config) -> None:
    session = build_session()
    auth = (cfg.username, cfg.password) if cfg.username and cfg.password else None
    container = azure_blob_container(cfg.blob_container)

    buffer: List[Dict[str, Any]] = []
    last_flush = time.monotonic()
    seq = 0

    total_rows = 0
    total_snapshots = 0
    total_failures = 0

    LOGGER.info(
        "event=start poll_seconds=%.1f flush_rows=%d flush_seconds=%.1f container=%s prefix=%s",
        cfg.poll_seconds,
        cfg.flush_rows,
        cfg.flush_seconds,
        cfg.blob_container,
        cfg.blob_prefix,
    )

    while not _STOP:
        t0 = time.monotonic()
        snapshot_time = utc_now()

        try:
            token = get_opensky_token()
            resp = session.get(
                cfg.url,
                timeout=cfg.timeout_seconds,
                headers={"Authorization": f"Bearer {token}"},
)


            if resp.status_code == 429:
                total_failures += 1
                sleep_for = max(cfg.poll_seconds, 15)
                LOGGER.warning("event=rate_limited sleep=%.1f", sleep_for)
                time.sleep(sleep_for)
                continue

            if resp.status_code >= 400:
                total_failures += 1
                LOGGER.warning("event=http_error status=%d", resp.status_code)
                time.sleep(cfg.poll_seconds)
                continue

            payload = resp.json()
            states = payload.get("states") or []
            rows = normalize_states(states, snapshot_time)

            buffer.extend(rows)
            total_rows += len(rows)
            total_snapshots += 1

            LOGGER.info(
                "event=snapshot_ok states=%d buffer=%d totals snapshots=%d rows=%d failures=%d",
                len(states),
                len(buffer),
                total_snapshots,
                total_rows,
                total_failures,
            )

        except Exception as e:
            total_failures += 1
            LOGGER.exception("event=request_failed msg=%s", str(e))

        should_flush = (
            len(buffer) >= cfg.flush_rows
            or (buffer and (time.monotonic() - last_flush) >= cfg.flush_seconds)
        )

        if should_flush:
            seq += 1
            flush_ts = utc_now()

            df = pd.DataFrame(buffer).drop_duplicates()

            blob_path = (
                f"{partition_prefix(cfg.blob_prefix, flush_ts)}/"
                f"{build_filename(flush_ts, seq)}"
            )

            try:
                upload_parquet_to_blob(
                    df=df,
                    container=container,
                    blob_path=blob_path,
                    compression=cfg.parquet_compression,
                )
                LOGGER.info(
                    "event=flush_ok blob_path=%s rows=%d cols=%d",
                    blob_path,
                    len(df),
                    len(df.columns),
                )
                buffer.clear()
                last_flush = time.monotonic()
            except Exception as e:
                total_failures += 1
                LOGGER.exception("event=flush_failed blob_path=%s msg=%s", blob_path, str(e))

        elapsed = time.monotonic() - t0
        sleep_for = max(0.0, cfg.poll_seconds - elapsed)
        if sleep_for:
            time.sleep(sleep_for)

    if buffer:
        seq += 1
        flush_ts = utc_now()
        df = pd.DataFrame(buffer).drop_duplicates()
        blob_path = (
            f"{partition_prefix(cfg.blob_prefix, flush_ts)}/"
            f"{build_filename(flush_ts, seq)}"
        )
        try:
            upload_parquet_to_blob(df, container, blob_path, cfg.parquet_compression)
            LOGGER.info("event=final_flush_ok blob_path=%s rows=%d", blob_path, len(df))
        except Exception as e:
            LOGGER.exception("event=final_flush_failed msg=%s", str(e))

    LOGGER.warning(
        "event=stopped totals snapshots=%d rows=%d failures=%d",
        total_snapshots,
        total_rows,
        total_failures,
    )


if __name__ == "__main__":
    run(Config())
