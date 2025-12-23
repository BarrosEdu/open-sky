from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dotenv import load_dotenv

load_dotenv()
# ----------------------------
# Config
# ----------------------------
@dataclass(frozen=True)
class Config:
    url: str = "https://opensky-network.org/api/states/all"

    # polling
    poll_seconds: float = float(os.getenv("OPENSKY_POLL_SECONDS", "10"))
    timeout_seconds: float = float(os.getenv("OPENSKY_TIMEOUT_SECONDS", "25"))

    # storage (bronze)
    out_dir: Path = Path(os.getenv("OPENSKY_OUT_DIR", "./data/bronze/open_sky"))
    # flush policy
    flush_rows: int = int(os.getenv("OPENSKY_FLUSH_ROWS", "20000"))  # rows buffered -> flush
    flush_seconds: float = float(os.getenv("OPENSKY_FLUSH_SECONDS", "30"))  # max seconds between flush

    # optional: store raw snapshots (for replay/debug)
    raw_dir: Optional[Path] = (
        Path(os.getenv("OPENSKY_RAW_DIR")) if os.getenv("OPENSKY_RAW_DIR") else None
    )

    # auth (optional; OpenSky may rate-limit unauth)
    username: Optional[str] = os.getenv("OPENSKY_USERNAME")
    password: Optional[str] = os.getenv("OPENSKY_PASSWORD")

    # parquet compression
    parquet_compression: str = os.getenv("OPENSKY_PARQUET_COMPRESSION", "snappy")


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

# ----------------------------
# Logging (structured-ish)
# ----------------------------
def setup_logger() -> logging.Logger:
    logger = logging.getLogger("opensky_ingest")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)sZ %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    return logger


LOGGER = setup_logger()


# ----------------------------
# Graceful shutdown
# ----------------------------
_STOP = False


def _handle_stop(sig, frame):
    global _STOP
    _STOP = True
    LOGGER.warning("signal_received=%s action=stop_requested", sig)


signal.signal(signal.SIGINT, _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)


# ----------------------------
# HTTP session w/ retries
# ----------------------------
def build_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({"User-Agent": "opensky-ingest/1.0"})
    return session


# ----------------------------
# Utilities
# ----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def partition_path(base: Path, ts: datetime) -> Path:
    # Hive-style partitions: date=YYYY-MM-DD/hour=HH
    return base / f"date={ts:%Y-%m-%d}" / f"hour={ts:%H}"


def atomic_write_parquet(df: pd.DataFrame, target_path: Path, compression: str) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")

    df.to_parquet(tmp_path, index=False, compression=compression)
    # atomic rename on same filesystem
    tmp_path.replace(target_path)


def safe_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
    tmp.replace(path)


def normalize_states(states: List[List[Any]], snapshot_time: datetime) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for st in states:
        row = dict(zip(COLUMNS, st))
        # add ingestion metadata
        row["snapshot_time"] = snapshot_time
        rows.append(row)
    return rows


def build_filename(prefix: str, ts: datetime, seq: int) -> str:
    # Example: part-20251223T102233Z-000001.parquet
    return f"{prefix}-{ts:%Y%m%dT%H%M%SZ}-{seq:06d}.parquet"


# ----------------------------
# Main loop
# ----------------------------
def run(cfg: Config) -> None:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    if cfg.raw_dir:
        cfg.raw_dir.mkdir(parents=True, exist_ok=True)

    session = build_session()
    auth = (cfg.username, cfg.password) if (cfg.username and cfg.password) else None

    buffer: List[Dict[str, Any]] = []
    last_flush = time.monotonic()
    seq = 0

    # lightweight "metrics"
    total_rows = 0
    total_snapshots = 0
    total_failures = 0

    LOGGER.info(
        "event=start poll_seconds=%.2f flush_rows=%d flush_seconds=%.2f out_dir=%s raw_dir=%s auth=%s",
        cfg.poll_seconds,
        cfg.flush_rows,
        cfg.flush_seconds,
        str(cfg.out_dir),
        str(cfg.raw_dir) if cfg.raw_dir else "none",
        "yes" if auth else "no",
    )

    while not _STOP:
        t0 = time.monotonic()
        snapshot_time = utc_now()

        try:
            resp = session.get(cfg.url, timeout=cfg.timeout_seconds, auth=auth)
            status = resp.status_code

            # handle rate limiting explicitly
            if status == 429:
                total_failures += 1
                retry_after = resp.headers.get("Retry-After")
                sleep_for = float(retry_after) if retry_after and retry_after.isdigit() else max(cfg.poll_seconds, 15)
                LOGGER.warning("event=rate_limited status=429 sleep=%.1f", sleep_for)
                time.sleep(sleep_for)
                continue

            if status >= 400:
                total_failures += 1
                LOGGER.warning("event=http_error status=%d body_snippet=%s", status, resp.text[:120].replace("\n", " "))
                time.sleep(cfg.poll_seconds)
                continue

            payload = resp.json()

            # optional raw snapshot store (good for replay/debug)
            if cfg.raw_dir:
                raw_name = f"snapshot-{snapshot_time:%Y%m%dT%H%M%SZ}.json"
                safe_write_json(cfg.raw_dir / raw_name, payload)

            states = payload.get("states") or []
            rows = normalize_states(states, snapshot_time)

            buffer.extend(rows)
            total_rows += len(rows)
            total_snapshots += 1

            api_latency_ms = (time.monotonic() - t0) * 1000.0
            LOGGER.info(
                "event=snapshot_ok states=%d buffer=%d api_latency_ms=%.1f totals snapshots=%d rows=%d failures=%d",
                len(states),
                len(buffer),
                api_latency_ms,
                total_snapshots,
                total_rows,
                total_failures,
            )

        except (requests.Timeout, requests.ConnectionError) as e:
            total_failures += 1
            LOGGER.warning("event=request_error type=%s msg=%s", type(e).__name__, str(e))
        except json.JSONDecodeError as e:
            total_failures += 1
            LOGGER.warning("event=json_decode_error msg=%s", str(e))
        except Exception as e:
            total_failures += 1
            LOGGER.exception("event=unexpected_error msg=%s", str(e))

        # Flush policy: by rows OR by time since last flush
        should_flush = (len(buffer) >= cfg.flush_rows) or ((time.monotonic() - last_flush) >= cfg.flush_seconds and buffer)
        if should_flush:
            seq += 1
            flush_ts = utc_now()
            df = pd.DataFrame(buffer)

            # basic hygiene (bronze): keep raw-ish, but remove exact duplicates
            # NOTE: idempotency & dedupe "for real" should be done in silver.
            df.drop_duplicates(inplace=True)

            part_dir = partition_path(cfg.out_dir, flush_ts)
            file_name = build_filename("part", flush_ts, seq)
            out_path = part_dir / file_name

            try:
                atomic_write_parquet(df, out_path, compression=cfg.parquet_compression)
                LOGGER.info(
                    "event=flush_ok path=%s rows=%d columns=%d compression=%s",
                    str(out_path),
                    len(df),
                    len(df.columns),
                    cfg.parquet_compression,
                )
                buffer.clear()
                last_flush = time.monotonic()
            except Exception as e:
                total_failures += 1
                LOGGER.exception("event=flush_failed path=%s msg=%s", str(out_path), str(e))
                # Don't clear buffer; try again next cycle.

        # Sleep remaining time (avoid drift)
        elapsed = time.monotonic() - t0
        sleep_for = max(0.0, cfg.poll_seconds - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)

    # final flush on shutdown
    if buffer:
        seq += 1
        flush_ts = utc_now()
        df = pd.DataFrame(buffer).drop_duplicates()
        part_dir = partition_path(cfg.out_dir, flush_ts)
        out_path = part_dir / build_filename("part", flush_ts, seq)
        try:
            atomic_write_parquet(df, out_path, compression=cfg.parquet_compression)
            LOGGER.info("event=final_flush_ok path=%s rows=%d", str(out_path), len(df))
        except Exception as e:
            LOGGER.exception("event=final_flush_failed path=%s msg=%s", str(out_path), str(e))

    LOGGER.warning(
        "event=stopped totals snapshots=%d rows=%d failures=%d",
        total_snapshots,
        total_rows,
        total_failures,
    )


if __name__ == "__main__":
    cfg = Config()
    run(cfg)
