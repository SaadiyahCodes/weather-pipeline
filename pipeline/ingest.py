"""BRONZE: land raw API responses exactly as received.

Append-only: every run writes a new file under bronze/dt=YYYY-MM-DD/.
--offline generates realistic synthetic data so the demo works without internet.
"""
import json
import os
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import requests

from . import config


def fetch_city(lat: float, lon: float) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(config.HOURLY_VARS),
        "past_days": config.PAST_DAYS,
        "forecast_days": config.FORECAST_DAYS,
        "timezone": "UTC",
    }
    r = requests.get(config.API_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def synth_city(name: str, lat: float, lon: float, now: datetime) -> dict:
    """Fake payload with the same shape as Open-Meteo's response."""
    start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=config.PAST_DAYS)
    n = (config.PAST_DAYS + config.FORECAST_DAYS) * 24
    times = [start + timedelta(hours=i) for i in range(n)]
    rng = np.random.default_rng(zlib.crc32(name.encode()))
    run_rng = np.random.default_rng(int(now.timestamp()) % 100000)  # forecast "revisions"
    local_hour = np.array([(t.hour + lon / 15.0) % 24 for t in times])
    base = 32 - 0.45 * (lat - 25)
    temp = base + 7 * np.sin((local_hour - 9) / 24 * 2 * np.pi) + rng.normal(0, 0.4, n) + run_rng.normal(0, 0.15, n)
    hum = np.clip(60 - 1.5 * (temp - base) + rng.normal(0, 3, n), 5, 100)
    wind = np.clip(12 + rng.normal(0, 3, n), 0, None)
    return {
        "latitude": lat,
        "longitude": lon,
        "hourly_units": {"temperature_2m": "°C", "relative_humidity_2m": "%", "wind_speed_10m": "km/h"},
        "hourly": {
            "time": [t.strftime("%Y-%m-%dT%H:%M") for t in times],
            "temperature_2m": [round(float(x), 1) for x in temp],
            "relative_humidity_2m": [round(float(x)) for x in hum],
            "wind_speed_10m": [round(float(x), 1) for x in wind],
        },
    }


def run_ingest(offline: bool = False, inject_bad: bool = False) -> tuple[Path, int]:
    """Write one bronze file for this run. Returns (file path, number of cities)."""
    now = datetime.now(timezone.utc)
    ingested_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    part_dir = config.BRONZE / f"dt={now:%Y-%m-%d}"
    part_dir.mkdir(parents=True, exist_ok=True)
    final = part_dir / f"run_{now:%Y%m%dT%H%M%S}Z.jsonl"
    tmp = final.with_name(final.name + ".partial")

    with open(tmp, "w", encoding="utf-8") as f:
        for i, (city, (lat, lon)) in enumerate(config.CITIES.items()):
            payload = synth_city(city, lat, lon, now) if offline else fetch_city(lat, lon)
            if inject_bad and i == 0:
                payload["hourly"]["temperature_2m"][5] = 999.0  # corrupt one value on purpose
            f.write(json.dumps({"city": city, "ingested_at": ingested_at, "payload": payload}) + "\n")
    os.replace(tmp, final)
    return final, len(config.CITIES)
