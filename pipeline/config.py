"""Shared settings. Override the data folder with PIPELINE_DATA_DIR."""
import os
from pathlib import Path

ROOT = Path(os.environ.get("PIPELINE_DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
BRONZE = ROOT / "bronze"          # raw API responses, append-only, never edited
QUARANTINE = ROOT / "quarantine"  # bronze files that failed the quality gate
SILVER = ROOT / "silver"          # cleaned, typed, deduplicated parquet
GOLD = ROOT / "gold"              # business-ready tables for the dashboard / BI
MANIFEST = ROOT / "manifest.json"  # status of the latest run
RUNLOG = ROOT / "runs.jsonl"       # one line per run (history)

# name -> (latitude, longitude)
CITIES = {
    "Dubai": (25.2048, 55.2708),
    "Sharjah": (25.3463, 55.4209),
    "Houston": (29.7604, -95.3698),
    "Miami": (25.7617, -80.1918),
    "New York": (40.7128, -74.0060),
    "San Jose": (37.3382, -121.8863),
}

API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"]
PAST_DAYS = 2
FORECAST_DAYS = 1

# One dtype policy for every silver column (same idea as a schema policy in a real pipeline)
SILVER_DTYPES = {
    "temperature_c": "float32",
    "humidity_pct": "float32",
    "wind_kmh": "float32",
}


def slug(name: str) -> str:
    return name.lower().replace(" ", "_")
